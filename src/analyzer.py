import json
import logging

from openai import AsyncOpenAI

from src.config import Config
from src.database import Database

logger = logging.getLogger("trading_bot")

SYSTEM_PROMPT = """Ты — аналитик финансовых рынков. Твоя задача — проанализировать экономические данные и вернуть структурированный JSON.

ОБЯЗАТЕЛЬНЫЙ формат ответа — ТОЛЬКО валидный JSON, без markdown, без ```json```, без пояснений:
{
  "importance": 3,
  "category": "Инфляция",
  "region": "США",
  "title": "CPI вырос на 0.27% м/м",
  "summary": "Индекс потребительских цен вырос до 327.460 с 326.588. Базовая инфляция остаётся выше целевого уровня ФРС.",
  "impact": "Может поддержать доходности Treasuries и доллар; вероятно давление на золото и риск-активы."
}

Правила для каждого поля:

importance (1-5):
  5 = Решение FOMC по ставке, NFP с большим отклонением, экстренные события (война, кризис)
  4 = Релиз CPI/PCE, GDP (первая оценка), крупный сюрприз в данных
  3 = Регулярные данные (розничные продажи, промпроизводство, торговый баланс, аукционы)
  2 = Еженедельные рутинные данные (заявки на пособие, запасы нефти без сюрпризов)
  1 = Без изменений, технические ревизии, незначительные обновления

category — ОДНО из:
  Инфляция, Рынок труда, ВВП/Рост, Ставки/ЦБ, Энергетика, Металлы, Валюты, Торговля, Потребитель, Гос.долг, Позиционирование, Промышленность

region — ОДНО из:
  США, Европа, Великобритания, Азия, Мир

title — краткий заголовок, максимум 60 символов, без эмодзи

summary — 1-3 предложения с конкретными цифрами, без форматирования markdown

impact — Формат: одно короткое предложение, затем ОБЯЗАТЕЛЬНО с новой строки (\n) список активов.
  Пример: "Замедление роста может снизить ожидания по ставке.\nВозможное влияние на: доллар, Treasuries, S&P 500, золото"
  Первая строка — краткий контекст (1 предложение).
  Вторая строка — ВСЕГДА начинается с "Возможное влияние на:" и список активов.
  Используй ТОЛЬКО мягкие формулировки: "может", "вероятно", "потенциально"
  НИКОГДА не пиши "это приведёт к", "это вызовет", "это означает"
  Список активов выбирай из: доллар, евро, фунт, йена, Treasuries, S&P 500, Nasdaq, нефть, золото, серебро, газ, крипто, риск-активы, облигации

Общие правила:
- ТОЛЬКО русский язык
- ТОЛЬКО JSON в ответе, ничего больше
- Никакого markdown (**, *, #, ```)
- Конкретные цифры обязательны"""

# Prompt for generating the morning digest
DIGEST_PROMPT = """Ты — аналитик финансовых рынков. Тебе даны краткие сводки экономических событий за ночь.
Напиши ОДНУ связную утреннюю сводку для трейдера.

Правила:
- Пиши ТОЛЬКО на русском языке
- Будь кратким — максимум 20 строк
- Указывай конкретные цифры
- Используй мягкие формулировки для прогнозов ("может", "вероятно")
- НИКАКОГО markdown форматирования (**, *, #, ```)
- Не добавляй приветствий и прощаний

ГРУППЫ — используй ТОЛЬКО эти группы с эмодзи. Пропусти группы без новостей.
Порядок строго фиксирован:

📊 Макро
💰 Инфляция
👷 Рынок труда
📈 ВВП / Рост
🏦 Ставки / Центробанки
🛢 Нефть / Энергетика
🥇 Металлы
🌾 Сельхоз / Сырьё
📉 Рынки / Позиционирование
💱 Валюты
🏛 Гос.долг / Аукционы
🏢 Корпоративное
⚡ Энергопереход
😱 Сентимент
🌐 Прочее
📋 Короткий вывод

ВАЖНО:
- Группа 🌐 Прочее используется ТОЛЬКО если новость не подходит ни к одной другой группе
- Группа 📋 Короткий вывод ВСЕГДА присутствует в конце — 2-3 предложения общего вывода
- НЕ придумывай новые группы, используй ТОЛЬКО из списка выше
- Каждая группа начинается с новой строки с эмодзи и названия"""


class AIAnalyzer:
    """Sends economic data to OpenAI for structured analysis with cost tracking."""

    def __init__(self, db: Database):
        self.db = db
        self.client = AsyncOpenAI(api_key=Config.OPENAI_API_KEY)

    def _estimate_tokens(self, text: str) -> int:
        return len(text) // 4

    def _truncate_input(self, text: str) -> str:
        estimated = self._estimate_tokens(text)
        if estimated <= Config.MAX_INPUT_TOKENS:
            return text
        max_chars = Config.MAX_INPUT_TOKENS * 4
        truncated = text[:max_chars]
        logger.warning(
            "Input truncated from ~%d to ~%d tokens",
            estimated, Config.MAX_INPUT_TOKENS,
        )
        return truncated + "\n\n[...данные обрезаны]"

    def _calculate_cost(self, input_tokens: int, output_tokens: int) -> float:
        input_cost = (input_tokens / 1_000_000) * Config.INPUT_COST_PER_M
        output_cost = (output_tokens / 1_000_000) * Config.OUTPUT_COST_PER_M
        return round(input_cost + output_cost, 6)

    async def check_daily_limit(self) -> tuple[bool, float]:
        spent = self.db.get_today_cost()
        return spent < Config.DAILY_COST_LIMIT_USD, spent

    def _parse_ai_json(self, text: str) -> dict | None:
        """Try to parse AI response as JSON, handling common issues."""
        if not text:
            return None

        # Strip markdown code fences if present
        cleaned = text.strip()
        if cleaned.startswith("```"):
            # Remove first line and last ```
            lines = cleaned.split("\n")
            lines = lines[1:]  # Remove ```json
            if lines and lines[-1].strip() == "```":
                lines = lines[:-1]
            cleaned = "\n".join(lines)

        try:
            data = json.loads(cleaned)
            # Validate required fields
            required = ["importance", "category", "region", "title", "summary", "impact"]
            for field in required:
                if field not in data:
                    logger.warning("AI JSON missing field: %s", field)
                    return None
            # Clamp importance to 1-5
            data["importance"] = max(1, min(5, int(data["importance"])))
            return data
        except (json.JSONDecodeError, ValueError) as e:
            logger.error("Failed to parse AI JSON: %s\nRaw: %s", e, text[:500])
            return None

    async def analyze(
        self, source: str, raw_data: str
    ) -> dict | None:
        """Send data to AI for structured analysis.

        Returns dict with keys: importance, category, region, title, summary,
        impact, input_tokens, output_tokens, cost_usd
        Returns None if daily limit exceeded or error occurs.
        """
        within_limit, spent_today = await self.check_daily_limit()
        if not within_limit:
            logger.warning(
                "Daily cost limit reached ($%.4f / $%.2f). Skipping.",
                spent_today, Config.DAILY_COST_LIMIT_USD,
            )
            return None

        safe_input = self._truncate_input(raw_data)

        try:
            response = await self.client.chat.completions.create(
                model=Config.OPENAI_MODEL,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": safe_input},
                ],
                max_completion_tokens=4000,
            )

            input_tokens = response.usage.prompt_tokens
            output_tokens = response.usage.completion_tokens
            cost = self._calculate_cost(input_tokens, output_tokens)

            raw_content = response.choices[0].message.content or ""

            self.db.log_token_usage(source, input_tokens, output_tokens, cost)

            logger.info(
                "AI analysis for %s: %d in / %d out tokens, $%.4f",
                source, input_tokens, output_tokens, cost,
            )

            # Parse structured JSON response
            parsed = self._parse_ai_json(raw_content)

            if parsed is None:
                # Fallback: if JSON parsing fails, use raw text
                logger.warning("AI returned non-JSON for %s, using fallback", source)
                return {
                    "importance": 3,
                    "category": "Другое",
                    "region": "Мир",
                    "title": "Обновление данных",
                    "summary": raw_content[:500],
                    "impact": "",
                    "input_tokens": input_tokens,
                    "output_tokens": output_tokens,
                    "cost_usd": cost,
                }

            parsed["input_tokens"] = input_tokens
            parsed["output_tokens"] = output_tokens
            parsed["cost_usd"] = cost
            return parsed

        except Exception as e:
            logger.error("AI analysis failed for %s: %s", source, e)
            self.db.log_activity(source, "ai_error", str(e), status="error")
            return None

    async def generate_digest(
        self, summaries: list[dict]
    ) -> dict | None:
        """Generate a morning digest from pre-analyzed summaries.

        Args:
            summaries: list of queue items with category, summary, impact fields
        """
        within_limit, spent_today = await self.check_daily_limit()
        if not within_limit:
            return None

        # Group by category for the AI
        lines = []
        for item in summaries:
            importance_dots = "●" * item["importance"] + "○" * (5 - item["importance"])
            lines.append(
                f"[{importance_dots}] [{item['category']}] [{item['region']}]\n"
                f"{item['title']}: {item['summary']}\n"
                f"Влияние: {item['impact']}\n"
            )

        input_text = (
            f"Ночные обновления ({len(summaries)} событий):\n\n"
            + "\n".join(lines)
        )

        safe_input = self._truncate_input(input_text)

        try:
            response = await self.client.chat.completions.create(
                model=Config.OPENAI_MODEL,
                messages=[
                    {"role": "system", "content": DIGEST_PROMPT},
                    {"role": "user", "content": safe_input},
                ],
                max_completion_tokens=4000,
            )

            input_tokens = response.usage.prompt_tokens
            output_tokens = response.usage.completion_tokens
            cost = self._calculate_cost(input_tokens, output_tokens)

            content = response.choices[0].message.content or ""

            self.db.log_token_usage("digest", input_tokens, output_tokens, cost)

            logger.info(
                "Digest generated: %d in / %d out tokens, $%.4f",
                input_tokens, output_tokens, cost,
            )

            return {
                "text": content.strip(),
                "input_tokens": input_tokens,
                "output_tokens": output_tokens,
                "cost_usd": cost,
            }

        except Exception as e:
            logger.error("Digest generation failed: %s", e)
            return None
