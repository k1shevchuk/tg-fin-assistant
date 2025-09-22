from datetime import datetime
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from telegram.ext import Application

from ._loguru import logger
from .db import SessionLocal
from .formatting import fmt_amount, format_idea_digest
from .ideas import generate_ideas, rank_and_filter
from .models import User
from .strategy import propose_allocation

def setup_jobs(app: Application, tz: str):
    sch = AsyncIOScheduler(timezone=tz)

    @sch.scheduled_job(CronTrigger(hour=10, minute=0))
    async def ping_income_days():
        today = datetime.now(sch.timezone).day
        with SessionLocal() as s:
            users = s.query(User).all()
            for u in users:
                if today in (u.advance_day, u.salary_day):
                    await app.bot.send_message(
                        u.user_id,
                        "Сегодня день выплаты (аванс/зарплата). Получил доход?"
                        " Открой бот, нажми «Внести взнос» и введи сумму — я предложу распределение."
                        " Если параметры поменялись, запусти /setup."
                    )

    @sch.scheduled_job(CronTrigger(day="15", hour=11, minute=0))
    async def soft_nudge():
        with SessionLocal() as s:
            users = s.query(User).all()
            for u in users:
                advice = propose_allocation((u.min_contrib + u.max_contrib) / 2, u.risk)
                lines = []
                for line in advice.plan:
                    percent = round(line.weight * 100)
                    lines.append(f"- {line.label}: {fmt_amount(line.amount)} ₽ (~{percent}%)")
                block = "\n".join(lines)
                text = (
                    "Напоминание про взнос. "
                    f"Цель: {advice.target}\n{block}\n"
                    "Когда будешь готов, нажми «Внести взнос» и введи сумму."
                )
                if advice.analytics:
                    extra = advice.analytics.get("title")
                    source = advice.analytics.get("source", "MOEX")
                    url = advice.analytics.get("url")
                    if extra:
                        text += f"\n\nСвежая аналитика {source}: {extra}"
                    if url:
                        text += f"\n{url}"
                await app.bot.send_message(u.user_id, text)

    @sch.scheduled_job(CronTrigger(hour=10, minute=30))
    async def push_daily_ideas():
        with SessionLocal() as s:
            users = s.query(User).all()
        for u in users:
            try:
                ideas = rank_and_filter(generate_ideas(u.risk or "balanced"))
            except Exception as exc:  # pragma: no cover
                logger.warning("Ideas digest failed for %s: %s", u.user_id, exc)
                continue
            if not ideas:
                continue
            digest = "\n\n".join(format_idea_digest(item) for item in ideas)
            header = "Идеи на сегодня:" if len(ideas) > 1 else "Идея дня:"
            await app.bot.send_message(
                u.user_id,
                f"{header}\n{digest}",
            )

    sch.start()

