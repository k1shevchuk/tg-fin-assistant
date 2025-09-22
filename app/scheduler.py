from datetime import datetime
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from telegram.ext import Application
from .db import SessionLocal
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
                target, plan = propose_allocation((u.min_contrib + u.max_contrib)/2, u.risk)
                lines = "\n".join(f"- {k}: {v:,.0f} ₽".replace(",", " ") for k, v in plan.items())
                text = (
                    "Напоминание про взнос. "
                    f"Цель: {target}\n{lines}\n"
                    "Когда будешь готов, нажми «Внести взнос» и введи сумму."
                )
                await app.bot.send_message(u.user_id, text)

    sch.start()

