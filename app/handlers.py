from datetime import date
from textwrap import shorten
from sqlalchemy import func

from ._loguru import logger
from telegram import Update, ReplyKeyboardMarkup, ReplyKeyboardRemove
from telegram.ext import ContextTypes, ConversationHandler
from ._requests import RequestException
from .db import SessionLocal
from .formatting import fmt_amount, fmt_signed, format_idea
from .ideas import generate_ideas, rank_and_filter
from .models import User, Contribution
from .providers import MarketDataError
from .strategy import propose_allocation

# --- Кнопки главного меню
ADJUST_BTN = "Изменить баланс"
MAIN_KB = ReplyKeyboardMarkup(
    [["Внести взнос", "Статус"], ["Сменить риск", ADJUST_BTN], ["Идеи"]],
    resize_keyboard=True
)

CANCEL_BTN = "Отмена"
RISK_CHOICES = ["conservative", "balanced", "aggressive"]
RISK_KB = ReplyKeyboardMarkup([RISK_CHOICES, [CANCEL_BTN]], resize_keyboard=True)
CONTRIB_KB = ReplyKeyboardMarkup([[CANCEL_BTN]], resize_keyboard=True)
ADJUST_KB = ReplyKeyboardMarkup([[CANCEL_BTN]], resize_keyboard=True)


def load_balance(session, user_id: int) -> float:
    return (
        session.query(func.sum(Contribution.amount))
        .filter(Contribution.user_id == user_id)
        .scalar()
        or 0.0
    )


async def record_manual_contribution(update: Update, ctx: ContextTypes.DEFAULT_TYPE, amount: float):
    advice = None
    total = 0.0
    error_note = "Не удалось рассчитать распределение сейчас. Попробуй позже."

    with SessionLocal() as s:
        u = s.get(User, update.effective_user.id)
        if not u:
            ctx.user_data.pop("mode", None)
            return await update.message.reply_text("Сначала /start", reply_markup=MAIN_KB)
        s.add(
            Contribution(
                user_id=u.user_id,
                date=date.today(),
                amount=amount,
                source="manual",
            )
        )
        s.commit()
        total = load_balance(s, u.user_id)
        try:
            advice = propose_allocation(amount, u.risk)
        except (MarketDataError, RequestException) as exc:
            logger.warning(
                "Allocation unavailable for %s: %s", update.effective_user.id, exc
            )
        except Exception as exc:  # pragma: no cover - unexpected failures
            logger.error(
                "Unexpected allocation failure for %s: %s",
                update.effective_user.id,
                exc,
            )
        else:
            error_note = ""

    ctx.user_data.pop("mode", None)

    lines: list[str] = []
    quote_sources: set[str] = set()

    if advice:
        for line in advice.plan:
            percent = round(line.weight * 100)
            base = f"- {line.label}: {fmt_amount(line.amount)} ₽ (~{percent}%)"
            if line.type == "cash":
                lines.append(base)
                continue

            if line.quote:
                quote_sources.add(line.quote.source)
                price = fmt_amount(line.quote.price, precision=2)
                if line.lots:
                    invested = line.invested or 0.0
                    info = (
                        f"  {line.lots} лот × {line.quote.lot} шт = {line.units} шт по {price} ₽"
                        f" → {fmt_amount(invested, precision=2)} ₽"
                    )
                    if line.leftover and line.leftover >= 1:
                        info += f" (остаток {fmt_amount(line.leftover)} ₽)"
                    lines.append(base + "\n" + info)
                else:
                    lines.append(
                        base
                        + "\n  "
                        + f"Цена {price} ₽ за бумагу. Отложим {fmt_amount(line.amount)} ₽,"
                        + " пока не хватит на целый лот."
                    )
            else:
                note = f" ({line.note})" if line.note else ""
                lines.append(base + note)

        if advice.analytics:
            summary = advice.analytics.get("summary") or ""
            snippet = shorten(summary, width=220, placeholder="…") if summary else ""
            analytics_lines = [
                "",
                f"Актуальная аналитика ({advice.analytics.get('source', 'MOEX')}):",
                advice.analytics.get("title", ""),
            ]
            if snippet:
                analytics_lines.append(snippet)
            url = advice.analytics.get("url")
            if url:
                analytics_lines.append(url)
            lines.extend(analytics_lines)

        if quote_sources:
            lines.append("")
            lines.append("Котировки: " + ", ".join(sorted(quote_sources)))

    message_parts: list[str] = [f"Зачислил {fmt_amount(amount)} ₽."]
    if advice:
        message_parts.append(f"Цель: {advice.target}")
        if lines:
            message_parts.append("Распределение:")
            message_parts.append("\n".join(lines))
        else:
            message_parts.append("Распределение: —")
    else:
        message_parts.append(error_note)

    message_parts.append("")
    message_parts.append(f"Текущий баланс: {fmt_amount(total)} ₽")

    text = "\n".join(message_parts)
    return await update.message.reply_text(text, reply_markup=MAIN_KB)


async def record_balance_adjustment(update: Update, ctx: ContextTypes.DEFAULT_TYPE, desired_total: float):
    with SessionLocal() as s:
        u = s.get(User, update.effective_user.id)
        if not u:
            ctx.user_data.pop("mode", None)
            return await update.message.reply_text("Сначала /start", reply_markup=MAIN_KB)
        current = load_balance(s, u.user_id)
        delta = round(desired_total - current, 2)
        if delta:
            s.add(
                Contribution(
                    user_id=u.user_id,
                    date=date.today(),
                    amount=delta,
                    source="adjustment",
                )
            )
            s.commit()
        new_total = current + delta
    ctx.user_data.pop("mode", None)
    if delta == 0:
        return await update.message.reply_text(
            f"Баланс уже составляет {fmt_amount(new_total)} ₽. Ничего не менял.",
            reply_markup=MAIN_KB,
        )
    change = fmt_signed(delta)
    return await update.message.reply_text(
        f"Баланс обновлён: {fmt_amount(new_total)} ₽ (изменение {change} ₽).",
        reply_markup=MAIN_KB,
    )


async def send_ideas(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    ctx.user_data.pop("mode", None)
    with SessionLocal() as s:
        u = s.get(User, update.effective_user.id)
        if not u:
            return await update.message.reply_text("Сначала /start", reply_markup=MAIN_KB)
        risk = u.risk or "balanced"
    try:
        generated = generate_ideas(risk)
        ranked = rank_and_filter(generated)
    except Exception as exc:  # pragma: no cover
        logger.error("Failed to build ideas for %s: %s", update.effective_user.id, exc)
        return await update.message.reply_text(
            "Не удалось собрать идеи сейчас. Попробуй позже.",
            reply_markup=MAIN_KB,
        )

    if not ranked:
        return await update.message.reply_text(
            "Пока нет актуальных идей. Попробуй обновить позже.",
            reply_markup=MAIN_KB,
        )

    blocks = [format_idea(item) for item in ranked]
    text = "\n\n".join(blocks)
    return await update.message.reply_text(text, reply_markup=MAIN_KB)

# --- Состояния мастера
ADV_DAY, SAL_DAY, MIN_AMT, MAX_AMT, RISK = range(5)

# /start -> показать меню и подсказку
async def start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    with SessionLocal() as s:
        u = s.get(User, uid) or User(user_id=uid)
        s.add(u); s.commit()
    await update.message.reply_text(
        "Я помогу инвестировать регулярно. Нажми “Статус” или запусти настройки: /setup",
        reply_markup=MAIN_KB
    )

# /setup -> мастер
async def setup_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    ctx.user_data.clear()
    await update.message.reply_text(
        "Укажи день АВАНСА (число месяца 1–28). В любой момент можно /cancel",
        reply_markup=ReplyKeyboardRemove()
    )
    return ADV_DAY

async def setup_adv_day(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    try:
        adv = int(update.message.text)
        if not 1 <= adv <= 28: raise ValueError
    except Exception:
        await update.message.reply_text("Число 1–28. Введи заново.")
        return ADV_DAY
    ctx.user_data["adv"] = adv
    await update.message.reply_text("Теперь день ЗАРПЛАТЫ (1–28):")
    return SAL_DAY

async def setup_sal_day(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    try:
        sal = int(update.message.text)
        if not 1 <= sal <= 28: raise ValueError
    except Exception:
        await update.message.reply_text("Число 1–28. Введи заново.")
        return SAL_DAY
    ctx.user_data["sal"] = sal
    await update.message.reply_text("Минимальный ежемесячный взнос (₽):")
    return MIN_AMT

async def setup_min(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    try:
        mn = int(update.message.text)
        if mn < 0: raise ValueError
    except Exception:
        await update.message.reply_text("Введи целое число ≥ 0.")
        return MIN_AMT
    ctx.user_data["min"] = mn
    await update.message.reply_text("Максимальный ежемесячный взнос (₽):")
    return MAX_AMT

async def setup_max(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    try:
        mx = int(update.message.text)
        if mx <= ctx.user_data["min"]: raise ValueError
    except Exception:
        await update.message.reply_text("Должно быть больше минимума. Введи заново.")
        return MAX_AMT
    ctx.user_data["max"] = mx
    kb = ReplyKeyboardMarkup([RISK_CHOICES], resize_keyboard=True)
    await update.message.reply_text("Выбери риск-профиль:", reply_markup=kb)
    return RISK

async def setup_risk(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    risk = update.message.text
    if risk not in set(RISK_CHOICES):
        kb = ReplyKeyboardMarkup([RISK_CHOICES], resize_keyboard=True)
        await update.message.reply_text(
            "Нажми одну из кнопок: conservative | balanced | aggressive",
            reply_markup=kb
        )
        return RISK
    with SessionLocal() as s:
        u = s.get(User, update.effective_user.id) or User(user_id=update.effective_user.id)
        u.advance_day = ctx.user_data["adv"]
        u.salary_day  = ctx.user_data["sal"]
        u.min_contrib = ctx.user_data["min"]
        u.max_contrib = ctx.user_data["max"]
        u.risk = risk
        s.add(u); s.commit()
    ctx.user_data.clear()
    await update.message.reply_text(
        f"Готово.\nАванс: {u.advance_day}\nЗарплата: {u.salary_day}\n"
        f"Коридор: {fmt_amount(u.min_contrib)}-{fmt_amount(u.max_contrib)} ₽\nРиск: {u.risk}\n\n"
        "В нужные дни спрошу про доход и предложу распределение.",
        reply_markup=MAIN_KB
    )
    return ConversationHandler.END

async def setup_cancel(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    ctx.user_data.clear()
    await update.message.reply_text(
        "Настройка прервана. Можно вернуться к меню или снова запустить /setup.",
        reply_markup=MAIN_KB
    )
    return ConversationHandler.END

# --- Обработчики кнопок главного меню
async def on_text(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    txt = (update.message.text or "").strip()
    mode = ctx.user_data.get("mode")

    if txt == CANCEL_BTN:
        if mode:
            ctx.user_data.pop("mode", None)
            return await update.message.reply_text(
                "Отменил. Возвращаюсь к меню.",
                reply_markup=MAIN_KB,
            )
        return await update.message.reply_text(
            "Хорошо, ничего не делаем.",
            reply_markup=MAIN_KB,
        )

    if txt == "Статус":
        ctx.user_data.pop("mode", None)
        with SessionLocal() as s:
            u = s.get(User, update.effective_user.id)
            if not u:
                return await update.message.reply_text("Сначала /start", reply_markup=MAIN_KB)
            total = load_balance(s, u.user_id)
        return await update.message.reply_text(
            f"Аванс: {u.advance_day}\nЗарплата: {u.salary_day}\n"
            f"Взносы: {fmt_amount(u.min_contrib)}-{fmt_amount(u.max_contrib)} ₽\n"
            f"Риск: {u.risk}\nТекущий баланс: {fmt_amount(total)} ₽",
            reply_markup=MAIN_KB,
        )

    if txt == "Сменить риск":
        ctx.user_data["mode"] = "risk"
        return await update.message.reply_text(
            "Выбери риск-профиль или нажми «Отмена».",
            reply_markup=RISK_KB,
        )

    if txt == "Идеи":
        return await send_ideas(update, ctx)

    if txt in RISK_CHOICES:
        with SessionLocal() as s:
            u = s.get(User, update.effective_user.id)
            if not u:
                ctx.user_data.pop("mode", None)
                return await update.message.reply_text("Сначала /start", reply_markup=MAIN_KB)
            u.risk = txt
            s.commit()
        ctx.user_data.pop("mode", None)
        return await update.message.reply_text(
            f"Риск-профиль обновлён: {txt}",
            reply_markup=MAIN_KB,
        )

    if mode == "risk":
        return await update.message.reply_text(
            "Пожалуйста, выбери одну из кнопок или нажми «Отмена».",
            reply_markup=RISK_KB,
        )

    if txt == "Внести взнос":
        ctx.user_data["mode"] = "contrib"
        return await update.message.reply_text(
            "Введи сумму взноса, ₽. Для отмены нажми «Отмена».",
            reply_markup=CONTRIB_KB,
        )

    if txt == ADJUST_BTN:
        with SessionLocal() as s:
            u = s.get(User, update.effective_user.id)
            if not u:
                ctx.user_data.pop("mode", None)
                return await update.message.reply_text("Сначала /start", reply_markup=MAIN_KB)
            total = load_balance(s, u.user_id)
        ctx.user_data["mode"] = "adjust"
        return await update.message.reply_text(
            f"Сейчас учтено {fmt_amount(total)} ₽. Введи желаемый баланс, ₽."
            " Чтобы обнулить, введи 0. Для отмены нажми «Отмена».",
            reply_markup=ADJUST_KB,
        )

    normalized = txt.replace(" ", "").replace(",", ".")
    try:
        amount = float(normalized)
    except ValueError:
        amount = None

    if mode == "contrib":
        if amount is None or amount <= 0:
            return await update.message.reply_text(
                "Нужна положительная сумма в рублях. Попробуй ещё раз или нажми «Отмена».",
                reply_markup=CONTRIB_KB,
            )
        return await record_manual_contribution(update, ctx, amount)

    if mode == "adjust":
        if amount is None or amount < 0:
            return await update.message.reply_text(
                "Нужна сумма в рублях (0 и больше). Попробуй ещё раз или нажми «Отмена».",
                reply_markup=ADJUST_KB,
            )
        return await record_balance_adjustment(update, ctx, amount)

    if amount is not None and amount > 0:
        return await record_manual_contribution(update, ctx, amount)

    if amount is not None:
        return await update.message.reply_text(
            "Сумма должна быть больше нуля. Чтобы изменить баланс, нажми «Изменить баланс».",
            reply_markup=MAIN_KB,
        )

    return await update.message.reply_text(
        "Не понял. Используй меню или введи сумму, ₽.",
        reply_markup=MAIN_KB,
    )

# Старые команды оставляем, если привык:
async def setup2(update: Update, ctx: ContextTypes.DEFAULT_TYPE):  # совместимость
    return await update.message.reply_text("Теперь есть /setup с кнопками.")

async def income(update: Update, ctx: ContextTypes.DEFAULT_TYPE):  # совместимость
    return await update.message.reply_text("Доход фиксировать не нужно. В дни выплат я сам спрошу.")
    
async def contrib(update: Update, ctx: ContextTypes.DEFAULT_TYPE):  # совместимость
    return await update.message.reply_text("Нажми «Внести взнос» и введи сумму.")

async def status(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    return await on_text(update, ctx)

async def risk(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    return await on_text(update, ctx)


async def ideas(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    return await send_ideas(update, ctx)
