from datetime import date
from sqlalchemy import func
from telegram import Update, ReplyKeyboardMarkup, ReplyKeyboardRemove
from telegram.ext import ContextTypes, ConversationHandler
from .db import SessionLocal
from .models import User, Contribution
from .strategy import propose_allocation

# --- Кнопки главного меню
MAIN_KB = ReplyKeyboardMarkup(
    [["Внести взнос", "Статус"], ["Сменить риск"]],
    resize_keyboard=True
)

CANCEL_BTN = "Отмена"
RISK_CHOICES = ["conservative", "balanced", "aggressive"]
RISK_KB = ReplyKeyboardMarkup([RISK_CHOICES, [CANCEL_BTN]], resize_keyboard=True)
CONTRIB_KB = ReplyKeyboardMarkup([[CANCEL_BTN]], resize_keyboard=True)


def fmt_amount(value: float) -> str:
    return f"{value:,.0f}".replace(",", " ")

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
            total = (
                s.query(func.sum(Contribution.amount))
                .filter(Contribution.user_id == u.user_id)
                .scalar()
                or 0.0
            )
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

    normalized = txt.replace(" ", "").replace(",", ".")
    try:
        amount = float(normalized)
    except ValueError:
        amount = None

    if amount is not None and amount > 0:
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
            total = (
                s.query(func.sum(Contribution.amount))
                .filter(Contribution.user_id == u.user_id)
                .scalar()
                or 0.0
            )
            target, plan = propose_allocation(amount, u.risk)
        lines = "\n".join(f"- {k}: {fmt_amount(v)} ₽" for k, v in plan.items())
        ctx.user_data.pop("mode", None)
        return await update.message.reply_text(
            f"Зачислил {fmt_amount(amount)} ₽.\nЦель: {target}\nРаспределение:\n{lines}\n"
            f"Текущий баланс: {fmt_amount(total)} ₽",
            reply_markup=MAIN_KB,
        )

    if mode == "contrib":
        return await update.message.reply_text(
            "Нужна сумма в рублях. Попробуй ещё раз или нажми «Отмена».",
            reply_markup=CONTRIB_KB,
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
