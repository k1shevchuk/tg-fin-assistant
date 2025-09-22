from datetime import date
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
    kb = ReplyKeyboardMarkup([["conservative","balanced","aggressive"]], resize_keyboard=True)
    await update.message.reply_text("Выбери риск-профиль:", reply_markup=kb)
    return RISK

async def setup_risk(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    risk = update.message.text
    if risk not in {"conservative","balanced","aggressive"}:
        kb = ReplyKeyboardMarkup([["conservative","balanced","aggressive"]], resize_keyboard=True)
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
        f"Коридор: {u.min_contrib}-{u.max_contrib} ₽\nРиск: {u.risk}\n\n"
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
    txt = update.message.text
    if txt == "Статус":
        with SessionLocal() as s:
            u = s.get(User, update.effective_user.id)
            if not u: return await update.message.reply_text("Сначала /start")
            return await update.message.reply_text(
                f"Аванс: {u.advance_day}\nЗарплата: {u.salary_day}\n"
                f"Взносы: {u.min_contrib}-{u.max_contrib} ₽\nРиск: {u.risk}",
                reply_markup=MAIN_KB
            )
    if txt == "Сменить риск":
        kb = ReplyKeyboardMarkup([["conservative","balanced","aggressive"]], resize_keyboard=True)
        return await update.message.reply_text("Выбери риск-профиль:", reply_markup=kb)
    if txt in {"conservative","balanced","aggressive"}:
        with SessionLocal() as s:
            u = s.get(User, update.effective_user.id)
            if not u: return await update.message.reply_text("Сначала /start")
            u.risk = txt; s.commit()
        return await update.message.reply_text(f"Риск-профиль обновлён: {txt}", reply_markup=MAIN_KB)
    if txt == "Внести взнос":
        return await update.message.reply_text("Введи сумму взноса, ₽:")
    # если это число — трактуем как взнос
    try:
        amount = float(txt.replace(" ", ""))
        with SessionLocal() as s:
            u = s.get(User, update.effective_user.id)
            if not u: return await update.message.reply_text("Сначала /start")
            s.add(Contribution(user_id=u.user_id, date=date.today(), amount=amount, source="manual")); s.commit()
            target, plan = propose_allocation(amount, u.risk)
        lines = "\n".join(f"- {k}: {v:,.0f} ₽".replace(",", " ") for k, v in plan.items())
        return await update.message.reply_text(
            f"Зачислил {amount:,.0f} ₽.\nЦель: {target}\nРаспределение:\n{lines}",
            reply_markup=MAIN_KB
        )
    except Exception:
        return await update.message.reply_text("Не понял. Используй меню или введи сумму, ₽.", reply_markup=MAIN_KB)

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
