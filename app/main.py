from telegram.ext import Application, CommandHandler, MessageHandler, ConversationHandler, filters
from .config import settings
from .db import engine, Base
from .handlers import (
    start, setup_start, setup_adv_day, setup_sal_day, setup_min, setup_max, setup_risk,
    setup_cancel, on_text, setup2, income, contrib, status, risk, ideas
)
from .scheduler import setup_jobs
from .handlers import ADV_DAY, SAL_DAY, MIN_AMT, MAX_AMT, RISK as RISK_STATE

def build_app() -> Application:
    app = Application.builder().token(settings.BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))

    app.add_handler(ConversationHandler(
        entry_points=[CommandHandler("setup", setup_start)],
        states={
            ADV_DAY: [MessageHandler(filters.TEXT & ~filters.COMMAND, setup_adv_day)],
            SAL_DAY: [MessageHandler(filters.TEXT & ~filters.COMMAND, setup_sal_day)],
            MIN_AMT: [MessageHandler(filters.TEXT & ~filters.COMMAND, setup_min)],
            MAX_AMT: [MessageHandler(filters.TEXT & ~filters.COMMAND, setup_max)],
            RISK_STATE: [MessageHandler(filters.TEXT & ~filters.COMMAND, setup_risk)],
        },
        fallbacks=[CommandHandler("cancel", setup_cancel)]
    ))

    # Совместимость старых команд
    app.add_handler(CommandHandler("setup2", setup2))
    app.add_handler(CommandHandler("income", income))
    app.add_handler(CommandHandler("contrib", contrib))
    app.add_handler(CommandHandler("status", status))
    app.add_handler(CommandHandler("risk", risk))
    app.add_handler(CommandHandler("ideas", ideas))

    # Универсальный обработчик текста и кнопок
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text))

    setup_jobs(app, settings.TZ)
    return app

def main():
    Base.metadata.create_all(bind=engine)
    app = build_app()
    app.run_polling()

if __name__ == "__main__":
    main()
