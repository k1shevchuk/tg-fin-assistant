Telegram Finance Assistant Bot
Описание

Этот проект — Telegram-бот для автоматизации инвестиционной дисциплины.
Он напоминает о зарплате и авансе, фиксирует внесённые суммы и предлагает базовое распределение в зависимости от риск-профиля.

📌 В связке с отдельным финансовым аналитиком (чат-промпт) система работает как личный помощник:

Бот → дисциплина: напоминания, сбор сумм, учёт.

Аналитик → стратегия: прогнозы, подбор инструментов, сценарии, ребаланс.

Возможности

Настройка профиля через /setup: даты аванса и зарплаты, диапазон ежемесячных инвестиций, риск-профиль.

Напоминания в дни выплат.

Учёт внесённых сумм через кнопки.

Автоматическое предложение распределения (консервативное / сбалансированное / агрессивное).

Хранение истории в SQLite.

Установка
1. Клонировать
git clone https://github.com/k1shevchuk/tg-fin-assistant.git
cd tg-fin-assistant

2. Зависимости
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

3. Конфигурация

Создайте .env в корне:

BOT_TOKEN=ваш_токен_от_BotFather
TZ=Europe/Moscow

4. Локальный запуск
python -m app.main

Автозапуск через systemd

Файл /etc/systemd/system/tgfinance.service:

[Unit]
Description=Telegram Finance Assistant Bot
After=network.target

[Service]
User=tgfinance
WorkingDirectory=/home/tgfinance/tg-fin-assistant
Environment="PATH=/home/tgfinance/tg-fin-assistant/venv/bin"
ExecStart=/home/tgfinance/tg-fin-assistant/venv/bin/python -m app.main
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target


Активировать:

sudo systemctl daemon-reload
sudo systemctl enable tgfinance
sudo systemctl start tgfinance
sudo systemctl status tgfinance


Логи:

journalctl -u tgfinance -f

Использование

/start — запуск бота.

/setup — мастер настройки (аванс, зарплата, взносы, риск).

Кнопки меню:

Внести взнос — добавить инвестицию.

Статус — посмотреть параметры.

Сменить риск — изменить риск-профиль.

В дни выплат бот сам спросит: «Получил ли ты доход? Какую сумму инвестируем?».

В связке с аналитиком

Этот бот = дисциплина (напоминания, фиксация взносов).

Отдельный чат с промптом = стратегия (сценарии, анализ макроэкономики, конкретные активы).

Вместе они работают как полноценный финансовый помощник.
