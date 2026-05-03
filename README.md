# 🤖 Aegis Dual Bot System

## 🔴 SHORT + 🟢 LONG боты в одном репозитории

---

## 📁 Структура репозитория

```
aegis-bots/                    ← Корень репозитория
├── README.md                  ← Этот файл
├── render.yaml               ← Конфиг Render (2 сервиса)
├── short-bot/                ← 🔴 SHORT бот
│   ├── src/
│   │   └── main.py          ← Основной код (992 строки)
│   ├── requirements.txt
│   ├── runtime.txt
│   └── .env.example
├── long-bot/                ← 🟢 LONG бот
│   ├── src/
│   │   └── main.py          ← Основной код (986 строк)
│   ├── requirements.txt
│   ├── runtime.txt
│   └── .env.example
└── shared/                  ← 📦 Общие модули
    ├── bot/
    │   └── telegram.py     ← Telegram интеграция
    ├── execution/
    │   ├── auto_trader.py
    │   └── trade_manager.py
    ├── upstash/
    │   └── redis_client.py ← Redis клиент
    └── __init__.py
```

---

## 🚀 Быстрый старт (GitHub + Render)

### Шаг 1: Создать репозиторий на GitHub

```bash
cd "/Users/artemt/Downloads/NEW very HARD BOT/aegis-bots"

# Инициализация git
git init
git remote add origin https://github.com/turushartm-cloud/aegis-bots.git

# Первый commit
git add .
git commit -m "🤖 Dual Bot System: SHORT + LONG + shared modules"
git branch -M main
git push -u origin main
```

---

### Шаг 2: Деплой через Render Blueprint

#### Вариант A: Blueprint (Автоматический)

1. **Зайдите на Render:** https://dashboard.render.com/blueprints/new
2. **Connect GitHub:** выберите `turushartm-cloud/aegis-bots`
3. **Render автоматически найдёт `render.yaml`** с двумя сервисами
4. **Нажмите "Apply"** — создастся 2 сервиса автоматически

#### Вариант B: Ручной (если Blueprint не сработал)

**Сервис 1: SHORT бот**
```
+ New → Web Service
Name: aegis-short-alpha
Region: Oregon (US West)
Runtime: Python 3
Plan: Starter ($7/mo)
Root Directory: short-bot
Build Command: pip install -r requirements.txt
Start Command: uvicorn src.main:app --host 0.0.0.0 --port $PORT
```

**Сервис 2: LONG бот**
```
+ New → Web Service
Name: aegis-long-alpha
Region: Oregon (US West)
Runtime: Python 3
Plan: Starter ($7/mo)
Root Directory: long-bot
Build Command: pip install -r requirements.txt
Start Command: uvicorn src.main:app --host 0.0.0.0 --port $PORT
```

---

### Шаг 3: Environment Variables

#### 🔴 SHORT бот (aegis-short-alpha)

**Обязательные (Secret):**
| Variable | Value | Где взять |
|----------|-------|-----------|
| `REDIS_URL` | `rediss://...` | Upstash Dashboard |
| `BINGX_API_KEY` | ваш ключ | BingX API Management |
| `BINGX_API_SECRET` | ваш секрет | BingX API Management |
| `SHORT_TELEGRAM_BOT_TOKEN` | `123456:ABC...` | @BotFather |
| `SHORT_TELEGRAM_CHAT_ID` | `-100123...` | @userinfobot |

**Опциональные (уже заданы в render.yaml):**
- `MAX_PAIRS=500`
- `MIN_SHORT_SCORE=60`
- `SCAN_INTERVAL=180`
- `AUTO_TRADING_ENABLED=false`

#### 🟢 LONG бот (aegis-long-alpha)

**Обязательные (Secret):**
| Variable | Value | Где взять |
|----------|-------|-----------|
| `REDIS_URL` | `rediss://...` | Тот же что у SHORT |
| `BINGX_API_KEY` | ваш ключ | BingX API Management |
| `BINGX_API_SECRET` | ваш секрет | BingX API Management |
| `LONG_TELEGRAM_BOT_TOKEN` | `123456:ABC...` | Можно тот же |
| `LONG_TELEGRAM_CHAT_ID` | `-100123...` | Можно тот же |

**Опциональные (уже заданы в render.yaml):**
- `MAX_PAIRS=500`
- `MIN_LONG_SCORE=70`
- `SCAN_INTERVAL=240`
- `AUTO_TRADING_ENABLED=false`

---

### Шаг 4: Запуск и проверка

#### Проверка SHORT бота:
```bash
curl https://aegis-short-alpha.onrender.com/health
# {"status": "ok", "bot": "Aegis-Short-Alpha", "version": "1.0.0"}
```

#### Проверка LONG бота:
```bash
curl https://aegis-long-alpha.onrender.com/health
# {"status": "ok", "bot": "Aegis-Long-Alpha", "version": "1.0.0"}
```

#### Настройка Webhook (Telegram):
```
https://aegis-short-alpha.onrender.com/webhook/setup
https://aegis-long-alpha.onrender.com/webhook/setup
```

---

## 📊 Telegram Команды

### 🔴 SHORT бот команды:
```
/start, /help, /ping, /status
/balance, /positions, /closeall
/signals, /scan, /stats
/watchlist, /risk
/dca BTC — DCA уровни
/perf — Performance
```

### 🟢 LONG бот команды:
```
/start, /help, /ping, /status
/balance, /positions, /closeall
/signals, /scan, /stats
/watchlist, /risk
/dca BTC — DCA уровни (ниже входа)
/perf — Performance
```

---

## 🔧 Отличия SHORT vs LONG

| Параметр | 🔴 SHORT | 🟢 LONG |
|----------|---------|---------|
| Логика | Ищет вершины | Ищет дна |
| DCA уровни | ВЫШЕ входа | НИЖЕ входа |
| MIN_SCORE | 60 | 70 |
| SCAN_INTERVAL | 180s | 240s |
| SL_BUFFER | 2.5% | 3.0% |
| TP_LEVELS | [2.5,4,6.5,9,12,17] | [3,5,8,12,18,25] |
| BTC фильтр | — | Блок при -3%/ч |
| Детекторы | Pump, OI, Liq, Delta | Dump, Wyckoff, BSL, OI |

---

## 📈 Мониторинг логов

### Что видно в логах (Verbose Mode):

**SHORT пример:**
```
🔍 [BTC] 📊 [BASE_SCORER] score=45.0 | reasons: ['RSI high']
🔍 [BTC] ❌ [SHORT_FILTER] БЛОКИРОВКА: BTC тренд вверх | delta=-1.5
🔍 [ETH] 📊 [BASE_SCORER] score=58.0 | reasons: ['Pattern: Sweep']
🔍 [ETH] 📊 [REALTIME] base=58.0 bonus=+12.0 final=70.0
🔍 [ETH] ✅ [AEGIS] score=78.0 >= 60 | components: {...}
🟢 [SIGNAL] ETH: score=78.0 — сигнал создан!
```

**LONG пример:**
```
🟢 [SOL] 📊 [BTC_FILTER] BTC +1.2% — OK для LONG
🟢 [SOL] 📊 [BASE_SCORER] score=52.0 | reasons: ['RSI low']
🟢 [SOL] 📊 [REALTIME] base=52.0 bonus=+15.0 final=67.0
🟢 [SOL] ✅ [AEGIS] score=72.0 | components: {...}
🟢 [SIGNAL-LONG] SOL: score=72.0 — сигнал создан!
```

---

## 💰 Стоимость

| Сервис | Plan | Цена |
|--------|------|------|
| aegis-short-alpha | Starter | $7/мес |
| aegis-long-alpha | Starter | $7/мес |
| **ИТОГО** | | **$14/мес** |

**Бесплатный Redis:** Upstash (10k req/day)

---

## 🛡️ Безопасность

1. **DEMO режим** включён по умолчанию
2. **AUTO_TRADING=false** — только сигналы
3. **Circuit Breakers** — автостоп при просадке
4. **Оба бота** не конфликтуют (разные ключи Redis)

---

## 🔄 Обновление кода

```bash
cd "/Users/artemt/Downloads/NEW very HARD BOT/aegis-bots"

# Изменения в short-bot/
# Изменения в long-bot/
# Изменения в shared/

git add .
git commit -m "Update: описание изменений"
git push origin main

# Render автоматически деплоит оба сервиса!
```

---

## 🆘 Поддержка

**Логи проверять в:**
- Render Dashboard → Service → Logs
- Telegram: `/logs` (внутри бота)

**Экстренный стоп:**
- Telegram: `/emergency_stop`
- Или: `/pause` → `/closeall`

---

**Версия:** 1.0.0  
**Дата:** 23.04.2026  
**Статус:** Готов к деплою ✅
