# 🚀 Aegis Dashboard Deployment Guide v3.3

## Что нового в v3.3
- ✅ **30-минутное кэширование** — снижает нагрузку на Redis
- ✅ **Health-check перед каждым запросом** — проверка соединения
- ✅ **Auto-reconnect** — автоматическое переподключение с exponential backoff
- ✅ **Connection pool** — пул соединений (10 штук)

---

## 📋 Предварительные требования

1. **GitHub аккаунт** — для размещения кода
2. **Upstash Redis** — 2 базы (LONG и SHORT боты)
   - URL формат: `redis://default:PASSWORD@HOST:PORT`
3. **Хостинг** — выбери один:
   - ☁️ **Render** (рекомендую) — бесплатно
   - ☁️ **Railway** — бесплатно
   - ☁️ **Heroku** — $7/месяц
   - 🖥️ **VPS** — свой сервер

---

## 🗂️ Структура проекта

```
dashboard-server/
├── main.py              # FastAPI сервер v3.3
├── index.html           # Фронтенд UI
├── requirements.txt     # Python зависимости
├── render.yaml          # Render.com конфиг
└── DEPLOY.md           # Эта инструкция
```

---

## 🔧 Шаг 1: Подготовка кода

### 1.1 Создай GitHub репозиторий

```bash
# Если ещё нет репозитория
cd /Users/artemt/Downloads/aegis-short-alpha-main/dashboard-server
git init
git add .
git commit -m "Dashboard v3.3 — caching + health-check + reconnect"

# Создай репо на GitHub и добавь remote
git remote add origin https://github.com/YOUR_USERNAME/aegis-dashboard.git
git push -u origin main
```

### 1.2 Убедись что `.gitignore` есть в корне

```bash
# В корне проекта /Users/artemt/Downloads/aegis-short-alpha-main/
echo "__pycache__/
*.pyc
.env
venv/
.DS_Store" > .gitignore
```

---

## 🔑 Шаг 2: Получение Redis URL

### Upstash Redis (рекомендую)

1. Зайди на https://upstash.com
2. Создай 2 базы:
   - `aegis-long-redis`
   - `aegis-short-redis`
3. Для каждой базы нажми **"Connect"** → **"redis-cli"**
4. Скопируй URL:
   ```
   redis://default:AXXXXXX@XXXX.upstash.io:6379
   ```

---

## ☁️ Шаг 3: Деплой на Render.com

### 3.1 Подготовка render.yaml

Файл уже есть — `@/Users/artemt/Downloads/aegis-short-alpha-main/dashboard-server/render.yaml`:

```yaml
services:
  - type: web
    name: aegis-dashboard
    env: python
    buildCommand: pip install -r requirements.txt
    startCommand: uvicorn main:app --host 0.0.0.0 --port $PORT
    envVars:
      - key: LONG_REDIS_URL
        sync: false  # Будет добавлен вручную
      - key: SHORT_REDIS_URL
        sync: false
      - key: CACHE_TTL
        value: 1800  # 30 минут
    plan: free
```

### 3.2 Деплой

1. Зайди на https://render.com
2. **New +** → **Blueprint**
3. Подключи GitHub репозиторий
4. Render автоматически найдёт `render.yaml`
5. Нажми **Apply**

### 3.3 Добавление Environment Variables

После создания сервиса:

1. Перейди в **Dashboard** → **aegis-dashboard**
2. **Environment** → **Add Environment Variable**
3. Добавь:
   ```
   LONG_REDIS_URL=redis://default:YOUR_LONG_PASS@host.upstash.io:6379
   SHORT_REDIS_URL=redis://default:YOUR_SHORT_PASS@host.upstash.io:6379
   CACHE_TTL=1800
   ```
4. Нажми **Save Changes**
5. Сервис автоматически перезапустится

### 3.4 Проверка

Открой URL (например `https://aegis-dashboard-xxx.onrender.com`):

- `/health` — должен показать:
  ```json
  {
    "status": "ok",
    "version": "3.3.0",
    "redis": {
      "long": {"connected": true, "message": "OK (Redis v7.x.x)"},
      "short": {"connected": true, "message": "OK (Redis v7.x.x)"}
    },
    "cache": {
      "total_keys": 0,
      "valid": 0,
      "expired": 0,
      "ttl_seconds": 1800
    }
  }
  ```

---

## 🚂 Шаг 4: Деплой на Railway (альтернатива)

### 4.1 Railway CLI

```bash
# Установка
npm install -g @railway/cli

# Логин
railway login

# В папке проекта
railway init
railway up

# Добавь переменные
railway variables set LONG_REDIS_URL="redis://..."
railway variables set SHORT_REDIS_URL="redis://..."
railway variables set CACHE_TTL="1800"
```

---

## 🐳 Шаг 5: Деплой на VPS (свой сервер)

### 5.1 Установка

```bash
# SSH на сервер
ssh user@your-server.com

# Клонируй репо
git clone https://github.com/YOUR_USERNAME/aegis-dashboard.git
cd aegis-dashboard/dashboard-server

# Установи Python 3.10+
sudo apt update && sudo apt install python3-pip python3-venv

# Создай venv
python3 -m venv venv
source venv/bin/activate

# Установи зависимости
pip install -r requirements.txt
```

### 5.2 Environment Variables

```bash
# Создай .env файл
cat > .env << EOF
LONG_REDIS_URL=redis://default:PASS@host.upstash.io:6379
SHORT_REDIS_URL=redis://default:PASS@host.upstash.io:6379
CACHE_TTL=1800
EOF
```

### 5.3 Systemd сервис

```bash
sudo tee /etc/systemd/system/aegis-dashboard.service > /dev/null << EOF
[Unit]
Description=Aegis Dashboard API
After=network.target

[Service]
Type=simple
User=www-data
WorkingDirectory=/home/user/aegis-dashboard/dashboard-server
Environment="PATH=/home/user/aegis-dashboard/dashboard-server/venv/bin"
EnvironmentFile=/home/user/aegis-dashboard/dashboard-server/.env
ExecStart=/home/user/aegis-dashboard/dashboard-server/venv/bin/uvicorn main:app --host 0.0.0.0 --port 10000
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable aegis-dashboard
sudo systemctl start aegis-dashboard
sudo systemctl status aegis-dashboard
```

### 5.4 Nginx (опционально)

```nginx
server {
    listen 80;
    server_name dashboard.yourdomain.com;

    location / {
        proxy_pass http://127.0.0.1:10000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
    }
}
```

---

## 📊 Environment Variables

| Переменная | Обязательная | Описание | Пример |
|------------|--------------|----------|--------|
| `LONG_REDIS_URL` | ✅ | Redis long-bot | `redis://default:pass@host:6379` |
| `SHORT_REDIS_URL` | ✅ | Redis short-bot | `redis://default:pass@host:6379` |
| `CACHE_TTL` | ❌ | Кэш TTL секунд | `1800` (30 мин) |
| `PORT` | ❌ | Порт сервера | `10000` |

---

## 🔍 API Endpoints

| Endpoint | Описание | Кэширование |
|----------|----------|-------------|
| `GET /` | Dashboard UI | Нет |
| `GET /health` | Health check + Redis status | Нет |
| `GET /api/overview` | P&L, статистика | ✅ 30 мин |
| `GET /api/positions` | Открытые позиции | ✅ 30 мин |
| `GET /api/history` | История сделок | ✅ 30 мин |
| `POST /api/cache/invalidate` | Сбросить кэш | Нет |
| `GET /api/cache/stats` | Статистика кэша | Нет |
| `GET /api/debug/{bot}` | Отладка | Нет |

---

## 🧪 Тестирование

```bash
# Локально
cd dashboard-server
export LONG_REDIS_URL="redis://..."
export SHORT_REDIS_URL="redis://..."
python main.py

# Проверь в другом терминале
curl http://localhost:10000/health
curl http://localhost:10000/api/overview
curl http://localhost:10000/api/cache/stats
```

---

## 🚨 Troubleshooting

### Redis не подключается
```bash
# Проверь URL
echo $LONG_REDIS_URL

# Тест redis-cli
redis-cli -u $LONG_REDIS_URL PING

# Логи Render/Railway
render logs --tail
railway logs
```

### Кэш не сбрасывается
```bash
# Ручной сброс
curl -X POST https://your-dashboard.com/api/cache/invalidate
```

### Много запросов к Redis
Проверь `CACHE_TTL` — должен быть `1800` (30 минут).

---

## 📱 Frontend обновление

Убедись что `index.html` обновляется раз в 30 минут:

```javascript
// В index.html
const REFRESH_INTERVAL = 30 * 60 * 1000; // 30 минут

async function loadData() {
    const res = await fetch('/api/overview');
    const data = await res.json();
    // ... отобрази данные
}

// Загрузка при старте
loadData();

// И обновление раз в 30 минут
setInterval(loadData, REFRESH_INTERVAL);
```

---

## 🎉 Готово!

Твой дашборд теперь:
- ⚡ Быстрый (кэширование)
- 🔄 Надёжный (auto-reconnect)
- 💚 Мониторится (health-check)
- 💰 Экономит ресурсы Redis

**URL для браузера:** `https://your-app.com`

---

Нужна помощь? Пиши в Telegram или проверь логи через `GET /health`!
