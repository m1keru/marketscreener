# Trading Consultant (uv edition)

## Setup

1. Create a `.env` file with the required keys (`OPENAI_API_KEY`, optional Telegram settings).
2. Sync dependencies locally:
   ```shell
   uv sync
   ```
3. Run a single analysis pass (respecting `.env` values):
   ```shell
   uv run main.py --once --limit 20
   ```
4. Чтобы увидеть подробный прогон фундаментальных фильтров, выставьте
   `DEBUG_MODE=1` в `.env`. Скрипт покажет метрики и результат каждого сравнения.

## Docker

```shell
docker compose up --build -d
```

The container mounts `history.json` and `reports/` from the host so daily state persists.

