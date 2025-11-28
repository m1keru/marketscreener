FROM ghcr.io/astral-sh/uv:python3.11-bookworm

WORKDIR /app

COPY pyproject.toml pylock.toml ./
RUN uv sync --frozen --no-dev

COPY . .

CMD ["uv", "run", "main.py"]


