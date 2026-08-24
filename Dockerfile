FROM python:3.12-slim

ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

RUN groupadd --gid 10001 scam-sniffer \
    && useradd --uid 10001 --gid scam-sniffer --no-create-home scam-sniffer

COPY pyproject.toml ./
COPY src ./src

RUN pip install --no-cache-dir .

USER scam-sniffer

CMD ["python", "-m", "scam_sniffer.cli"]
