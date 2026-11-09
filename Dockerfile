FROM python:3.11-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

RUN useradd --create-home --uid 10001 --shell /usr/sbin/nologin extract

WORKDIR /app
COPY pyproject.toml README.md LICENSE ./
COPY src ./src
RUN pip install --no-cache-dir .

USER extract
ENTRYPOINT ["python", "-m", "lft_pipelines.extract"]
