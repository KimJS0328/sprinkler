FROM python:3.8-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /poetry-build
COPY poetry.lock* pyproject.toml* /poetry-build/

RUN pip install "poetry==1.6" \
    && poetry config virtualenvs.create false \
    && poetry lock --no-update \
    && poetry install --no-interaction --no-ansi
