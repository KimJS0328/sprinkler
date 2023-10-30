FROM python:3.8

ENV PYTHONUNBUFFERED=1

WORKDIR /poetry-build
COPY poetry.lock* pyproject.toml* /poetry-build/

RUN apt update \
    && apt install -y graphviz graphviz-dev \
    && pip install "poetry==1.6" \
    && poetry config virtualenvs.create false \
    && poetry install --no-interaction --no-ansi --all-extras
