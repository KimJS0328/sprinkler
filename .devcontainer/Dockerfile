FROM python:3.8

ENV PYTHONUNBUFFERED=1

WORKDIR /project_build
COPY poetry.lock* pyproject.toml* /project_build/

RUN apt update && apt install -y graphviz graphviz-dev \
    && pip install "poetry==1.6" "openai==0.28.0" \
    && poetry config virtualenvs.create false \
    && poetry lock --no-update \
    && poetry install --no-interaction --no-ansi --all-extras
