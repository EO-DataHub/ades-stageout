# syntax=docker/dockerfile:1
FROM python:3.11-slim-bullseye

RUN rm -f /etc/apt/apt.conf.d/docker-clean; \
    echo 'Binary::apt::APT::Keep-Downloaded-Packages "true";' > /etc/apt/apt.conf.d/keep-cache

RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt,sharing=locked \
    apt-get update -y && apt-get upgrade -y

RUN --mount=type=cache,target=/root/.cache/pip python -m pip install --upgrade pip

WORKDIR /app

COPY requirements.txt .
RUN --mount=type=cache,target=/root/.cache/pip python -m pip install -r requirements.txt

COPY /stageout ./stageout
RUN --mount=type=cache,target=/root/.cache/pip python -m pip install ./stageout

ENV PYTHONPATH="/app"

CMD ["python", "-m", "stageout"]