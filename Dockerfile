# syntax=docker/dockerfile:1
FROM ghcr.io/terradue/ogc-eo-application-package-hands-on/stage:1.3.2

RUN --mount=type=cache,target=/root/.cache/pip python -m pip install --upgrade pip

COPY requirements.txt .
RUN --mount=type=cache,target=/root/.cache/pip python -m pip install -r requirements.txt

COPY stageout.py .

CMD ["python", "stageout.py"]