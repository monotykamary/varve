FROM python:3.12.14-slim-bookworm@sha256:782412e85d0f0984994c290652577d4018aff08145c85b262bb63dc0c7522254

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1
WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir --requirement requirements.txt
COPY core.py benchmark.py Dockerfile ./
RUN mkdir -p /results && useradd --create-home --uid 10001 benchmark && chown benchmark:benchmark /results
USER benchmark
CMD ["sleep", "infinity"]
