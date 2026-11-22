# Stage 1: install dependencies into a virtual environment
FROM python:3.12-slim AS build
WORKDIR /build
COPY requirements.txt .
RUN python -m venv /venv && /venv/bin/pip install --no-cache-dir -r requirements.txt

# Stage 2: small runtime image with no build tools
FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1 PATH="/venv/bin:$PATH"
# Non-root user: a compromised service cannot modify the system
RUN useradd --create-home --uid 10001 app && mkdir /data && chown app /data
WORKDIR /app
COPY --from=build /venv /venv
COPY common ./common
COPY payment ./payment
USER app
EXPOSE 8000
HEALTHCHECK --interval=10s --timeout=3s --start-period=15s --retries=5 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/health')"
CMD ["uvicorn", "payment.main:app", "--host", "0.0.0.0", "--port", "8000"]
