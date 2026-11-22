# MaltaShop place-order prototype

A small prototype of MaltaShop's "place an order" flow, built for the Unit 5 assignment (Service-Oriented Architecture and Microservices Fundamentals).

## What it is

Four small Python (FastAPI) services that talk through a RabbitMQ message broker. Each service owns its own SQLite database file. There is no shared database.

| Service | Role | Port |
|---|---|---|
| order | REST endpoint `POST /orders`; runs the order saga | 8000 |
| inventory | Reserves stock, and releases it if the order fails | 8001 |
| payment | Charges through a simulated card provider (declines amounts above 2000) | 8002 |
| notification | Subscribes to order events and records an email | 8003 |
| rabbitmq | Message broker (management UI on 15672) | 5672 / 15672 |

### The flow

1. `POST /orders` saves the order as `PENDING` and publishes `order.placed` and `stock.reserve`.
2. Inventory reserves stock and publishes `stock.reserved` (or `stock.rejected`).
3. Order asks Payment to charge by publishing `payment.charge`.
4. Payment publishes `payment.succeeded` or `payment.failed`.
5. On success the order becomes `CONFIRMED`. On failure Order publishes `stock.release` (the compensating action) and the order becomes `CANCELLED`.
6. Notification receives `order.placed`, `order.confirmed` and `order.cancelled`. Checkout never calls it.

## How to run

Requirements: Docker with Compose v2.

```bash
cp .env.example .env
docker compose up --build
```

In a second terminal:

```bash
./scripts/smoke-test.sh          # runs a successful order, a declined order and a duplicate request
curl -X POST localhost:8000/orders -H "Content-Type: application/json" \
  -H "X-Correlation-ID: demo-1" \
  -d '{"customer_id":"c-1","email":"a@b.mt","items":[{"sku":"KETTLE-1","qty":1}],"amount":100}'
curl localhost:8000/orders/<order_id>
curl localhost:8001/stock
curl localhost:8003/notifications
docker compose logs order notification | grep demo-1   # same correlation ID in both services
```

Run the unit tests without Docker:

```bash
pip install -r requirements.txt pytest
python -m pytest tests -q
```

## Environment variables

| Variable | Used by | Meaning | Default |
|---|---|---|---|
| `RABBITMQ_USER` | compose, all services | Broker user | none, set in `.env` |
| `RABBITMQ_PASS` | compose, all services | Broker password | none, set in `.env` |
| `ENVIRONMENT` | all services | Written into every log line | `local` |
| `APP_VERSION` | all services | Image version (commit SHA in CI), written into logs | `dev` |
| `RABBITMQ_URL` | each service | Full broker URL, built by compose | set by compose |
| `DB_PATH` | each service | Location of the service's own database file | `/data/service.db` |
| `SERVICE_NAME` | each service | Name written into logs | set by compose |
| `MAX_ATTEMPTS` | each service | Handler retries before a message goes to the dead-letter queue | `3` |

## Repository layout

```
common/        shared helpers: JSON logging, database, RabbitMQ wrapper
order/ inventory/ payment/ notification/   one folder and one Dockerfile per service
tests/         unit tests
scripts/       smoke-test.sh
.github/workflows/ci.yml   build, test, scan and integration-test pipeline
```

## Known limits (prototype only)

- SQLite is used to keep the prototype small. Production would use managed PostgreSQL.
- Messages are published after the database write, not atomically with it. Production would add the transactional outbox pattern.
- No API gateway, authentication or TLS.
- The payment provider is simulated.
