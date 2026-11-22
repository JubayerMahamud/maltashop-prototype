"""RabbitMQ helpers: durable topic exchange, persistent messages, manual acks,
retry-then-dead-letter, and an idempotent consumer wrapper."""
import asyncio
import json
import os
import uuid

import aio_pika

from common.logs import correlation_id_var, log

RABBIT_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")
EXCHANGE = "maltashop.events"
DLX = "maltashop.dead"
MAX_ATTEMPTS = int(os.getenv("MAX_ATTEMPTS", "3"))


class Broker:
    def __init__(self, conn):
        self.db = conn  # service's own database, used for idempotency
        self.connection = None
        self.channel = None
        self.exchange = None
        self.dlx = None

    async def start(self):
        for attempt in range(30):  # broker may still be starting
            try:
                self.connection = await aio_pika.connect_robust(RABBIT_URL)
                break
            except Exception:
                log("broker_not_ready", attempt=attempt + 1)
                await asyncio.sleep(2)
        else:
            raise RuntimeError("RabbitMQ unreachable")
        self.channel = await self.connection.channel(publisher_confirms=True)
        await self.channel.set_qos(prefetch_count=10)
        self.exchange = await self.channel.declare_exchange(
            EXCHANGE, aio_pika.ExchangeType.TOPIC, durable=True
        )
        self.dlx = await self.channel.declare_exchange(
            DLX, aio_pika.ExchangeType.FANOUT, durable=True
        )
        dlq = await self.channel.declare_queue("dead-letters", durable=True)
        await dlq.bind(self.dlx)

    async def publish(self, routing_key, body, message_id=None):
        """Persistent message; the broker confirms it was stored before we continue."""
        message = aio_pika.Message(
            body=json.dumps(body).encode(),
            content_type="application/json",
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            message_id=message_id or str(uuid.uuid4()),
            correlation_id=correlation_id_var.get(),
            headers={"attempts": 0},
        )
        await self.exchange.publish(message, routing_key=routing_key)
        log("event_published", routing_key=routing_key, message_id=message.message_id)

    async def subscribe(self, queue_name, routing_keys, handler):
        queue = await self.channel.declare_queue(
            queue_name,
            durable=True,
            arguments={"x-dead-letter-exchange": DLX},
        )
        for key in routing_keys:
            await queue.bind(self.exchange, routing_key=key)

        async def on_message(message: aio_pika.IncomingMessage):
            correlation_id_var.set(message.correlation_id or "-")
            # Idempotent consumer: a redelivered message is acknowledged and skipped.
            seen = self.db.execute(
                "SELECT 1 FROM processed_messages WHERE message_id = ?",
                (message.message_id,),
            ).fetchone()
            if seen:
                log("duplicate_ignored", message_id=message.message_id)
                await message.ack()
                return
            try:
                await handler(message.routing_key, json.loads(message.body))
                self.db.execute(
                    "INSERT OR IGNORE INTO processed_messages VALUES (?)",
                    (message.message_id,),
                )
                log("event_consumed", routing_key=message.routing_key,
                    message_id=message.message_id)
                await message.ack()
            except Exception as exc:  # retry a few times, then dead-letter
                attempts = int((message.headers or {}).get("attempts", 0)) + 1
                log("handler_failed", error=str(exc), attempts=attempts)
                if attempts >= MAX_ATTEMPTS:
                    await message.reject(requeue=False)  # -> dead-letter queue
                else:
                    retry = aio_pika.Message(
                        body=message.body,
                        content_type=message.content_type,
                        delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                        message_id=message.message_id,
                        correlation_id=message.correlation_id,
                        headers={"attempts": attempts},
                    )
                    await self.channel.default_exchange.publish(
                        retry, routing_key=queue_name
                    )
                    await message.ack()

        await queue.consume(on_message)

    async def stop(self):
        if self.connection:
            await self.connection.close()
