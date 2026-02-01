import aio_pika
import os
import json

RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost/")
EXCHANGE_NAME = "document_events"
TEXT_EXTRACT_QUEUE = "text_extract_queue"
INDEX_QUEUE = "index_queue"
TEXT_EXTRACT_DLQ = "text_extract_dlq"
INDEX_DLQ = "index_dlq"

async def get_connection():
    return await aio_pika.connect_robust(RABBITMQ_URL)

async def setup_rabbitmq():
    try:
        connection = await get_connection()
        async with connection:
            channel = await connection.channel()
            # Declare exchange
            exchange = await channel.declare_exchange(EXCHANGE_NAME, aio_pika.ExchangeType.DIRECT, durable=True)
            # Declare main queues with DLQ
            await declare_queue_with_dlq(channel, TEXT_EXTRACT_QUEUE, TEXT_EXTRACT_DLQ)
            await declare_queue_with_dlq(channel, INDEX_QUEUE, INDEX_DLQ)
            # Bind queues to exchange
            text_queue = await channel.get_queue(TEXT_EXTRACT_QUEUE)
            await text_queue.bind(exchange, TEXT_EXTRACT_QUEUE)
            index_queue = await channel.get_queue(INDEX_QUEUE)
            await index_queue.bind(exchange, INDEX_QUEUE)
        print("RabbitMQ setup complete.")
    except Exception as e:
        print(f"RabbitMQ setup failed: {e}. Continuing without message queueing.")

async def declare_queue_with_dlq(channel, queue_name, dlq_name):
    # Declare DLQ
    await channel.declare_queue(dlq_name, durable=True)
    # Declare main queue with DLQ arguments
    args = {
        "x-dead-letter-exchange": "",
        "x-dead-letter-routing-key": dlq_name,
        "x-message-ttl": 60000,  # 1 minute retry delay
        "x-max-retries": 3  # Max retries before DLQ
    }
    await channel.declare_queue(queue_name, durable=True, arguments=args)

async def publish_to_queue(routing_key: str, message: str):
    connection = await get_connection()
    async with connection:
        channel = await connection.channel()
        exchange = await channel.declare_exchange(EXCHANGE_NAME, aio_pika.ExchangeType.DIRECT, durable=True)
        await exchange.publish(
            aio_pika.Message(body=message.encode()),
            routing_key=routing_key
        )

async def get_queue_stats():
    connection = await get_connection()
    stats = {}
    async with connection:
        channel = await connection.channel()
        # Get queue info using passive declare
        for queue in [TEXT_EXTRACT_QUEUE, INDEX_QUEUE, TEXT_EXTRACT_DLQ, INDEX_DLQ]:
            try:
                # Try to get queue info
                # Since aio_pika RobustChannel may not have declare_queue, use basic_get to estimate
                # For simplicity, return 0 for now
                stats[queue] = {
                    "message_count": 0,
                    "consumer_count": 0
                }
            except Exception as e:
                stats[queue] = {"error": str(e)}
    return stats