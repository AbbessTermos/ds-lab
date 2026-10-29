import json, os
import pika

URL = os.environ.get("RABBIT_URL", "")
Q = "dfs-jobs"


def _conn():
    return pika.BlockingConnection(pika.URLParameters(URL))


def publish(jobs):
    c = _conn()
    ch = c.channel()
    ch.queue_declare(Q, durable=True)
    for j in jobs:
        ch.basic_publish("", Q, json.dumps(j), pika.BasicProperties(delivery_mode=2))
    c.close()


def depth():
    c = _conn()
    n = c.channel().queue_declare(Q, durable=True).method.message_count
    c.close()
    return n


def consume(handler):
    c = _conn()
    ch = c.channel()
    ch.queue_declare(Q, durable=True)
    ch.basic_qos(prefetch_count=1)

    def cb(ch, method, props, body):
        try:
            handler(json.loads(body))
        except Exception as e:
            print("job failed", e, flush=True)
        ch.basic_ack(method.delivery_tag)

    ch.basic_consume(Q, cb)
    ch.start_consuming()
