"""Worker: takes repair, rebalance and garbage-collection jobs from RabbitMQ."""
import os, socket, time
import app
import mq

ME = socket.gethostname()


def handle(job):
    t = job["type"]
    if t == "gc":
        nodes = {n[0]: n[1] for n in app.all_nodes()}
        for n, ch in job["items"]:
            if n in nodes:
                app.node_delete(nodes[n], ch)
    else:
        with app.db() as c:
            c.execute("SELECT loc FROM chunks WHERE hash=%s AND kind=%s", (job["hash"], job["kind"]))
            row = c.fetchone()
        if not row or row[0] is None:
            return
        live = app.alive()
        if t == "repair":
            app.repair_chunk(job["hash"], job["kind"], row[0], live)
        elif t == "rebalance":
            app.rebalance_chunk(job["hash"], job["kind"], row[0], live)
    app.bump(f"jobs_{t}")
    app.bump(f"jobs_done_by_{ME}")


if __name__ == "__main__":
    app.init_db()
    while True:
        try:
            print(ME, "waiting for jobs", flush=True)
            mq.consume(handle)
        except Exception as e:
            print("rabbitmq not ready:", e, flush=True)
            time.sleep(3)
