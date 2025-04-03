import psycopg
from flask import Flask, Response, g, jsonify, request

from pgqueuer.db import SyncPsycopgDriver, dsn
from pgqueuer.queries import SyncQueries

app = Flask(__name__)


def get_driver():
    if "driver" not in g:
        conn = psycopg.connect(dsn(), autocommit=True)
        g.driver = SyncPsycopgDriver(conn)

    return g.driver


@app.teardown_appcontext
def teardown_db(exception):
    driver = g.pop("driver", None)

    if driver is not None:
        driver._connection.close()


@app.route("/enqueue", methods=["POST"])
def enqueue() -> Response:
    queries = SyncQueries(get_driver())
    data = request.get_json(force=True)
    entrypoint = data.get("entrypoint", "default")
    payload = data.get("payload")
    priority = int(data.get("priority", 0))

    job_ids = queries.enqueue(entrypoint, payload.encode() if payload else None, priority)
    return jsonify({"job_ids": [str(jid) for jid in job_ids]})


@app.route("/queue_size")
def queue_size() -> Response:
    queries = SyncQueries(get_driver())
    stats = queries.queue_size()
    return jsonify(
        [
            {
                "entrypoint": s.entrypoint,
                "priority": s.priority,
                "status": s.status,
                "count": s.count,
            }
            for s in stats
        ]
    )


if __name__ == "__main__":
    app.run(debug=True)
