from typing import cast

import psycopg
from flask import Flask, Response, g, jsonify, request

from pgqueuer import DBSettings
from pgqueuer.db import SyncPsycopgDriver
from pgqueuer.queries import SyncQueries


def create_app() -> Flask:
    app = Flask(__name__)
    # Read the PGQUEUER_* env vars once, not on every request.
    settings = DBSettings()

    def get_driver() -> SyncPsycopgDriver:
        if "driver" not in g:
            conn = psycopg.connect("", autocommit=True)
            g.driver = SyncPsycopgDriver(conn)

        return cast(SyncPsycopgDriver, g.driver)

    @app.teardown_appcontext
    def teardown_db(_exception: BaseException | None) -> None:
        driver = cast(SyncPsycopgDriver | None, g.pop("driver", None))

        if driver is not None:
            driver._connection.close()  # noqa: SLF001 - the example owns this connection

    @app.route("/enqueue", methods=["POST"])
    def enqueue() -> Response:
        queries = SyncQueries(get_driver(), settings)
        data = request.get_json(force=True)
        entrypoint = data.get("entrypoint", "default")
        payload = data.get("payload")
        priority = int(data.get("priority", 0))

        job_ids = queries.enqueue(entrypoint, payload.encode() if payload else None, priority)
        return jsonify({"job_ids": [str(jid) for jid in job_ids]})

    @app.route("/queue_size")
    def queue_size() -> Response:
        queries = SyncQueries(get_driver(), settings)
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

    return app


if __name__ == "__main__":
    create_app().run(debug=True)
