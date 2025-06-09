from diagrams import Diagram, Edge
from diagrams.programming.framework import FastAPI
from diagrams.onprem.database import PostgreSQL
from diagrams.generic.blank import Blank

with Diagram('pgqueuer_component', filename='docs/images/pgqueuer_component', show=False, outformat='svg') as diag:
    producer = Blank('Producer')
    fastapi = FastAPI('Entrypoint Router')
    db = PostgreSQL('PostgreSQL Broker')
    consumer = Blank('Consumer')

    producer >> Edge(label='enqueue job') >> db
    db >> Edge(label='NOTIFY event') >> fastapi
    fastapi >> Edge(label='dispatch job') >> consumer
