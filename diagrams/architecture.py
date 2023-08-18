from diagrams import Diagram, Cluster
from diagrams.generic.compute import Rack
from diagrams.onprem.workflow import Airflow
from diagrams.azure.database import DataLake

with Diagram("Schools Ranking Data Engineering Project"):
    api = Rack("API")
    airflow = Airflow("Airflow")

    with Cluster("Delta Lake"):
        raw = DataLake("Raw Layer")
        silver = DataLake("Silver Layer")
        gold = DataLake("Gold Layer")

    api >> airflow >> raw >> airflow >> silver >> airflow >> gold
