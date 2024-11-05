from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import dag, task
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.models import Variable

DAG_ID = "NPI_Airbyte_job"
CONN_ID = Variable.get("AIRBYTE_NPI_S3_CONN_ID", default_var="default_connection_id")  # Retrieve the connection ID from an Airflow variable

@dag(
    dag_id=DAG_ID,
    schedule="@daily",
    start_date=datetime(2024, 11, 5),
    dagrun_timeout=timedelta(minutes=60),
    tags=["airbyte", "npi", "ingest"],
    catchup=False,
    is_paused_upon_creation=True
)
def NPI_S3_ingestion():

    # First task: A simple print task
    @task
    def start_message():
        print("Starting the NPI data sync process.")

    # Airbyte task to sync data
    npi_sync_task = AirbyteTriggerSyncOperator(
        task_id="sync_npi_healthcare_professionals_to_s3",
        connection_id=CONN_ID,
    )

    # Last task: A simple print task
    @task
    def end_message():
        print("NPI data sync process completed.")

    # Set task dependencies to control execution flow
    start_task = start_message()
    start_task >> npi_sync_task >> end_message()  # Define task execution order

dag = NPI_S3_ingestion()
