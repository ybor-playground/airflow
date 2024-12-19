from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import dag, task
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.models import Variable

DAG_ID = "transfer_replication_Airbyte_job"

# Set the Variable: AIRBYTE_transfer_replication_CONN_ID in the Airflow UI under Admin > Variables or via the CLI:
CONN_ID = Variable.get("AIRBYTE_transfer_replication_CONN_ID", default_var="default_connection_id")  # Retrieve the connection ID from an Airflow variable

@dag(
    dag_id=DAG_ID,
    schedule="@daily",
    start_date=datetime(2024, 11, 5),
    dagrun_timeout=timedelta(minutes=60),
    tags=["airbyte", "Transfers", "ingest"],
    catchup=False,
    is_paused_upon_creation=True
)
def transfer_replication_func():

    # First task: A simple print task
    @task
    def start_message():
        print("Starting the transfers replication")
        return True

    # Airbyte task to sync data
    transfer_replication_task = AirbyteTriggerSyncOperator(
        task_id="transfers_replication_from_Azure_EDW_to_Blob",
        connection_id=CONN_ID,
    )

    # Last task: A simple print task
    @task
    def end_message():
        print("Transfers data sync process completed.")

    # Set task dependencies to control execution flow
    start_task = start_message()
    start_task >> transfer_replication_task >> end_message()  # Define task execution order

dag = transfer_replication_func()
