from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.operators.python import get_current_context
from airflow.utils.dates import days_ago
from kubernetes.client import models as k8s


class Constants:
    DAG_NAME = "npi_data_pipeline"
    DAG_SCHEDULE = "0 15 * * *"  # 3 PM UTC or 8 AM PST - recurring schedule daily
    DAG_OWNER = "airflow"
    ORG = "ybor"  # {{ org-name }} - best generated by an archetype macro
    VENTURE = "playground"  # {{ venture-name }} - best generated by an archetype macro
    ENV = "dev"  # usually dev, stg or prod

    # CI/CD Configurations
    DOCKER_IMAGE_PREFIX = f"p6m.jfrog.io/{ORG}-{VENTURE}/applications"
    NPI_DOCKER_IMAGE = f"{DOCKER_IMAGE_PREFIX}/transforms-data-adapter-server:main"
    TRANSFORMS_SERVICE_ACCOUNT = f"transforms-sa-{ENV}-{ORG}-{VENTURE}"

    PULL_SECRET = "dockerconfig"  # "regcred"

    # other constants
    ISTIO_ANNOTATION = "sidecar.istio.io/inject"
    DO_NOT_EVICT = "karpenter.sh/do-not-evict"
    DO_NOT_CONSOLIDATE = "karpenter.sh/do-not-consolidate"
    DO_NOT_DISRUPT = "karpenter.sh/do-not-disrupt"


dag_parameters = {
    "mode": Param(
        "development",
        type="string",
        title="Development or Production run?",
        description="(mandatory) Development or Production ?",
    ),
    "output_database": Param(
        "playground",
        type="string",
        title="Destination database where the output should be register to",
        description="(mandatory) Database name for current Run",
    ),
    "table_name": Param(
        "raw_npi",
        type="string",
        title="Destination table where the output should be written to",
        description="(mandatory) Table name for current Run",
    ),
    "input_file": Param(
        "s3://airbyte-state-dev-us-east-2-ybor-playground/airbyte-sync/State/",
        type="string",
        title="File path of the input, typically this will be s3 file path or adls",
        description="(mandatory) input file path for Project run",
    ),
    "source_format": Param(
        "json",
        type="string",
        title="Input dataset format",
        description="(mandatory) File format for the input dataset",
    )
}


@task
def init_task(args) -> dict:
    from datetime import datetime

    print(f"init_task args = {args}")
    dag_run = get_current_context()["dag_run"]
    print(f"init_task dag_run = {dag_run}")

    now = datetime.utcnow().strftime("%Y-%m-%d-%H:%M:%S.%f")[:-3]

    if dag_run.external_trigger:
        print(f"As this run is manual, getting parameters from DAG.")
        rc = {
            "mode": dag_run.conf["mode"].strip(),
            "output_database": dag_run.conf["output_database"].strip(),
            "source_format": dag_run.conf["source_format"].strip(),
            "task_id": f"debug-run-{now}",
            "scheduled": False,
            "dag_run_id": dag_run.run_id,
            "dag_name": dag_run.dag_id,
            "input_file": dag_run.conf["input_file"].strip(),
            "table_name": dag_run.conf["table_name"].strip()
        }
        print(f"init_task output = {rc}")
        return rc

    else:
        print(f"As this run is scheduled, getting parameters from airflow.")
        rc = {
            "mode": "n/a",
            "output_database": "playground",
            "source_format": "json",
            "task_id": f"debug-run-{now}",
            "scheduled": True,
            "dag_run_id": dag_run.run_id,
            "dag_name": dag_run.dag_id,
        }
        print(f"init_task output = {rc}")
        return rc


@task.kubernetes(
    task_id="transformation_task",
    name="transformation_task",
    namespace="airflow",
    image=Constants.NPI_DOCKER_IMAGE,
    in_cluster=True,
    get_logs=True,
    service_account_name=Constants.TRANSFORMS_SERVICE_ACCOUNT,
    do_xcom_push=True,
    image_pull_secrets=[k8s.V1LocalObjectReference(Constants.PULL_SECRET)],
    is_delete_operator_pod=True,
    labels={"app": "transformations", "app_type": "driver"},
    annotations={
        Constants.ISTIO_ANNOTATION: "false", Constants.DO_NOT_EVICT: "true",
        Constants.DO_NOT_CONSOLIDATE: "true", Constants.DO_NOT_DISRUPT: "true"},
    container_resources=k8s.V1ResourceRequirements(
        requests={"memory": "1Gi", "cpu": "2.0", "ephemeral-storage": "1Gi"},
        limits={"memory": "2Gi", "cpu": "2.0", "ephemeral-storage": "3Gi"},
    ),
    priority_class_name="workflow",
)
def transform_task(args: dict) -> dict:

    import logging

    # use {{ org_name }}_{{ venture_name }} archetype macro
    from transforms_data_adapter.transforms.generic_table_registration import run

    logger = logging.getLogger(__name__)
    logging.basicConfig(level=logging.INFO)
    
    args = {
        "json_spec_file_path": "/app/src/transforms_data_adapter/transforms/resources/npi.json",
        "update_dict": {
            "input": args["input_file"].replace("s3://", "s3a://"),
            "output": args["output_database"]+"."+args["table_name"],
            "source_format": args["source_format"],
            "job_metadata_dag_name": args["dag_name"],
            "job_metadata_dag_run_id": args["dag_run_id"]
        },
    }


    logger.info(f"input = {args}")
    rc = run(args)
    logger.info(f"output = {rc}")

    return rc


@dag(
    dag_id=Constants.DAG_NAME,
    default_args={
        "owner": Constants.DAG_OWNER,
        "depends_on_past": False,
    },
    params=dag_parameters,
    start_date=days_ago(1),
    schedule_interval=None, # Constants.DAG_SCHEDULE,
    max_active_runs=1,
)
def workflow():

    # Note : This task will go away shortly
    @task
    def echo_task(args: dict) -> dict:
        print(args)
        return args

    # pipe output of each stage as input to the next stage

    scheduler_info = {
        "scheduler_from_date": "{{ prev_data_interval_end_success }}",
        "scheduler_to_date": "{{ data_interval_end }}",
    }

    input_args = init_task(scheduler_info)
    npi_transforms_output = transform_task(input_args)
    echo_task(npi_transforms_output)


workflow()
