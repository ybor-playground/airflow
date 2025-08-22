from datetime import datetime
import base64
import json

from airflow import DAG
from airflow.decorators import task
from airflow.providers.http.operators.http import SimpleHttpOperator, HttpOperator
from airflow.exceptions import AirflowFailException

# --- K8s secret reader ---
def read_x_api_key_from_k8s(secret_name: str = "langflow-runtime-key",
                            namespace: str = "airflow") -> str:
    """
    Returns the plaintext value for header 'langflow_api_key' from a Kubernetes Secret.
    Handles two layouts:
      A) Secret data contains key 'langflow_api_key' directly
      B) Secret data contains a JSON blob that includes {"langflow_api_key": "..."}
    """
    try:
        from kubernetes import client, config
    except Exception as e:
        raise RuntimeError("The 'kubernetes' package is required in the worker image") from e

    # In cluster first; fall back to local kubeconfig for dev
    try:
        config.load_incluster_config()
    except Exception:
        config.load_kube_config()

    v1 = client.CoreV1Api()
    sec = v1.read_namespaced_secret(secret_name, namespace)

    # Helper to base64-decode -> str
    def b64_to_str(b64s: str) -> str:
        return base64.b64decode(b64s).decode("utf-8")

    # Case A: direct key in .data
    if getattr(sec, "data", None) and "langflow_api_key" in sec.data:
        return b64_to_str(sec.data["langflow_api_key"])

    # Case B: try to find a JSON value that contains x-api-key
    for _, v in (sec.data or {}).items():
        try:
            maybe = json.loads(b64_to_str(v))
            if isinstance(maybe, dict) and "langflow_api_key" in maybe:
                return maybe["langflow_api_key"]
        except Exception:
            pass

    # Some clusters may put bytes in binary_data
    if getattr(sec, "binary_data", None) and "langflow_api_key" in sec.binary_data:
        return b64_to_str(sec.binary_data["langflow_api_key"])

    raise AirflowFailException(
        f"Could not find 'langflow_api_key' in secret '{secret_name}' (namespace '{namespace}')"
    )

# --- DAG ---
with DAG(
    dag_id="test_langflow_http_with_k8s_secret",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    render_template_as_native_obj=True,  # allows XComArg to stay as dicts/objects
    doc_md="""
    Test DAG that reads an API key from a Kubernetes Secret and calls an HTTP endpoint with it.
    """,
) as dag:

    @task(task_id="build_headers")
    def build_headers() -> dict:
        api_key = read_x_api_key_from_k8s(secret_name="langflow-runtime-key", namespace="airflow")
        return {"x-api-key": api_key, "Content-Type": "application/json"}

    headers_xcom = build_headers()
    # Make a simple GET to httpbin which echoes our headers back to us
    call_api = HttpOperator(
        task_id="call_langflow",
        http_conn_id="lang_flow_dev_http",
        endpoint="/api/v1/build/9e27f6fa-3b84-49a2-a6e3-4946b6926bb7/flow?log_builds=true&event_delivery=streaming",
        method="POST",
        headers=headers_xcom,
        data= json.dumps({
                "output_type": "text",
                "input_type": "text",
                "input_value": "hello world!"
            }),
        response_filter=lambda r: r.json(),
        log_response=True,
        do_xcom_push=True,
        dag=dag
    )

    @task(task_id="assert_header_echoed")
    def assert_header_echoed(resp: dict, headers: dict) -> None:
        echoed = resp.get("headers", {})
        expected = headers.get("x-api-key")
        got = echoed.get("X-Api-Key") or echoed.get("x-api-key")
        if got != expected:
            raise AirflowFailException(
                f"x-api-key mismatch. expected={expected!r} got={got!r} (echoed headers: {echoed})"
            )

    assert_header_echoed = assert_header_echoed(call_api.output, headers_xcom)

    headers_xcom >> call_api >> assert_header_echoed
