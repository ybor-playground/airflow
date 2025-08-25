from datetime import datetime
from urllib.parse import urljoin
import os
import requests

from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowFailException


with DAG(
    dag_id="langflow_rest_call_with_requests",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    render_template_as_native_obj=True,
) as dag:

    @task
    def build_headers() -> dict:
        api_key = os.getenv("langflow_api_key")
        if not api_key:
            raise AirflowFailException("Missing env var: langflow_api_key")
        return {
            "x-api-key": api_key,
            "Content-Type": "application/json",
        }

    @task
    def call_rest(
        base_url: str,
        endpoint: str,
        method: str,
        headers: dict,
        params: dict | None = None,
        body: dict | None = None,
        timeout: int = 60,
        verify: bool = True,
    ) -> dict:
        """
        Makes an HTTP request and returns parsed JSON.
        The returned dict is stored in XCom automatically.
        """
        # robust URL join no matter how slashes are passed
        base = base_url.rstrip("/") + "/"
        path = endpoint.lstrip("/")
        url = urljoin(base, path)

        resp = requests.request(
            method=method.upper(),
            url=url,
            headers=headers or {},
            params=params,
            json=body,
            timeout=timeout,
            verify=verify,
        )
        resp.raise_for_status()
        try:
            return resp.json()  # <-- This is what downstream tasks will receive via XCom
        except ValueError as e:
            raise AirflowFailException(f"Response was not JSON: {e}\nFirst 200 chars: {resp.text[:200]}")

    @task
    def downstream_use(json_payload: dict) -> None:
        # example: assert something in the returned JSON
        if not json_payload:
            raise AirflowFailException("Empty JSON payload from upstream.")
        # do whatever you need with json_payload
        print("Keys in response:", list(json_payload.keys()))

    headers = build_headers()

    response_json = call_rest(
        base_url="https://langflow-ide.aks.westus2.azure.dev.ybor-playground.p6m.run",
        endpoint="/api/v1/build/9e27f6fa-3b84-49a2-a6e3-4946b6926bb7/flow?log_builds=true&event_delivery=streaming",
        method="POST",
        headers=headers,
        body={
            "output_type": "text",
            "input_type": "text",
            "input_value": "hello world!",
        },
        # set verify=False if you must skip TLS verification
        verify=False,
        timeout=60,
    )

    downstream_use(response_json)
