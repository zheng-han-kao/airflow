import logging
import os
from datetime import datetime, timedelta
from pathlib import Path
import json
import http.client
import urllib.parse

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

log = logging.getLogger(__name__)

dag = DAG(
    "example_using_k8s_executor",
    schedule_interval="0 1 * * *",
    catchup=False,
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        "start_date": datetime(2022, 9, 27),
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 2,
        "retry_delay": timedelta(seconds=30),
        "sla": timedelta(hours=23),
    },
)

# set http connect timeout
HTTP_CONNECT_TIMEOUT = 300


def __parsing_host_port_path__(url):
    # parsing url value
    parsed_url = urllib.parse.urlparse(url)
    # initial value
    host = parsed_url.netloc
    port = 80
    path = parsed_url.path
    if ":" in parsed_url.netloc:
        host = parsed_url.netloc.split(":")[0]
        port = parsed_url.netloc.split(":")[1]
    return host, port, path

def __ElecTransSummService__(api_url: str, fab: str, Stage: str, start_time: str, end_time: str):
    # initial response
    response = None
    # parsing API URL
    host, port, path = __parsing_host_port_path__(api_url)
    http_cli = http.client.HTTPConnection(host, port, timeout=HTTP_CONNECT_TIMEOUT)

    try:
        http_cli.request("POST", path, body=json.dumps({"Fab": fab,
                                                        "Stage": Stage,
                                                        "Start_Time": start_time,
                                                        "End_Time": end_time
                                                        }),
                         headers={"Content-type": "application/json"})

        response = http_cli.getresponse()
        if response.status == 200:
            rsp_data = response.read().decode()
            '''************************************'''
            if rsp_data == "{}":
                return None
            else:
                return json.loads(rsp_data)
        else:
            raise Exception(f"Call ElecTransSummService API error: "
                            f"{json.loads(response.read().decode())['detail'][0]['msg']}")
    except ConnectionRefusedError as ex:
        raise Exception(f"Call ElecTransSummService API got connect error: {ex}")
    finally:
        if response is not None:
            response.close()
        if http_cli is not None:
            http_cli.close()
            del http_cli

def call_getiotdata_etl():
    __ElecTransSummService__(api_url="http://10.248.0.71:8000/ElecTransSummService/ElecTransSumm", fab="L5A", Stage="ARRAY", start_time="2022-09-27", end_time="2022-09-27")

def call_calcminkwh_etl():
    __ElecTransSummService__(api_url="http://10.248.0.71:8000/ElecTransSummService/ElecTransSumm", fab="L5A", Stage="ARRAY", start_time="2022-09-27", end_time="2022-09-27")

with dag:
    task_1 = PythonOperator(
        task_id="task-1",
        python_callable=call_getiotdata_etl,
    )
    task_2 = PythonOperator(
        task_id="task-2",
        python_callable=call_calcminkwh_etl,
    )

task_1 >> task_2
