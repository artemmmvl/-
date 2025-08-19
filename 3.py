from __future__ import annotations

import csv
import json
import os
from pathlib import Path
from typing import List, Dict, Any

import pendulum
import requests
from airflow import DAG
from airflow.decorators import task

API_URL = "https://jsonplaceholder.typicode.com/users"

AIRFLOW_HOME = Path(os.environ.get("AIRFLOW_HOME", str(Path.home() / "airflow")))
OUTPUT_DIR = AIRFLOW_HOME / "output"
OUTPUT_CSV = OUTPUT_DIR / "users.csv"

default_args = {
    "owner": "data-eng",
    "retries": 1,
    "retry_delay": pendulum.duration(minutes=3),
}

with DAG(
    dag_id="etl_users_json_to_csv_1",
    description="Extract users JSON -> Transform (id, name, email) -> Load CSV (output/users.csv)",
    default_args=default_args,
    start_date=pendulum.datetime(2025, 8, 1, tz="UTC"),
    schedule_interval=None,   
    catchup=False,
    max_active_runs=1,
    tags=["etl", "json", "csv"],
) as dag:

    @task(task_id="extract")
    def extract() -> List[Dict[str, Any]]:
        """Загрузка JSON-данных с удалённого ресурса."""
        resp = requests.get(API_URL, timeout=30)
        resp.raise_for_status()
        try:
            data = resp.json()
        except json.JSONDecodeError as e:
            raise ValueError(f"Некорректный JSON: {e}") from e
        if not isinstance(data, list):
            raise ValueError(f"Ожидался список пользователей, получено: {type(data)}")
        return data

    @task(task_id="transform")
    def transform(raw: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Преобразование: оставить только id, name, email."""
        result: List[Dict[str, Any]] = []
        for item in raw:
            if not all(k in item for k in ("id", "name", "email")):
                continue
            result.append({
                "id": item["id"],
                "name": item["name"],
                "email": item["email"],
            })
        if not result:
            raise ValueError("После трансформации нет данных для сохранения.")
        return result

    @task(task_id="load")
    def load(rows: List[Dict[str, Any]]) -> str:
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        with OUTPUT_CSV.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["id", "name", "email"])
            writer.writeheader()
            writer.writerows(rows)
        return str(OUTPUT_CSV.resolve())

    load(transform(extract()))
