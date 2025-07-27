import os
from airflow.models import DagBag

def test_dag_loaded():
    dag_bag = DagBag(dag_folder=os.getenv("AIRFLOW__CORE__DAGS_FOLDER", "airflow/dags"))
    assert len(dag_bag.import_errors) == 0, f"DAG import errors: {dag_bag.import_errors}"
    assert "etl_transactions_local" in dag_bag.dags
    dag = dag_bag.get_dag("etl_transactions_local")
    assert dag is not None
    assert len(dag.tasks) == 4
    task_ids = [t.task_id for t in dag.tasks]
    assert task_ids == ["wait_for_file", "wait_for_min_size", "run_etl", "validate_and_alert"]
