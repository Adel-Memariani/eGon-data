from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import airflow

from egon.data.airflow.tasks import initdb
from egon.data.airflow.tasks_test import initdb_local

with airflow.DAG(
    "egon-data-processing-pipeline",
    description="The eGo^N data processing DAG.",
    default_args={"start_date": days_ago(1)},
) as pipeline:
    setup = PythonOperator(task_id="initdb", python_callable=initdb)
    test_DLR = PythonOperator(task_id="initdb_local", python_callable=initdb_local)
    
    setup.set_downstream(test_DLR)
    
    # If you a second task, e.g. `teardown`, which should come after `setup`
    # you would specify this in the following way:
    #
    # setup.set_downstream(teardown)
    #
    # or
    #
    # teardown.set_upstream(setup)
