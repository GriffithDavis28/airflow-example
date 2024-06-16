from datetime import timedelta
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule

def fail_state(ti):
    ti.xcom_push(key='task_status', value='failed' )    
    logging.error('Failed....')
    raise Exception('Failed....')
   

def branch_task(ti, **kwargs):
    failed_task_status = ti.xcom_pull(task_ids='failed_state', key='task_status')
    if failed_task_status == 'failed':
        state = 'skipped_state'
    else:
        state = 'success_state'
    ti.xcom_push(key='task_status', value=state)
    logging.info(f'Task state: {state}')
    return state


def success_state(ti):
    ti.xcom_push(key='task_status', value='success')
    logging.info('Success....')
    print('Success....')


def skip_state(ti):
    ti.xcom_push(key='task_status', value='skipped')
    logging.error('Skipped....')
    print('Skipped....')


def logs(ti):
    fail_status=ti.xcom_pull(key='task_status', task_ids='failed_state')
    logging.info(f'{fail_status}')
    branch_status=ti.xcom_pull(key='task_status', task_ids='branch_state')
    logging.info(f'{branch_status}')
    skipped_status=ti.xcom_pull(key='task_status', task_ids='skipped_state')
    logging.info(f'{skipped_status}')
    success_status=ti.xcom_pull(key='task_status', task_ids='success_state')
    logging.info(f'{success_status}')

default_args = {
    'owner': 'ariflow',
    'retires': 5,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='status_dag_v3',
    default_args=default_args,
    description='Checking stats',
    schedule_interval=None
) as dag:
    failed_task = PythonOperator(
        task_id='failed_state',
        python_callable=fail_state,
    )

    branchTask = BranchPythonOperator(
        task_id='branch_state',
        python_callable=branch_task,
        provide_context=True
    )

    success_task = PythonOperator(
        task_id='success_state',
        python_callable=success_state,
        provide_context=True
    )

    skipped_task = PythonOperator(
        task_id='skipped_state',
        python_callable=skip_state,
        provide_context=True,
        trigger_rule=TriggerRule.ONE_SUCCESS
    )

    log_states = PythonOperator(
        task_id='logging',
        provide_context=True,
        python_callable=logs,
        trigger_rule=TriggerRule.ALL_DONE
    )


    failed_task>>branchTask

    branchTask>>(skipped_task, success_task)

    (skipped_task, success_task)>>log_states

    