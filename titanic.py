import os
import datetime as dt
import pandas as pd
import numpy
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# базовые аргументы DAG
settings = {
    'dag_id': 'titanic_pivot',  # Имя DAG
    'schedule_interval': None,  # Периодичность запуска, например, "00 15 * * *"
    'start_date': dt.datetime(2020, 12, 23),  # Время начала выполнения пайплайна
    'default_args': { # Базовые аргументы DAG
        'retries': 1,  # Количество повторений в случае неудач
        'retry_delay': dt.timedelta(minutes=1),  # Пауза между повторами
    }
}


def get_path(file_name):
    return os.path.join(os.path.expanduser('~'), file_name)


def download_titanic_dataset():
    url = 'https://web.stanford.edu/class/archive/cs/cs109/cs109.1166/stuff/titanic.csv'
    df = pd.read_csv(url)
    df.to_csv(get_path('titanic.csv'), encoding='utf-8')


def pivot_dataset():
    titanic_df = pd.read_csv(get_path('titanic.csv'))
    df = titanic_df.pivot_table(index=['Sex'],
                                columns=['Pclass'],
                                values='Name',
                                aggfunc='count').reset_index()
    df.to_csv(get_path('titanic_pivot.csv'))
    
def mean_fare_per_class():
    titanic_dfa = pd.read_csv(get_path('titanic.csv'))
    dfa = titanic_dfa.pivot_table(index=['Pclass'],                                                                values='Fare',                              aggfunc='mean').reset_index()
    dfa.to_csv(get_path('titanic_mean_fares.csv'))
    
# В контексте DAG'а зададим набор task'ок
# Объект-инстанс Operator'а - это и есть task
with DAG(**settings) as dag:
    
    # BashOperator, выполняющий указанную bash-команду
    first_task = BashOperator(
        task_id='first_task',
        bash_command='echo "Here we start! Info: run_id={{ run_id }} | dag_run={{ dag_run }}"',
        dag=dag,
    )
    # Загрузка датасета
    create_titanic_dataset = PythonOperator(
        task_id='download_titanic_dataset',
        python_callable=download_titanic_dataset,
        dag=dag,
    )
    # Чтение, преобразование и запись датасета
    pivot_titanic_dataset = PythonOperator(
        task_id='pivot_dataset',
        python_callable=pivot_dataset,
        dag=dag,
    )
    
    # Домашнее задание
    mean_fares_titanic_dataset = PythonOperator(
        task_id='mean_fares_titanic_dataset',
        python_callable=mean_fare_per_class,
        dag=dag,
    )
    
    # BashOperator, выполняющий указанную bash-команду
    last_task = BashOperator(
        task_id='last_task',
        bash_command='echo "Pipeline finished! Execution date is: date_time={{ ds }}"',
        dag=dag,
    )
    # Порядок выполнения тасок
    first_task >> create_titanic_dataset >> (pivot_titanic_dataset, mean_fares_titanic_dataset) >> last_task
