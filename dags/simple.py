from airflow.operators.python import PythonOperator
from airflow import DAG
from datetime import datetime


def print_hello(): # Обычная функция, которая выведет в логах Hello
    print("Hello")

def print_world():
    print("World") # Обычная функция, которая выведет в логах World


with DAG(
    'hello_world_dag - tefaier', # Название DAG в оркестраторе
    start_date=datetime(2025, 9, 11), # Дата первого запуска
    schedule="@daily", # Расписание переодичности выполнения
    tags=['demo'], # Название группы файлов\команды разработки
    catchup=False # Поведение по умолчанию если start_date раньше чем текущая дата
) as dag: # Объявляем DAG для запуска двух задач.
    task_1 = PythonOperator(
        task_id='print_hello',
        python_callable=print_hello
    ) # Объявляем первую задачу.

    task_2 = PythonOperator(
        task_id='print_world',
        python_callable=print_world
    ) # Обяъвляем вторую задачу

    task_1 >> task_2 # объявляем порядок выполнения задач