from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# 1. Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# 2. Initialize the DAG
with DAG(
    dag_id='airflow_welcome_demo',
    default_args=default_args,
    description='A simple welcome DAG for demonstration purposes',
    schedule_interval=None,  # Trigger manually for demos
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['demo', 'onboarding'],
) as dag:

    # 3. Define a Python function for the PythonOperator
    def print_welcome_message(**context):
        execution_date = context['ds']
        print(f"==================================================")
        print(f" Hello! Welcome to your Apache Airflow Demo! ")
        print(f" The current logical execution date is: {execution_date}")
        print(f"==================================================")
        return "Welcome message printed successfully!"

    # 4. Define Tasks
    
    # Task 1: A Bash command to start the workflow
    start_task = BashOperator(
        task_id='print_start_log',
        bash_command='echo "Starting the Airflow Demo DAG at $(date)"',
    )

    # Task 2: A Python task running our welcome function
    welcome_task = PythonOperator(
        task_id='print_welcome_message',
        python_callable=print_welcome_message,
    )

    # Task 3: A Bash task showcasing simple Jinja templating
    template_demo_task = BashOperator(
        task_id='jinja_template_demo',
        bash_command='echo "Today\'s date via Airflow macro is: {{ ds }}"',
    )

    # Task 4: An empty task representing the completion of the pipeline
    end_task = BashOperator(
        task_id='print_end_log',
        bash_command='echo "Demo workflow completed successfully!"',
    )

    # 5. Set Task Dependencies (Flow)
    start_task >> welcome_task >> template_demo_task >> end_task

