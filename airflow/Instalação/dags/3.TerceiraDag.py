###### MÓDULOS ######
# O airflow está num conteiner, então os módulos são importados do airflow
# Não é necessário instalar nada via pip
import pendulum # Importante para a questão do time-zone
from airflow import DAG
from airflow.operators.bash import BashOperator

###### DAG ######
# Tudo dentro do contexto da DAG use a DAG de forma automática
# Reaproveitamento de código
with DAG(
    dag_id = "terceira_dag", # Nome único para a DAG
    description = "Minha terceira DAG no Airflow",
    schedule = None, # Sem agendamento
    start_date=pendulum.datetime(2025, 1, 1, tz="America/Sao_Paulo"), #Qualquer data passada
    catchup = False, # Se a DAG falhar ele executa execuções passadas que ficaram pendentes
    tags=["curso", "exemplo"],
) as dag:
###### TASKS ######
    task1 = BashOperator(
        task_id = "task1",
        bash_command = "sleep 5",
    )
    task2 = BashOperator(
        task_id = "task2",
        bash_command = "echo 'Hello, Airflow!'",
        # bash_command = "exit 1", # Simula uma falha "upstream failed"
    )
    task3 = BashOperator(
        task_id = "task3",
        bash_command = "date",
    )
##### Ordem de precedência / Execução ######
    [ task1 , task2 ] >> task3 