###### MÓDULOS ######
# O airflow está num conteiner, então os módulos são importados do airflow
# Não é necessário instalar nada via pip
import pendulum # Importante para a questão do time-zone
from airflow import DAG
from airflow.sdk import task

###### DAG ######
# Tudo dentro do contexto da DAG use a DAG de forma automática
# Reaproveitamento de código
with DAG(
    dag_id = "Xcom", # Nome único para a DAG
    description = "Exemplo_Xcom_1",
    schedule = None, # Sem agendamento
    start_date=pendulum.datetime(2024, 1, 1, tz="America/Sao_Paulo"), #Qualquer data passada
    catchup = False, # Se a DAG falhar ele executa execuções passadas que ficaram pendentes
    tags=["curso", "exemplo","xcom"],
) as dag:
###### TASKS ######
    @task
    def task_write():
        return {"valorxcom1": 1000}
    @task
    def task_read(payload: dict):
        print(f"O valor recebido do XCom é: {payload['valorxcom1']}")

    task_read(task_write())