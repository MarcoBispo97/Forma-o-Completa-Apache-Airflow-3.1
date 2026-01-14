###### MÓDULOS ######
# O airflow está num conteiner, então os módulos são importados do airflow
# Não é necessário instalar nada via pip
import pendulum # Importante para a questão do time-zone
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sdk import get_current_context

###### DAG ######
# Tudo dentro do contexto da DAG use a DAG de forma automática
# Reaproveitamento de código
with DAG(
    dag_id = "Xcom2", # Nome único para a DAG
    description = "Exemplo_Xcom_2",
    schedule = None, # Sem agendamento
    start_date=pendulum.datetime(2024, 1, 1, tz="America/Sao_Paulo"), #Qualquer data passada
    catchup = False, # Se a DAG falhar ele executa execuções passadas que ficaram pendentes
    tags=["curso", "exemplo","xcom"],
) as dag:
###### TASKS ######
    def task_write():
        # Retorna o dicionário com o contexto enviado a task instance
        #t.i. = task information
        ti = get_current_context()['ti']
        ti.xcom_push(key="valorxcom1", value=10000)

    def task_read():
        ti = get_current_context()['ti']
        valor = ti.xcom_pull(key="valorxcom1", task_ids="task_write")
        print(f"O valor recebido do XCom é: {valor}")

    task1 = PythonOperator(
        task_id = "task_write",
        python_callable = task_write,
    )
    task2 = PythonOperator(
        task_id = "task_read",
        python_callable = task_read,
    )

    task1 >> task2  # Indica a ordem de execução