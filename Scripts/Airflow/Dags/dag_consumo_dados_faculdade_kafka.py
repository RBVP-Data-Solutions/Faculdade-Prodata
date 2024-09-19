from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

default_args = {
    'start_date': datetime.now(),
    'catchup': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1)
}

# DAG 1: Consome dados dos alunos
with DAG(
    'DAG_CONSUMO_DADOS_ALUNOS',
    description='DAG que consome dados dos alunos no apache Kafka',
    default_args=default_args,
    schedule_interval='*/50 * * * *'
) as dag_alunos:
    consumir_dados_kafka = BashOperator(
        task_id='CONSUMIR_DADOS_ALUNOS_KAFKA',
        bash_command='python3 /usr/local/airflow/scripts/ingestao_dados_alunos_kafka.py',
    )

# DAG 2: Consome dados das disciplinas (acionada após a DAG 1)
with DAG(
    'DAG_CONSUMO_DADOS_DISCIPLINAS',
    description='DAG que consome dados das disciplinas no apache Kafka',
    default_args=default_args,
    schedule_interval=None,  # Acionamento manual ou por dependência
    start_date=datetime.now()
) as dag_disciplinas:
    consumir_dados_disciplinas_kafka = BashOperator(
        task_id='CONSUMIR_DADOS_DISCIPLINAS_KAFKA',
        bash_command='python3 /usr/local/airflow/scripts/ingestao_dados_disciplinas_kafka.py',
    )

with DAG(
    'DAG_CONSUMO_DADOS_ALUNOS_CURSO_DISCIPLINAS',
    description='DAG que consome dados dos alunos com o curso e disciplinas no apache Kafka',
    default_args=default_args,
    schedule_interval=None,  # Acionamento manual ou por dependência
    start_date=datetime.now()
) as dag_alunos_curso_disciplinas:
    consumir_dados_alunos_curso_disciplinas_kafka = BashOperator(
        task_id='CONSUMIR_DADOS_ALUNOS_CURSO_DISCIPLINAS_KAFKA',
        bash_command='python3 /usr/local/airflow/scripts/ingestao_dados_alunos_curso_disciplinas_kafka.py',
    )

with DAG(
    'DAG_CONSUMO_DADOS_ATIVIDADES_ALUNOS',
    description='DAG que consome dados das atividades dos alunos no apache Kafka',
    default_args=default_args,
    schedule_interval=None,  # Acionamento manual ou por dependência
    start_date=datetime.now()
) as dag_atividades_alunos:
    consumir_dados_atividades_alunos_kafka = BashOperator(
        task_id='CONSUMIR_DADOS_ATIVIDADES_ALUNOS_KAFKA',
        bash_command='python3 /usr/local/airflow/scripts/ingestao_dados_atividades_alunos_kafka.py',
    )

# Conexão entre as DAGs
# Aciona a DAG_CONSUMO_DADOS_DISCIPLINAS