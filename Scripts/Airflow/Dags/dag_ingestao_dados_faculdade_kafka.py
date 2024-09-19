from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

# DAG 1: Processa dados dos alunos
with DAG(
    'DAG_INGESTAO_DADOS_ALUNOS',
    description='DAG que gera dados dos alunos no apache Kafka',
    default_args={'start_date': datetime(2023, 1, 1), 'catchup': False},
    schedule_interval='*/50 * * * *'
) as dag_alunos:
    produzir_dados_kafka = BashOperator(
        task_id='PRODUZIR_DADOS_ALUNOS_KAFKA',
        bash_command='python3 /usr/local/airflow/scripts/produtor_dados_alunos_kafka.py',
    )

# DAG 2: Processa dados das disciplinas (acionada após a DAG 1)
with DAG(
    'DAG_INGESTAO_DADOS_DISCIPLINAS',
    description='DAG que gera dados das disciplinas no apache Kafka',
    default_args={'start_date': datetime(2023, 1, 1), 'catchup': False},
    schedule_interval=None,  # Acionamento manual ou por dependência
    start_date=datetime.now()
) as dag_disciplinas:
    produzir_dados_disciplina_kafka = BashOperator(
        task_id='PRODUZIR_DADOS_DISCIPLINAS_KAFKA',
        bash_command='python3 /usr/local/airflow/scripts/produtor_dados_disciplinas_kafka.py',
    )

with DAG(
    'DAG_INGESTAO_DADOS_ALUNOS_CURSO_DISCIPLINAS',
    description='DAG que gera dados dos alunos com o curso e disciplinas no apache Kafka',
    default_args={'start_date': datetime(2023, 1, 1), 'catchup': False},
    schedule_interval=None,  # Acionamento manual ou por dependência
    start_date=datetime.now()
) as dag_alunos_curso_disciplinas:
    produzir_dados_alunos_curso_disciplinas_kafka = BashOperator(
        task_id='PRODUZIR_DADOS_ALUNOS_CURSO_DISCIPLINAS_KAFKA',
        bash_command='python3 /usr/local/airflow/scripts/produtor_dados_alunos_curso_disciplinas_kafka.py',
    )

with DAG(
    'DAG_INGESTAO_DADOS_ATIVIDADES_ALUNOS',
    description='DAG que gera dados das atividades dos alunos no apache Kafka',
    default_args={'start_date': datetime(2023, 1, 1), 'catchup': False},
    schedule_interval=None,  # Acionamento manual ou por dependência
    start_date=datetime.now()
) as dag_atividades_alunos:
    produzir_dados_atividades_alunos_kafka = BashOperator(
        task_id='PRODUZIR_DADOS_ATIVIDADES_ALUNOS_KAFKA',
        bash_command='python3 /usr/local/airflow/scripts/produtor_dados_atividades_alunos_kafka.py',
    )

with DAG(
    'DAG_INGESTAO_DADOS_STAGING',
    description='DAG que gera dados normalizados no apache Spark',
    default_args={'start_date': datetime(2023, 1, 1), 'catchup': False},
    schedule_interval=None,  # Acionamento manual ou por dependência
    start_date=datetime.now()
) as dag_dados_staging:
    normalizar_dados_staging = BashOperator(
        task_id='NORMALIZAR_DADOS_STAGING_SPARK',
        bash_command='python3 /usr/local/airflow/scripts/normaliza_dados_faculdade_prodata.py',
    )
# Conexão entre as DAGs
 # Aciona a DAG_INGESTAO_DADOS_DISCIPLINAS