from azure.storage.blob import BlobServiceClient
from kafka import KafkaProducer
from faker import Faker
from datetime import datetime, date
import pandas as pd
import json
import io

# Configurações
server = '10.0.0.4:9092'  # Endereço IP e porta do servidor Kafka
topic = 'dados-faculdade'  # Nome do tópico Kafka
cadeia_conexao = pd.read_csv('/home/rodrigo/Dev/Faculdade-PRODATA/Faculdade-PRODATA/keys.txt', dtype=str)
nome_container = 'datalake-dados-faculdade'

# Conexão com o Azure Blob Storage
blob_service_client = BlobServiceClient.from_connection_string(cadeia_conexao)
container_client = blob_service_client.get_container_client(nome_container)

# Função para ler dados de um blob como DataFrame
def read_blob_to_df(blob_client):
    blob_data = blob_client.download_blob().readall()
    df = pd.read_csv(io.BytesIO(blob_data))
    return df

# Listar blobs no diretório de cursos e ler dados
blobs_list = container_client.list_blobs(name_starts_with='raw/dados_faculdade/dados_cursos/')
dfs_cursos = []
for blob in blobs_list:
    blob_client = container_client.get_blob_client(blob)
    dados_cursos_raw = read_blob_to_df(blob_client)
    dfs_cursos.append(dados_cursos_raw)

# Concatenar todos os DataFrames de cursos
df_cursos_raw = pd.concat(dfs_cursos, ignore_index=True)

# Listar blobs no diretório de disciplinas e ler dados
blobs_list = container_client.list_blobs(name_starts_with='raw/dados_faculdade/dados_disciplinas/')
dfs_disciplinas = []
for blob in blobs_list:
    blob_client = container_client.get_blob_client(blob)
    dados_disciplinas_raw = read_blob_to_df(blob_client)
    dfs_disciplinas.append(dados_disciplinas_raw)

# Concatenar todos os DataFrames de disciplinas
df_disciplinas_raw = pd.concat(dfs_disciplinas, ignore_index=True)

# Listar blobs no diretório de alunos e ler dados
blobs_list = container_client.list_blobs(name_starts_with='raw/dados_faculdade/dados_alunos/')
dfs_alunos = []
for blob in blobs_list:
    blob_client = container_client.get_blob_client(blob)
    dados_alunos_raw = read_blob_to_df(blob_client)
    dfs_alunos.append(dados_alunos_raw)

# Concatenar todos os DataFrames de alunos
df_alunos_raw = pd.concat(dfs_alunos, ignore_index=True)

# Merge dos DataFrames para formar um DataFrame completo
df_completo = df_alunos_raw \
    .merge(df_cursos_raw, on='Curso') \
    .merge(df_disciplinas_raw, on='Curso')

# Função para gerar dados fake com base nos DataFrames mesclados
def gerar_dados_fake():
    fake = Faker('pt_BR')
    dados_fake = []

    def status():
        if nota_aluno >= 30 and nota_aluno < 70 and frequencia_aluno >= 75:
            return 'Exame Especial'
        elif nota_aluno < 30 or frequencia_aluno < 75:
            return 'Reprovado'
        else:
            return 'Aprovado'

    def motivo_reprovacao():
        if frequencia_aluno < 75:
            return 'frequência'
        elif nota_aluno < 30:
            return 'nota'
        else:
            return ''

    for index, row in df_completo.iterrows():
        nota_aluno = fake.random_int(min=1, max=100)
        frequencia_aluno = fake.random_int(min=1, max=100)

        data_ingresso = datetime.strptime(row['Data_Ingresso'], '%d/%m/%Y').date()
        data_conclusao = datetime.strptime(row['Data_Prevista_Conclusao'], '%d/%m/%Y').date()

        dado_fake = {
            'Curso': row['Curso'],
            'Codigo da Disciplina': row['Código da Disciplina'],
            'Período': fake.date_between_dates(date_start=data_ingresso, date_end=data_conclusao),
            'Matrícula do aluno': row['Matricula'],
            'Nota do aluno': nota_aluno,
            'Frequencia do aluno': frequencia_aluno,
            'Status': status(),
            'Motivo reprovação': motivo_reprovacao()
        }
        dados_fake.append(dado_fake)

    return dados_fake

# Função para serializar objetos date para JSON
def json_serializer(obj):
    if isinstance(obj, pd.Series):
        return obj.to_dict()
    elif isinstance(obj, date):
        return obj.strftime('%d/%m/%Y')
    raise TypeError(f'Tipo não serializável: {type(obj)}')

# Função para enviar dados para o Kafka
def enviar_dados_kafka(dados):
    producer = KafkaProducer(bootstrap_servers=server, value_serializer=lambda v: json.dumps(v, default=json_serializer).encode('utf-8'))

    for dado in dados:
        producer.send(topic, dado)

    producer.flush()

# Gerar dados fake e enviar para o Kafka
dados_fake = gerar_dados_fake()
enviar_dados_kafka(dados_fake)
