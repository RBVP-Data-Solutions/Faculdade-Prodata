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

    codigo_atividade = 0
    soma_valor_atividade_por_disciplina = {}

    for index, row in df_completo.iterrows():

        data_ingresso = datetime.strptime(row['Data_Ingresso'], '%d/%m/%Y').date()
        data_conclusao = datetime.strptime(row['Data_Prevista_Conclusao'], '%d/%m/%Y').date()

        nome_atividade = fake.random_element(elements=['Exercício', 'Trabalho', 'Avaliação'])
        codigo_atividade += 1

        # Inicializa a soma das notas para a disciplina se ainda não existe
        codigo_disciplina = row['Código da Disciplina']
        if codigo_disciplina not in soma_valor_atividade_por_disciplina:
            soma_valor_atividade_por_disciplina[codigo_disciplina] = 0

        valor = 0
        if nome_atividade == 'Exercício':
            valor = 20
        elif nome_atividade == 'Trabalho':
            valor = 30
        else:
            valor = 50

        # Ajusta o valor se ultrapassar 100 pontos
        if soma_valor_atividade_por_disciplina[codigo_disciplina] + valor > 100:
            valor = 100 - soma_valor_atividade_por_disciplina[codigo_disciplina]

        # Adiciona o valor ao total da disciplina
        soma_valor_atividade_por_disciplina[codigo_disciplina] += valor

        # Se a soma for 100, não adiciona mais atividades
        if soma_valor_atividade_por_disciplina[codigo_disciplina] == 100:
            continue

        nota = fake.random_int(min=0, max=valor)

        dado_fake = {
            'Curso': row['Curso'],
            'Codigo da Disciplina': codigo_disciplina,
            'Período': fake.date_between_dates(date_start=data_ingresso, date_end=data_conclusao),
            'Matrícula do aluno': row['Matricula'],
            'Código da atividade': codigo_atividade,
            'Nome da atividade': f"{nome_atividade} {codigo_atividade}",
            'Valor da atividade': valor,
            'Nota obtida pelo aluno': nota
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
