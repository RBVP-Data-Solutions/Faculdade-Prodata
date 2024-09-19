from azure.storage.blob import BlobServiceClient
from dateutil.relativedelta import relativedelta
from kafka import KafkaProducer
from faker import Faker
import json
import pandas as pd
import io
from datetime import timedelta, date, datetime

server = '10.0.0.4:9092'  # Substitua pelo endereço IP e porta corretos do Apache Kafka
topic = 'dados-faculdade' # Substituia pelo nome do topico
cadeia_conexao = pd.read_csv('/home/rodrigo/Dev/Faculdade-PRODATA/Faculdade-PRODATA/keys.txt', dtype=str)
nome_container = 'datalake-dados-faculdade'

blob_service_client = BlobServiceClient.from_connection_string(cadeia_conexao)
container_client = blob_service_client.get_container_client(nome_container)

# Função para ler dados de um blob como DataFrame
def read_blob_to_df(blob_client):
    blob_data = blob_client.download_blob().readall()
    df = pd.read_csv(io.BytesIO(blob_data))
    return df

fake = Faker('pt_BR')

def gerar_dados_disciplinas(cursos):
    disciplinas = []
    codigo_disciplina = 1
    nomes_disciplinas_usados = set()  # Para garantir nomes únicos por curso

    blobs_list = container_client.list_blobs(name_starts_with='raw/dados_faculdade/dados_cursos/')
    dfs_cursos = []
    for blob in blobs_list:
        blob_client = container_client.get_blob_client(blob)
        dados_cursos_raw = read_blob_to_df(blob_client)
        dfs_cursos.append(dados_cursos_raw)

    # Termos comuns em nomes de disciplinas de TI
    termos_ti = ["Sistemas", "Programação", "Banco de Dados", "Redes", "Segurança", "Engenharia", "Desenvolvimento", "Análise", "Cloud", "Inteligência Artificial", "Software", "Hardware", "Ciência", "Tecnologia", "Aplicações", "Web", "Mobile", "Gestão", "Projeto", "Interface", "Usuário"]
    for curso in cursos:
        # Define um número aleatório de disciplinas para o curso (entre 6 e 10)
        num_disciplinas = fake.random_int(min=6, max=10)

        for _ in range(num_disciplinas):
            while True:
                # Escolhe 2 ou 3 termos aleatoriamente para compor o nome da disciplina
                num_termos = fake.random_int(min=2, max=3)
                termos_escolhidos = fake.random_elements(elements=termos_ti, length=num_termos, unique=True)
                nome_disciplina = ' '.join(termos_escolhidos).capitalize()

                chave_unica = f"{curso}-{nome_disciplina}"
                if chave_unica not in nomes_disciplinas_usados:
                    nomes_disciplinas_usados.add(chave_unica)
                    break

            tipo = fake.random_element(elements=('Teórica', 'Prática'))

            curso = fake.random_int(min=1, max=5)

            def carga_horaria_prevista():
                if curso == 1:
                    return fake.random_int(min=17, max=91)
                elif curso == 2:
                    return fake.random_int(min=17, max=63)
                elif curso == 3:
                    return fake.random_int(min=17, max=63)
                elif curso == 5:
                    return fake.random_int(min=17, max=47)
                else:
                    return fake.random_int(min=17, max=47)

            carga_horaria_prevista = carga_horaria_prevista()

            disciplinas.append({
                'Código da Disciplina': codigo_disciplina,
                'Curso': curso,
                'Nome da Disciplina': nome_disciplina,
                'Tipo': tipo,
                'Nota de Corte': 70,
                'Carga Horária Prevista': carga_horaria_prevista,
                'Frequência Mínima': carga_horaria_prevista * 0.75
            })

            codigo_disciplina += 1

    return disciplinas


#disciplinas_geradas = gerar_dados_disciplinas(cursos_exemplo, cargas_horarias_cursos)
cursos = ['Engenharia de Software', 'Ciência da Computação', 'Sistemas de Informação', 'Jogos Digitais', 'Tecnólogo em Redes Digitais']

def json_serializer(obj):
    """Função para serializar objetos date para JSON."""
    if isinstance(obj, date):
        return obj.strftime('%d-%m-%Y')
    raise TypeError(f'Tipo não serializável: {type(obj)}')

def enviar_dados_kafka(dados):
    producer = KafkaProducer(bootstrap_servers=server, value_serializer=lambda v: json.dumps(v, default=json_serializer).encode('utf-8'))

    for dado in dados:
        producer.send(topic, dado)

    producer.flush()

disciplinas = gerar_dados_disciplinas(cursos)
enviar_dados_kafka(disciplinas)
