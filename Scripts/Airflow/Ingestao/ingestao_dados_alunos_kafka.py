from kafka import KafkaConsumer, TopicPartition
import json
import os
import pandas as pd
from azure.storage.blob import BlobClient
from datetime import datetime
import time

# Configuracoes do Kafka
server = '10.0.0.4:9092'  # Substitua pelo endereço IP e porta corretos do Apache Kafka
topic = 'dados-faculdade' # Substituia pelo nome do topico
offset_file = '/usr/local/datasets/kafka_control_topic_offset.txt'
cadeia_conexao = pd.read_csv('/home/rodrigo/Dev/Faculdade-PRODATA/Faculdade-PRODATA/keys.txt', dtype=str)
nome_container = 'datalake-dados-faculdade'

# Função para obter o último offset lido a partir de um arquivo
def get_last_offset():
    try:
        with open(offset_file, 'r') as file:
            last_offset = int(file.read())
    except FileNotFoundError:
        last_offset = -1

    return last_offset

# Função para atualizar o último offset lido em um arquivo
def update_last_offset(offset):
    with open(offset_file, 'w') as file:
        file.write(str(offset))

# Criação do consumidor
consumer = KafkaConsumer(
    bootstrap_servers=server,
    value_deserializer=lambda x: x.decode('utf-8'),
    auto_offset_reset='earliest',
    consumer_timeout_ms=1000 # Define o tempo limite para aguardar novas mensagens (em milissegundos)
)

# Parametro de data para encerrar o loop se novas mensagens não forem visualizadas
last_message_time = time.time()

dados_alunos = []

# Consumindo os dados do topico
while True:
    # Atribui o consumidor ao tópico
    consumer.subscribe([topic])
    # Obtém as partições do tópico
    partitions = consumer.partitions_for_topic(topic)
    # Consumindo os dados do tópico
    for partition in partitions:
        # Obtém o último offset lido para a partição
        last_offset = get_last_offset()
        # Define o offset inicial para ler desde o começo
        consumer.seek_to_beginning()
        # Realiza o consumo das mensagens
        for message in consumer:
#           if time.time() - last_message_time > 5:  # Verifica se o tempo desde a última mensagem é maior que 5 segundos
#               print("Não há novas mensagens disponíveis.")
#               break
            offset = message.offset
            data = message.value
            # Verifica se a mensagem já foi vista anteriormente
            if offset <= last_offset:
                continue

            try:
                json_data = json.loads(data)
                # Processa a mensagem JSON conforme necessário
                if all(field in json_data for field in ['Matricula', 'Nome', 'Sexo', 'CPF', 'Data_Nascimento', 'E-mail', 'Endereco', 'Telefone', 'Curso', 'Data_Ingresso', 'Data_Prevista_Conclusao', 'Data_Conclusao', 'Status_Matricula']):
                    dados_alunos.append(json_data)
                # Atualiza o tempo da última mensagem
                last_message_time = time.time()
            except (json.JSONDecodeError, TypeError):
                print("Dado fora do formato JSON ou mal-formatado. O registro será ignorado.")
            # Atualiza o tempo da última mensagem
            last_message_time = time.time()

            # Atualiza o último offset lido
            update_last_offset(offset)

    if time.time() - last_message_time > 5:  # Verifica se o tempo desde a última mensagem é maior que 5 segundos
        print("Não há novas mensagens disponíveis.")
        break

# Encerra o consumidor
consumer.close()

# Verifica se existem dados para salvar
if dados_alunos:
    # Cria um DataFrame do Pandas com os dados lidos
    df = pd.DataFrame(dados_alunos)

    # Gera o nome do arquivo com base na data e hora atual
    current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
    arquivo_local = f'/usr/local/datasets/dados_alunos_{current_time}.csv'
    arquivo_datalake = f'raw/dados_faculdade/dados_alunos/dados_alunos_{current_time}.csv'

    # Salva o DataFrame em um arquivo CSV
    df.to_csv(arquivo_local, index=False, sep=',')

    # Salva o arquivo no Data lake
    blob = BlobClient.from_connection_string(conn_str=cadeia_conexao, container_name=nome_container, blob_name=arquivo_datalake)
    with open(arquivo_local, "rb") as data:
      blob.upload_blob(data, overwrite = True)

else:
    print('Não há dados para salvar em CSV.')
    # Encerra o consumidor
