from dateutil.relativedelta import relativedelta
from kafka import KafkaProducer
from faker import Faker
import json
from datetime import timedelta, date, datetime

server = '10.0.0.4:9092'  # Replace with your Kafka broker address
topic = 'dados-faculdade'  # Replace with your Kafka topic name

fake = Faker('pt_BR')

def gerar_dados_fake():
    dados_fake = []
    for _ in range(100):
        sexo = fake.random_element(elements=('M', 'F'))
        data_nascimento = fake.date_of_birth(minimum_age=18, maximum_age=65)

        if sexo == 'M':
            nome = fake.first_name_male()
            sobrenome = fake.last_name_male()
        else:
            nome = fake.first_name_female()
            sobrenome = fake.last_name_female()

        email = f"{nome.lower()}.{sobrenome.lower()}@trabalhopuc.br"

        data_ingresso = fake.date_between_dates(date_start=date(2015, 1, 1), date_end=date.today())

        codigo_curso = fake.random_int(min=1, max=5)

        def duracao_curso_anos():
            if codigo_curso == 1:
                return 5
            elif codigo_curso == 2 or codigo_curso == 3:
                return 4
            elif codigo_curso == 4:
                return 3
            else:
                return 2

        duracao = duracao_curso_anos()
        data_conclusao = data_ingresso + relativedelta(years=duracao)

        def data_conclusao_curso():
            if data_conclusao > date.today():
                return ''
            else:
                return data_conclusao.strftime("%d/%m/%Y")

        def status():
            if data_conclusao_curso() == '':
                return fake.random_element(elements=('Ativo', 'Trancado', 'Cancelado'))
            else:
                return 'Finalizado'

        dado_fake = {
            'Matricula': fake.unique.random_int(min=100000, max=999999),
            'Nome': f"{nome} {sobrenome}",
            'Sexo': sexo,
            'CPF': fake.unique.cpf(),
            'Data_Nascimento': data_nascimento.strftime("%d/%m/%Y"),
            'E-mail': email.replace(' ', '.'),
            'Endereco': f"{fake.city()}, {fake.state()}",
            'Telefone': fake.phone_number(),
            'Curso': codigo_curso,
            'Data_Ingresso': data_ingresso.strftime("%d/%m/%Y"),
            'Data_Prevista_Conclusao': data_conclusao.strftime("%d/%m/%Y"),
            'Data_Conclusao': data_conclusao_curso(),
            'Status_Matricula': status()
        }
        dados_fake.append(dado_fake)
    return dados_fake

def json_serializer(obj):
    if isinstance(obj, (datetime, date)):
        return obj.strftime('%d-%m-%Y')
    raise TypeError(f'Type {type(obj)} not serializable')

def enviar_dados_kafka(dados):
    producer = KafkaProducer(bootstrap_servers=server, value_serializer=lambda v: json.dumps(v, default=json_serializer).encode('utf-8'))

    for dado in dados:
        producer.send(topic, dado)

    producer.flush()

dados_fake = gerar_dados_fake()
enviar_dados_kafka(dados_fake)
