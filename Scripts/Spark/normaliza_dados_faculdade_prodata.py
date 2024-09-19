# IMPORTACA DAS BIBLIOTECAS
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import regexp_replace
from azure.storage.blob import BlobClient, BlobServiceClient
import io
import pandas as pd
import os
# CRIACAO DA APLICACAO SPARK

spark = SparkSession.builder.appName("ATIVIDADE_6").getOrCreate()


# VARIAVEIS DE APOIO

cadeia_conexao = pd.read_csv('/home/rodrigo/Dev/Faculdade-PRODATA/Faculdade-PRODATA/keys.txt', dtype=str)
nome_container = 'datalake-dados-faculdade'

# CONEXÃO COM AZURE BLOB STORAGE
blob_service_client = BlobServiceClient.from_connection_string(cadeia_conexao)
container_client = blob_service_client.get_container_client(nome_container)

# Listar blobs no diretório de cursos e ler dados

blobs_list = container_client.list_blobs(name_starts_with='raw/dados_faculdade/dados_cursos/')
dfs_cursos = []
for blob in blobs_list:
    blob_client = container_client.get_blob_client(blob)
    blob_data = blob_client.download_blob().readall()
    df_cursos_raw = pd.read_csv(io.BytesIO(blob_data))
    dfs_cursos.append(df_cursos_raw)

# Concatenar todos os DataFrames de cursos
df_cursos_raw = pd.concat(dfs_cursos, ignore_index=True)
df_spark_cursos_raw = spark.createDataFrame(df_cursos_raw)
df_spark_cursos_raw.createOrReplaceTempView("dados_cursos")

# Listar blobs no diretório de disciplinas e ler dados
blobs_list = container_client.list_blobs(name_starts_with='raw/dados_faculdade/dados_disciplinas/')
dfs_disciplinas = []
for blob in blobs_list:
    blob_client = container_client.get_blob_client(blob)
    blob_data = blob_client.download_blob().readall()
    dados_disciplinas_raw = pd.read_csv(io.BytesIO(blob_data))
    dfs_disciplinas.append(dados_disciplinas_raw)

# Concatenar todos os DataFrames de disciplinas
df_disciplinas_raw = pd.concat(dfs_disciplinas, ignore_index=True)
df_spark_disciplinas_raw = spark.createDataFrame(df_disciplinas_raw)
df_spark_disciplinas_raw.createOrReplaceTempView("dados_disciplinas")

# Listar blobs no diretório de alunos e ler dados
blobs_list = container_client.list_blobs(name_starts_with='raw/dados_faculdade/dados_alunos/')
dfs_alunos = []
for blob in blobs_list:
    blob_client = container_client.get_blob_client(blob)
    blob_data = blob_client.download_blob().readall()
    dados_alunos_raw = pd.read_csv(io.BytesIO(blob_data))
    dfs_alunos.append(dados_alunos_raw)

# Concatenar todos os DataFrames de alunos
dados_alunos_raw = pd.concat(dfs_alunos, ignore_index=True)
df_spark_alunos_raw = spark.createDataFrame(dados_alunos_raw)
df_spark_alunos_raw.createOrReplaceTempView("dados_alunos")


# Listar blobs no diretório da relação alunos curso disciplina e ler dados
blobs_list = container_client.list_blobs(name_starts_with='raw/dados_faculdade/dados_alunos_curso_disciplinas/')
dfs_alunos_curso_disciplinas = []
for blob in blobs_list:
    blob_client = container_client.get_blob_client(blob)
    blob_data = blob_client.download_blob().readall()
    dados_alunos_curso_disciplinas_raw = pd.read_csv(io.BytesIO(blob_data))
    dfs_alunos_curso_disciplinas.append(dados_alunos_curso_disciplinas_raw)

# Concatenar todos os DataFrames da relação alunos curso disciplinas
dados_alunos_curso_disciplinas_raw = pd.concat(dfs_alunos_curso_disciplinas, ignore_index=True)
df_spark_alunos_curso_disciplinas_raw = spark.createDataFrame(dados_alunos_curso_disciplinas_raw)
df_spark_alunos_curso_disciplinas_raw.createOrReplaceTempView("dados_alunos_curso_disciplinas")

# Listar blobs no diretório da relação alunos curso disciplina e ler dados
blobs_list = container_client.list_blobs(name_starts_with='raw/dados_faculdade/dados_atividades_alunos/')
dfs_atividades_alunos = []
for blob in blobs_list:
    blob_client = container_client.get_blob_client(blob)
    blob_data = blob_client.download_blob().readall()
    dados_atividades_alunos_raw = pd.read_csv(io.BytesIO(blob_data))
    dfs_atividades_alunos.append(dados_atividades_alunos_raw)

# Concatenar todos os DataFrames da relação alunos curso disciplinas
dados_atividades_alunos_raw = pd.concat(dfs_atividades_alunos, ignore_index=True)
df_spark_atividades_alunos_raw = spark.createDataFrame(dados_atividades_alunos_raw)
df_spark_atividades_alunos_raw.createOrReplaceTempView("dados_atividades_alunos")

# NORMALIZAR OS DADOS

df_cursos_normalizado = spark.sql(
"""
SELECT  DISTINCT CAST(Curso AS INT) AS Curso,
                 `Nome do Curso` AS Nome_Curso,
                 CAST(Periodos AS INT) AS Periodos,
                 CAST(`Carga Horaria Prevista Por Disciplina(horas)` AS INT) AS Carga_Horaria_Prevista,
                 CAST(`Disciplinas Por Periodo` AS INT) AS Disciplinas_Por_Periodo,
                 CAST(`Media de alunos por disciplinas` AS INT) AS Media_Alunos_Disciplina
FROM dados_cursos
""")

df_cursos_normalizado.coalesce(1).write.mode("overwrite").format("parquet").options(header=True, inferSchema=True).save('/usr/local/datasets/dados_cursos')


df_alunos_normalizado = spark.sql(
"""
SELECT  DISTINCT CAST(Matricula AS INT) AS Matricula,
                 Nome,
                 Sexo,
                 CPF,
                 to_date(Data_Nascimento, 'dd/MM/yyyy') AS Data_Nascimento,
                 `E-mail` AS Email,
                 Endereco,
                 Telefone,
                 CAST(Curso AS INT),
                 to_date(Data_Ingresso, 'dd/MM/yyyy') AS Data_Ingresso,
                 to_date(Data_Prevista_Conclusao, 'dd/MM/yyyy') AS Data_Prevista_Conclusao,
                 to_date(Data_Conclusao, 'dd/MM/yyyy') AS Data_Conclusao,
                 Status_Matricula
FROM dados_alunos
""")

df_alunos_normalizado.coalesce(1).write.mode("overwrite").format("parquet").options(header=True, inferSchema=True).save('/usr/local/datasets/dados_alunos')
spark.catalog.dropTempView("dados_alunos")

df_disciplinas_normalizado = spark.sql(
"""
SELECT  DISTINCT CAST(`Código da Disciplina` AS INT) AS Codigo_Disciplina,
                 CAST(Curso AS INT),
                 `Nome da Disciplina` AS Nome_Disciplina,
                 Tipo,
                 CAST(`Nota de Corte` AS FLOAT) AS Nota_Corte,
                 CAST(`Carga Horária Prevista` AS INT) AS Carga_Horaria_Prevista,
                 CAST(`Frequência Mínima` AS INT) AS Frequencia_Minima
FROM dados_disciplinas
""")

df_disciplinas_normalizado.coalesce(1).write.mode("overwrite").format("parquet").options(header=True, inferSchema=True).save('/usr/local/datasets/dados_disciplinas')
spark.catalog.dropTempView("dados_disciplinas")

df_alunos_curso_disciplinas_normalizado = spark.sql(
"""
SELECT  DISTINCT CAST(Curso AS INT),
                 CAST(`Codigo da Disciplina` AS INT) AS Codigo_Disciplina,
                 to_date(`Período`, 'dd/MM/yyyy') AS Periodo,
                 CAST(`Matrícula do aluno` AS INT) AS Matricula_Aluno,
                 CAST(`Nota do aluno` AS FLOAT) AS Nota_Aluno,
                 CAST(`Frequencia do aluno` AS INT) AS Frequencia_Aluno,
                 Status,
                 `Motivo reprovação` AS Motivo_Reprovacao
FROM dados_alunos_curso_disciplinas
""")

df_alunos_curso_disciplinas_normalizado.coalesce(1).write.mode("overwrite").format("parquet").options(header=True, inferSchema=True).save('/usr/local/datasets/dados_alunos_curso_disciplinas')
spark.catalog.dropTempView("dados_alunos_curso_disciplinas")

df_atividades_alunos_normalizado = spark.sql(
"""
SELECT  DISTINCT CAST(Curso AS INT),
                 CAST(`Codigo da Disciplina` AS INT) AS Codigo_Disciplina,
                 to_date(`Período`, 'dd/MM/yyyy') AS Periodo,
                 CAST(`Matrícula do aluno` AS INT) Matricula_Aluno,
                 CAST(`Código da atividade` AS INT) AS Codigo_Atividade,
                 `Nome da atividade` AS Nome_Atividade,
                 CAST(`Valor da atividade` AS FLOAT) AS Valor_Atividade,
                 CAST(`Nota obtida pelo aluno` AS FLOAT) AS Nota_Obtida
FROM dados_atividades_alunos
""")

df_atividades_alunos_normalizado.coalesce(1).write.mode("overwrite").format("parquet").options(header=True, inferSchema=True).save('/usr/local/datasets/dados_atividades_alunos')
spark.catalog.dropTempView("dados_atividades_alunos")
spark.stop()

# METODO PARA FAZER UPLOAD DE ARQUIVOS
caminho_df_cursos_local = '/usr/local/datasets/dados_cursos/'
caminho_df_cursos_datalake = 'consume/dados_faculdade/dados_cursos/dados_cursos.parquet'
for item in os.listdir(caminho_df_cursos_local):
  if item.endswith(".parquet"):
    blob = BlobClient.from_connection_string(conn_str=cadeia_conexao, container_name="datalake-dados-faculdade", blob_name=caminho_df_cursos_datalake)
    with open(caminho_df_cursos_local + item, "rb") as data:
      blob.upload_blob(data, overwrite = True)

caminho_df_alunos_local = '/usr/local/datasets/dados_alunos/'
caminho_df_alunos_datalake = 'consume/dados_faculdade/dados_alunos/dados_alunos.parquet'
for item in os.listdir(caminho_df_alunos_local):
  if item.endswith(".parquet"):
    blob = BlobClient.from_connection_string(conn_str=cadeia_conexao, container_name="datalake-dados-faculdade", blob_name=caminho_df_alunos_datalake)
    with open(caminho_df_alunos_local + item, "rb") as data:
      blob.upload_blob(data, overwrite = True)

caminho_df_disciplinas_local = '/usr/local/datasets/dados_disciplinas/'
caminho_df_disciplinas_datalake = 'consume/dados_faculdade/dados_disciplinas/dados_disciplinas.parquet'
for item in os.listdir(caminho_df_disciplinas_local):
  if item.endswith(".parquet"):
    blob = BlobClient.from_connection_string(conn_str=cadeia_conexao, container_name="datalake-dados-faculdade", blob_name=caminho_df_disciplinas_datalake)
    with open(caminho_df_disciplinas_local + item, "rb") as data:
      blob.upload_blob(data, overwrite = True)

caminho_df_dados_alunos_curso_disciplinas_local = '/usr/local/datasets/dados_alunos_curso_disciplinas/'
caminho_df_dados_alunos_curso_disciplinas_datalake = 'consume/dados_faculdade/dados_alunos_curso_disciplinas/dados_alunos_curso_disciplinas.parquet'
for item in os.listdir(caminho_df_dados_alunos_curso_disciplinas_local):
  if item.endswith(".parquet"):
    blob = BlobClient.from_connection_string(conn_str=cadeia_conexao, container_name="datalake-dados-faculdade", blob_name=caminho_df_dados_alunos_curso_disciplinas_datalake)
    with open(caminho_df_dados_alunos_curso_disciplinas_local + item, "rb") as data:
      blob.upload_blob(data, overwrite = True)

caminho_df_dados_atividades_alunos_local = '/usr/local/datasets/dados_atividades_alunos/'
caminho_df_dados_atividades_alunos_datalake = 'consume/dados_faculdade/dados_atividades_alunos/dados_atividades_alunos.parquet'
for item in os.listdir(caminho_df_dados_atividades_alunos_local):
  if item.endswith(".parquet"):
    blob = BlobClient.from_connection_string(conn_str=cadeia_conexao, container_name="datalake-dados-faculdade", blob_name=caminho_df_dados_atividades_alunos_datalake)
    with open(caminho_df_dados_atividades_alunos_local + item, "rb") as data:
      blob.upload_blob(data, overwrite = True)
