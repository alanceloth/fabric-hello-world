from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, input_file_name, regexp_extract, to_timestamp, max as spark_max
from pyspark.sql.utils import AnalysisException

# Configurar o Spark
spark = SparkSession.builder.getOrCreate()

# Caminho do Lakehouse (onde os JSON estão armazenados)
json_path = "Files/raw_btcusd/"
lakehouse_table_path = "Tables/bitcoin_prices"

# Regex para capturar o timestamp no nome do arquivo (exemplo: 2024-12-21T01:14:10.7952326Z)
date_regex = r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})"

# Função para verificar se a tabela existe
def table_exists(table_name):
    try:
        spark.sql(f"DESCRIBE TABLE {table_name}")
        return True
    except AnalysisException:
        return False

# Função para obter a última data de inserção
def get_last_inserted_date(table_name):
    if table_exists(table_name):
        df_existing = spark.read.format("delta").load(lakehouse_table_path)
        return df_existing.select(spark_max("quotation_date").alias("last_date")).collect()[0]["last_date"]
    else:
        return None

# Obter a última data de inserção na tabela (ou None se não existir)
last_inserted_date = get_last_inserted_date("bitcoin_prices")

# Ler todos os arquivos JSON do diretório
df = spark.read.json(json_path).withColumn("file_name", input_file_name())

# Extração e transformação dos dados
df_extracted = df.select(
    col("data.amount").cast("float").alias("amount"),
    col("data.base").alias("base"),
    col("data.currency").alias("currency"),
    to_timestamp(
        regexp_extract(col("file_name"), date_regex, 1), "yyyy-MM-dd'T'HH:mm:ss"
    ).alias("quotation_date"),  # Data de cotação convertida para timestamp
    current_timestamp().alias("processing_timestamp")  # Timestamp de processamento
)

# Filtrar arquivos com base na última data de inserção
if last_inserted_date:
    print(f"Última data de inserção: {last_inserted_date}")
    df_filtered = df_extracted.filter(col("quotation_date") > last_inserted_date)
else:
    print("Tabela não encontrada ou vazia. Usando todos os arquivos para criar a tabela.")
    df_filtered = df_extracted

# Verificar se há novos dados para inserir
if df_filtered.count() > 0:
    # Salvar os dados no formato Delta no Lakehouse
    df_filtered.write.format("delta").mode("append").save(lakehouse_table_path)
    # Registrar a tabela no catálogo, se ainda não estiver registrada
    spark.sql(f"CREATE TABLE IF NOT EXISTS bitcoin_prices USING DELTA LOCATION '{lakehouse_table_path}'")
    print("Novos dados salvos com sucesso no Lakehouse!")
else:
    print("Nenhum dado novo para inserir.")

# Mostrar os dados filtrados para conferência
display(df_filtered)
