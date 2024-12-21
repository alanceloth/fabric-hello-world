from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, input_file_name, regexp_extract, to_timestamp

# Configurar o Spark
spark = SparkSession.builder.getOrCreate()

# Caminho do Lakehouse (onde os JSON estão armazenados)
json_path = "Files/raw_btcusd/"

# Ler todos os arquivos JSON do diretório e incluir o nome do arquivo como coluna
df = spark.read.json(json_path).withColumn("file_name", input_file_name())

# Regex para capturar o timestamp no nome do arquivo (exemplo: 2024-12-21T01:14:10.7952326Z)
date_regex = r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})"

# Extração e transformação dos dados
df_extracted = df.select(
    col("data.amount").cast("float").alias("amount"),
    col("data.base").alias("base"),
    col("data.currency").alias("currency"),
    to_timestamp(
        regexp_extract(col("file_name"), date_regex, 1), "yyyy-MM-dd'T'HH:mm:ss"
    ).alias("quotation_date"),  # Data de cotação convertida para timestamp
    current_timestamp().alias("processing_timestamp")  # Timestamp de processamento no formato datetime
)

# # Mostrar os dados para conferência
# display(df_extracted)

# Caminho para salvar os dados no Lakehouse como Delta Table
lakehouse_table_path = "Tables/bitcoin_prices"

# Salvar os dados no formato Delta no Lakehouse
df_extracted.write.format("delta").mode("append").save(lakehouse_table_path)

# Registrar a tabela no catálogo do Lakehouse para uso futuro
spark.sql(f"CREATE TABLE IF NOT EXISTS bitcoin_prices USING DELTA LOCATION '{lakehouse_table_path}'")

print("Dados salvos no Lakehouse como Delta Table!")