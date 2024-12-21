from pyspark.sql import SparkSession

# Configurar o Spark
spark = SparkSession.builder.getOrCreate()

# Criar a tabela deduplicada na primeira execução, se não existir
spark.sql("""
CREATE TABLE IF NOT EXISTS stg_bitcoin_prices (
    amount FLOAT,
    base STRING,
    currency STRING,
    quotation_date TIMESTAMP,
    processing_timestamp TIMESTAMP
) USING DELTA
""")

# Deduplicação e atualização incremental com MERGE
spark.sql("""
MERGE INTO stg_bitcoin_prices AS target
USING (
    WITH ranked_data AS (
        SELECT 
            *, 
            ROW_NUMBER() OVER (PARTITION BY amount, base, currency ORDER BY processing_timestamp DESC) AS row_num
        FROM bitcoin_prices
    )
    SELECT *
    FROM ranked_data
    WHERE row_num = 1
) AS source
ON target.amount = source.amount
   AND target.base = source.base
   AND target.currency = source.currency
   AND target.quotation_date = source.quotation_date
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")

print("Tabela 'stg_bitcoin_prices' criada ou atualizada com sucesso, com duplicatas removidas.")
