import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Carrega as variáveis de ambiente do arquivo .env
load_dotenv()

# Configuração do banco de dados
DATABASE_URL = os.getenv("DATABASE_URL")

# Pasta contendo os arquivos CSV
CSV_FOLDER_PATH = os.getenv("CSV_FOLDER_PATH")

# Conexão com o banco de dados usando SQLAlchemy
engine = create_engine(DATABASE_URL)

def infer_schema_and_upload_to_db(csv_file, engine):
    """
    Lê um arquivo CSV, infere seu schema e faz upload para o banco de dados.
    """
    # Lê o CSV com todas as linhas
    df = pd.read_csv(csv_file)
    
    # Nome da tabela com prefixo raw_
    table_name = f"raw_{os.path.splitext(os.path.basename(csv_file))[0]}"
    
    # Upload para o banco de dados
    with engine.connect() as connection:
        # Cria a tabela no banco (substitui se já existir)
        df.to_sql(table_name, con=connection, index=False, if_exists="replace")
        print(f"Tabela {table_name} criada com sucesso!")

def main():
    # Lista todos os arquivos na pasta
    for file_name in os.listdir(CSV_FOLDER_PATH):
        # Verifica se é um arquivo CSV
        if file_name.endswith(".csv"):
            csv_path = os.path.join(CSV_FOLDER_PATH, file_name)
            print(f"Processando arquivo: {csv_path}")
            # Processa o arquivo CSV e cria a tabela no banco
            infer_schema_and_upload_to_db(csv_path, engine)

if __name__ == "__main__":
    main()
