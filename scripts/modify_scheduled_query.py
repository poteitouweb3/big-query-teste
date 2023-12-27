from google.cloud import bigquery_datatransfer
import google.auth
import os

# Certifique-se de que as credenciais e o projeto são passados para o cliente
project_id = os.getenv('GCP_PROJECT_ID')
credentials, _ = google.auth.default()

client = bigquery_datatransfer.DataTransferServiceClient(credentials=credentials)

# Use o ID do projeto correto ao listar as configurações de transferência
parent = f"projects/{project_id}"

# Carregue a consulta SQL a partir de um arquivo
with open(os.getenv("QUERY_FILE"), "r") as sql_file:
    sql = sql_file.read()

# Identificador da consulta agendada
scheduled_query_name = os.getenv("SCHEDULED_QUERY_NAME")

# Encontre a consulta agendada pelo nome
transfer_configs = client.list_transfer_configs(parent=parent)
for transfer_config in transfer_configs:
    if transfer_config.display_name == scheduled_query_name:
        # Atualize a consulta agendada
        transfer_config.query = sql
        updated_config = client.update_transfer_config(transfer_config, fields=["query"])
        print(f"Updated scheduled query: {updated_config.display_name}")
        break
else:
    print(f"Scheduled query with the name {scheduled_query_name} not found.")
