from google.cloud import bigquery_datatransfer_v1 as bqdts
from google.protobuf import field_mask_pb2
import google.auth
import os

# Certifique-se de que as credenciais e o projeto são passados para o cliente
project_id = os.getenv('GCP_PROJECT_ID')
credentials, _ = google.auth.default()

client = bqdts.DataTransferServiceClient(credentials=credentials)

# Use o ID do projeto correto ao listar as configurações de transferência
parent = f"projects/{project_id}"

# Carregue a consulta SQL a partir de um arquivo
sql_file_path = os.getenv("QUERY_FILE")
if sql_file_path is None:
    raise EnvironmentError("The 'QUERY_FILE' environment variable is not set.")

with open(sql_file_path, "r") as sql_file:
    sql = sql_file.read()

# Identificador da consulta agendada
scheduled_query_name = os.getenv("SCHEDULED_QUERY_NAME")
if scheduled_query_name is None:
    raise EnvironmentError("The 'SCHEDULED_QUERY_NAME' environment variable is not set.")

# Encontre a consulta agendada pelo nome
transfer_configs = client.list_transfer_configs(parent=parent)
for transfer_config in transfer_configs:
    if transfer_config.display_name == scheduled_query_name:
        # Prepare o update mask e o novo objeto transfer config
        update_mask = field_mask_pb2.FieldMask(paths=["params"])
        transfer_config.params["query"] = sql  # Set the SQL query in params
        
        # Cria o UpdateTransferConfigRequest
        update_request = bqdts.UpdateTransferConfigRequest(
            transfer_config=transfer_config,
            update_mask=update_mask
        )
        
        # Atualize a consulta agendada
        updated_config = client.update_transfer_config(update_request)
        print(f"Updated scheduled query: {updated_config.display_name}")
        break
else:
    print(f"Scheduled query with the name {scheduled_query_name} not found.")
