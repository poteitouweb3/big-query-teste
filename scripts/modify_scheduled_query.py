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

# Obtenha a lista de nomes de consultas agendadas da variável de ambiente
scheduled_query_names_env = os.getenv("SCHEDULED_QUERIES")
if scheduled_query_names_env is None:
    raise EnvironmentError("The 'SCHEDULED_QUERIES' environment variable is not set.")

# Divida a string em uma lista de nomes
scheduled_query_names = scheduled_query_names_env.split(',')

# Encontre e atualize as consultas agendadas pelos nomes
transfer_configs = client.list_transfer_configs(parent=parent)
for transfer_config in transfer_configs:
    if transfer_config.display_name in scheduled_query_names:
        update_mask = field_mask_pb2.FieldMask(paths=["params"])
        transfer_config.params["query"] = sql  # Set the SQL query in params
        
        update_request = bqdts.UpdateTransferConfigRequest(
            transfer_config=transfer_config,
            update_mask=update_mask
        )
        
        updated_config = client.update_transfer_config(update_request)
        print(f"Updated scheduled query: {updated_config.display_name}")

# Informe se alguma consulta agendada especificada não foi encontrada
for query_name in scheduled_query_names:
    if not any(transfer_config.display_name == query_name for transfer_config in transfer_configs):
        print(f"Scheduled query with the name {query_name} not found.")
