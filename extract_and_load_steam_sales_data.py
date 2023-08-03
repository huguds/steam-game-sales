import requests
import pandas as pd
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import configparser

# Lê as configurações do arquivo config.ini
config = configparser.ConfigParser()
config.read('config.ini')

# Variáveis
app_ids = []  # App IDs dos jogos
access_token = config.get('Steam', 'access_token')
base_url = 'https://store.steampowered.com/api/appdetails'

# ###################################################### Requisição  #####################################################################################
# Requisição para obter a lista de App IDs dos jogos na Steam
response_appid = requests.get(f'https://api.steampowered.com/ISteamApps/GetAppList/v2/?key={access_token}')

# Verifica o status code da requisição
if response_appid.status_code == 200:
    data_app_id = response_appid.json()

# Filtra os dados para obter somente os App IDs
data_app_id = data_app_id['applist']['apps']

# Definir um limite para o retorno dos dados
limit = 125
contador = 0

# Percorre os dados e adiciona os App IDs em uma lista
for data in data_app_id:
    app_id = data['appid']
    params = {'appids': f'{app_id}'}
    response = requests.get(base_url, params=params)
    if response.status_code == 200:
        game_data = response.json().get(str(app_id), {})
        if game_data.get('success', False):  # Verifica se 'success' é verdadeiro
            app_ids.append(app_id)
            contador += 1
            print(contador)
    if contador == limit:
        break


# Cria um DataFrame vazio para armazenar os dados dos jogos
df = pd.DataFrame(columns=['game_name', 'app_id', 'game_price', 'game_rating', 'game_genre'])

# Função para obter as informações dos jogos na Steam e adicioná-las ao DataFrame
def get_game_info(app_ids):
    apps_ids = app_ids
    for app_id in apps_ids:
        params = {'appids': f'{app_id}'}
        response = requests.get(base_url, params=params)
        if response.status_code == 200:
            game_data = response.json().get(str(app_id), {})
            try:
                if game_data.get('success', False):  # Verifica se 'success' é verdadeiro
                    game_name = game_data['data']['name']
                    game_price = game_data['data']['price_overview']['final'] / 100
                    game_rating = game_data['data']['metacritic']['score'] if 'metacritic' in game_data['data'] else 'N/A'
                    genres_list = game_data['data']['genres'] if 'genres' in game_data['data'] else []
                    game_genre = ', '.join(genre['description'] for genre in genres_list) if genres_list else 'N/A'
                    # Adiciona os dados do jogo ao DataFrame
                    df.loc[len(df)] = [game_name, app_id, game_price, game_rating, game_genre]
                else:
                    print(f'Error fetching data for App ID: {app_id}')
            except KeyError:
                print(f'Error accessing price or rating for App ID: {app_id}')
        else:
            print('Error: Unable to fetch game information.')

# Remove a palavra "game_" do nome das colunas antes de carregar os dados no BigQuery
df.columns = df.columns.str.replace('game_', '')

# Chamada da função para obter as informações dos jogos
get_game_info(app_ids)

# Exibe o DataFrame com os dados limpos
print(df.head())

################################################## Salvar os dados no BigQuery ################################################################

# Cria um cliente do BigQuery
client = bigquery.Client()

# Defina o ID do projeto e o ID do conjunto de dados criado no BigQuery
project_id = 'voltaic-charter-394503'
dataset_id = 'steam_game_sales'

# Nome da tabela no BigQuery (sem o caminho completo)
table_name = 'steam_sales_data'

# Cria uma tabela no BigQuery
table_id = f"{project_id}.{dataset_id}.{table_name}"

schema = [
    bigquery.SchemaField("name", "STRING"),
    bigquery.SchemaField("app_id", "INTEGER"),
    bigquery.SchemaField("price", "FLOAT"),
    bigquery.SchemaField("rating", "STRING"),
    bigquery.SchemaField("genre", "STRING")
]

# Verifica se o conjunto de dados já existe
dataset_ref = client.dataset(dataset_id)
try:
    client.get_dataset(dataset_ref)
    print(f"O conjunto de dados {dataset_id} já existe.")
except NotFound:
    # Cria o conjunto de dados no BigQuery
    dataset = bigquery.Dataset(dataset_ref)
    dataset = client.create_dataset(dataset)
    print(f"O conjunto de dados {dataset_id} foi criado com sucesso.")

# Cria uma tabela no BigQuery (se ainda não existir)
table_ref = dataset_ref.table(table_name)
try:
    client.get_table(table_ref)
    print(f"A tabela {table_name} já existe. Os dados serão adicionados a ela.")
except NotFound:
    # Cria a tabela no BigQuery
    table = bigquery.Table(table_ref, schema=schema)
    table = client.create_table(table)
    print(f"A tabela {table_name} foi criada com sucesso.")

print(df.head())

# Carrega os dados no BigQuery
job = client.load_table_from_dataframe(df, table_ref)
job.result()
print(f"Os dados foram carregados com sucesso na tabela {table_name} no BigQuery.")