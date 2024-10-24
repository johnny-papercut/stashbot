from google.cloud import bigquery
import requests

DATASET = 'stashbot'
GAMES_TABLE = f'{DATASET}.games'
SETTINGS_TABLE = f'{DATASET}.settings'


def bq_query(query: str) -> list:

    bq = bigquery.Client()
    job = bq.query(query)

    return [row for row in job.result()]


def write_to_bigquery(table: str, rows: list):
    bq = bigquery.Client()

    schema = [
        bigquery.SchemaField("user", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("game_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("game_name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("status", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("rating", "FLOAT", mode="NULLABLE"),
    ]

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    )

    bq.load_table_from_json(rows, table, job_config=job_config).result()


def get_setting(key: str) -> str:

    values = bq_query(f"SELECT value FROM `{SETTINGS_TABLE}` WHERE key = '{key}'")

    if not values:
        return 'NF'
    
    return values[0].value


def update_setting(key: str, value: str):
    if not bq_query("SELECT value FROM `{SETTINGS_TABLE}` WHERE key = '{key}'"):
        bq_query(f"INSERT INTO `{SETTINGS_TABLE}` (key, value) VALUES ('{key}', '{value}')")
    else:
        bq_query(f"UPDATE `{SETTINGS_TABLE}` SET value = '{value}' WHERE key = '{key}'")


def get_existing_games(user: str) -> dict:

    existing = {}

    for row in bq_query(f"SELECT * FROM `{GAMES_TABLE}` WHERE user = '{user}'"):
        existing[row.game_id] = {
            'user': row.user,
            'game_id': row.game_id,
            'game_name': row.game_name,
            'status': row.status,
            'rating': row.rating
        }

    return existing


def update_existing(user: str, added: list):

    game_ids = ', '.join([f"\"{g.get('game_id')}\"" for g in added])
    bq_query(f"DELETE FROM `{GAMES_TABLE}` WHERE game_id IN ({game_ids}) AND user = '{user}'")
    write_to_bigquery(GAMES_TABLE, added)


def send_to_webhook(messages: list, webhook: str):

    payload = {"content": '\n'.join(messages)}

    response = requests.post(webhook, json=payload)
    print(response.text)