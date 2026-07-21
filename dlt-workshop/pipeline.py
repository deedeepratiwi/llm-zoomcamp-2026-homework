import os
from pathlib import Path

import dlt
import requests
from dotenv import load_dotenv

load_dotenv(dotenv_path=Path(__file__).resolve().parent.parent / '.env')


def _parse_query_response(payload):
    columns = payload.get('columns', [])
    if not columns:
        return []

    row_count = len(columns[0].get('values', []))
    rows = []
    for index in range(row_count):
        row = {}
        for column in columns:
            row[column['name']] = column['values'][index]
        rows.append(row)
    return rows


@dlt.resource(name='spans', write_disposition='replace')
def logfire_records():
    token = os.getenv('LOGFIRE_READ_TOKEN')
    if not token:
        raise RuntimeError('LOGFIRE_READ_TOKEN is not set')

    sql = 'SELECT * FROM records'
    response = requests.get(
        'https://logfire-eu.pydantic.dev/v1/query',
        headers={'Authorization': f'Bearer {token}'},
        params={'sql': sql},
        timeout=60,
    )
    response.raise_for_status()

    payload = response.json()
    for row in _parse_query_response(payload):
        yield row


def main():
    pipeline = dlt.pipeline(
        pipeline_name='logfire_pipeline',
        destination='duckdb',
        dataset_name='agent_traces',
    )

    load_info = pipeline.run(logfire_records())
    print(load_info)


if __name__ == '__main__':
    main()