import json
import base64

from lambdas.consume import consume


def lambda_handler(event, context):
    for record in event['Records']:
        try:
            payload = base64.b64decode(record['kinesis']['data'])
            data = json.loads(payload)
            consume(data)
        except Exception as e:
            print(e)
            continue
