import json
import base64

from layer import Layer
from layer.env import Env

from lambdas.consume import consume


def lambda_handler(event, _context):
    l = Layer(Env.DEV)

    for record in event['Records']:
        try:
            payload = base64.b64decode(record['kinesis']['data'])
            data = json.loads(payload)
            consume(data, l)
        except Exception as e:
            print(e)
            continue
