import base64


print('Loading function')


def lambda_handler(event, context):

    for record in event['Records']:

        payload = base64.b64decode(record['kinesis']['data'])
        print("Decoded payload: " + payload)
    return 'Successfully processed {} records.'.format(len(event['Records']))

