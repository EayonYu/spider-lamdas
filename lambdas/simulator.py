import json
import boto3

iot_data_client = boto3.client('iot-data')


def lambda_handler(event, context):
    try:
        body = event.get("body", '')
        if not body:
            return {
                'statusCode': 400,
                'body': json.dumps('empty body')
            }

        data = json.loads(body)
        thing_name = data.get('thing_name', '')
        desired = data.get('desired', '')
        if not thing_name:
            return {
                'statusCode': 400,
                'body': json.dumps('empty thing name')
            }
        if not desired:
            return {
                'statusCode': 400,
                'body': json.dumps('empty desired')
            }

        print(thing_name)
        print(desired)
        payload = json.dumps({"state": {"desired": desired}})
        iot_data_client.update_thing_shadow(
            thingName=thing_name,
            payload=payload
        )

        return {
            'statusCode': 200
        }
    except Exception as e:
        return {
            'statusCode': 400,
            'body': str(e)
        }
