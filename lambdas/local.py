import json
import time

from layer import Layer
from layer.env import Env

from lambdas.consume import consume


def main():
    l = Layer(Env.LOCAL)

    stream = 'prod-mirror-update'
    response = l.aws.kinesis_client.describe_stream(StreamName=stream)
    shard_id = response['StreamDescription']['Shards'][0]['ShardId']
    shard_iterator = l.aws.kinesis_client.get_shard_iterator(
        StreamName=stream,
        ShardId=shard_id,
        ShardIteratorType='LATEST'
    )
    next_shard_iterator = shard_iterator['ShardIterator']

    while True:
        record_response = l.aws.kinesis_client.get_records(ShardIterator=next_shard_iterator, Limit=1)
        mill_is_behind_latest = record_response.get('MillisBehindLatest', 0)
        next_shard_iterator = record_response.get('NextShardIterator', None)
        if not next_shard_iterator:
            print('shard has been closed')
            return
        if mill_is_behind_latest == 0:
            print('waiting...')
            time.sleep(3)
            continue

        records = record_response.get('Records', [])
        for record in records:
            try:
                print(f'PartitionKey:{record["PartitionKey"]}, SequenceNumber: {record["SequenceNumber"]}')
                data = json.loads(record['Data'])
                consume(data)
            except Exception as e:
                print(e)
                continue

        time.sleep(1)


if __name__ == '__main__':
    main()
