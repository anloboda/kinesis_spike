import json
import logging
import os

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)
kinesis_client = boto3.client('kinesis', region_name='eu-west-1')
sqs_client = boto3.client('sqs', region_name='eu-west-1')
queue_url = os.getenv("QUEUE_URL")

def lambda_handler(event, context):
    logger.info(f"Received SNS event: {json.dumps(event, indent=2)}")

    for record in event['Records']:
        event_data = json.loads(record['Sns']['Message'])
        sns_subject = record['Sns'].get('Subject', 'No Subject')

        logger.info(f"SNS Message: {record['Sns']['Message']}")

        kinesis_batch_info = event_data["KinesisBatchInfo"]
        shard_id = kinesis_batch_info["shardId"]
        start_sequence_number = kinesis_batch_info["startSequenceNumber"]
        stream_arn = kinesis_batch_info["streamArn"]
        batch_size = kinesis_batch_info["batchSize"]

        stream_name = stream_arn.split('/')[-1]

        response = kinesis_client.get_shard_iterator(
            StreamName=stream_name,
            ShardId=shard_id,
            ShardIteratorType='AT_SEQUENCE_NUMBER',
            StartingSequenceNumber=start_sequence_number
        )

        shard_iterator = response['ShardIterator']

        records_response = kinesis_client.get_records(
            ShardIterator=shard_iterator,
            Limit=batch_size
        )

        if records_response['Records']:
            original_payload = []
            for kinesis_record in records_response['Records']:
                original_payload.append(kinesis_record['Data'].decode('utf-8'))
                logger.info(f"RECORD DATA {kinesis_record['Data']}")
            event_data["originalPayload"] = original_payload
            sqs_client.send_message(
                QueueUrl=queue_url,
                MessageBody=json.dumps(event_data),
            )

