import logging
from datetime import datetime

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

kinesis_client = boto3.client('kinesis', region_name='eu-west-1')


def lambda_handler(event, context):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    records = [
        {'Data': f"{now} - record1", 'PartitionKey': '1'},
        {'Data': f"{now} - record2", 'PartitionKey': '1'},
        {'Data': f"{now} - record3", 'PartitionKey': '1'},
        {'Data': f"{now} - record4", 'PartitionKey': '1'},
        {'Data': f"{now} - record5", 'PartitionKey': '1'},
        {'Data': f"{now} - record6", 'PartitionKey': '1'},
        {'Data': f"{now} - record7", 'PartitionKey': '1'},
        {'Data': f"{now} - record8", 'PartitionKey': '1'},
    ]

    logger.info(f"Sending batch to kinesis: {records}")

    kinesis_client.put_records(
        StreamName="TestStream",
        Records=records,
    )

