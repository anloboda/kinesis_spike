import logging
import random
from datetime import datetime
from aws_lambda_powertools.utilities.data_classes.kinesis_stream_event import KinesisStreamRecord
from aws_lambda_powertools.utilities.typing import LambdaContext
from aws_lambda_powertools.utilities.batch.types import PartialItemFailureResponse
from aws_lambda_powertools.utilities.batch import BatchProcessor, EventType
from aws_lambda_powertools.utilities.data_classes import (
    KinesisStreamEvent,
    event_source,
)

logger = logging.getLogger()
logger.setLevel(logging.INFO)

processor = BatchProcessor(event_type=EventType.KinesisDataStreams)


def handle_record(record: KinesisStreamRecord, context: LambdaContext):
    if random.choice([True, False]):
        logger.info(f"!!! exception for the record {record.kinesis.data_as_text()}")
        raise Exception("Random exception occurred!")
    else:
        logger.info(f"no exception for the record {record.kinesis.data_as_text()}")


@event_source(data_class=KinesisStreamEvent)
def lambda_handler(
        event: KinesisStreamEvent, context: LambdaContext
) -> PartialItemFailureResponse:
    batch = event["Records"]
    logger.info(f"Received the batch size: {len(batch)}, time: {datetime.now()}")
    with processor(
            records=batch, handler=lambda record: handle_record(record, context)
    ):
        processed_messages = processor.process()
        logger.info(f"Processed {len(processed_messages)} messages")

    response = processor.response()
    logger.info(f"Response from batch processor:{response}")
    return response



