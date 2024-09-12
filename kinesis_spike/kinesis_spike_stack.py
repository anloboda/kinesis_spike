from aws_cdk import (
    aws_kinesis as kinesis,
    aws_lambda as _lambda,
    aws_lambda_event_sources as lambda_event_sources,
    aws_sns as sns,
    aws_sqs as sqs,
    aws_sns_subscriptions as subscriptions,
    aws_iam as iam,
    Stack,
)
from constructs import Construct


class KinesisSpikeStack(Stack):

    def __init__(self, scope: Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        kinesis_stream = kinesis.Stream(self, "TestKinesisStream", stream_name="TestStream")
        failure_topic = sns.Topic(self, "FailedKinesisRecordsTopic", topic_name="FailedKinesisRecordsTopic")
        failed_dlq = sqs.Queue(self, "FailedKinesisRecordDLQ", queue_name="FailedKinesisRecordDLQ")

        powertools_layer = _lambda.LayerVersion.from_layer_version_arn(
            self,
            "PowertoolsLayer",
            f"arn:aws:lambda:{self.region}:017000801446:layer:AWSLambdaPowertoolsPythonV2:42"
        )

        publisher = _lambda.Function(self, "KinesisPublisher",
            function_name='kinesis-publisher-lambda-cdk',
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler="kinesis_publisher.lambda_handler",
            code=_lambda.Code.from_asset("./lambda_kinesis_publisher"),
        )

        kinesis_stream.grant_write(publisher)

        processor = _lambda.Function(self, "KinesisRecordProcessor",
            function_name='kinesis-record-processor-lambda-cdk',
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler="processor.lambda_handler",
            code=_lambda.Code.from_asset("./lambda_processor"),
            layers=[powertools_layer]
        )

        kinesis_stream.grant_read(processor)
        failure_topic.grant_publish(processor)

        event_source_mapping = lambda_event_sources.KinesisEventSource(
            kinesis_stream,
            batch_size=10,
            starting_position=_lambda.StartingPosition.TRIM_HORIZON,
            report_batch_item_failures=True,
            on_failure=lambda_event_sources.SnsDlq(failure_topic),
            retry_attempts=2
        )
        processor.add_event_source(event_source_mapping)

        hydrate_failed_records = _lambda.Function(self, "HydrateOriginalRecord",
            function_name='hydrate-original-record-lambda-cdk',
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler="kinesis_payload_hydrator.lambda_handler",
            code=_lambda.Code.from_asset("./lambda_hydrator"),
            environment= {
                "QUEUE_URL": failed_dlq.queue_url,
            }
        )
        kinesis_stream.grant_read(hydrate_failed_records)
        failure_topic.add_subscription(subscriptions.LambdaSubscription(hydrate_failed_records))
        failed_dlq.grant_send_messages(hydrate_failed_records)
