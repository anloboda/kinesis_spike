import aws_cdk as core
import aws_cdk.assertions as assertions

from kinesis_spike.kinesis_spike_stack import KinesisSpikeStack

# example tests. To run these tests, uncomment this file along with the example
# resource in kinesis_spike/kinesis_spike_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = KinesisSpikeStack(app, "kinesis-spike")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
