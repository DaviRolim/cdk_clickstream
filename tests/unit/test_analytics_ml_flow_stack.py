import json
import pytest

from aws_cdk import core
from analytics-ml-flow.analytics_ml_flow_stack import AnalyticsMlFlowStack


def get_template():
    app = core.App()
    AnalyticsMlFlowStack(app, "analytics-ml-flow")
    return json.dumps(app.synth().get_stack("analytics-ml-flow").template)


def test_sqs_queue_created():
    assert("AWS::SQS::Queue" in get_template())


def test_sns_topic_created():
    assert("AWS::SNS::Topic" in get_template())
