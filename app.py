#!/usr/bin/env python3

from aws_cdk import core

from analytics_ml_flow.analytics_ml_flow_stack import AnalyticsMlFlowStack


app = core.App()
AnalyticsMlFlowStack(app, "analytics-ml-flow", env={'region': 'us-west-2'})

app.synth()
