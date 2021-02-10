from aws_cdk import (
    aws_iam as iam,
    aws_sqs as sqs,
    aws_sns as sns,
    aws_s3 as s3,
    aws_elasticsearch as elasticsearch,
    aws_events as events,
    aws_events_targets as targets,
    aws_kinesisfirehose as firehose,
    aws_lambda as _lambda,
    aws_lambda_event_sources as lambda_sources,
    aws_kinesis as kinesis,
    aws_glue as glue,
    aws_kinesisfirehose as firehose,
    aws_sns_subscriptions as subs,
    aws_lambda_python as lambda_python,
    core
)

from aws_solutions_constructs import aws_kinesis_streams_lambda  as kinesis_lambda

from common.configurations.glue_config import glue_column
from utils import get_code


class AnalyticsMlFlowStack(core.Stack):

    def __init__(self, scope: core.Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        # Kinesis to lambda
        self.stream_lambda = kinesis_lambda.KinesisStreamsToLambda(
            self, 'clickstream',
            lambda_function_props=_lambda.FunctionProps(
                runtime=_lambda.Runtime.PYTHON_3_7,
                handler='index.lambda_handler',
                code=_lambda.Code.inline(get_code('send_data_to_firehose.py'))
            ),
            kinesis_stream_props=kinesis.StreamProps(
                stream_name='clickstream',
                retention_period=core.Duration.days(1),
                shard_count=4
            ),
            kinesis_event_source_props=lambda_sources.KinesisEventSourceProps(
                starting_position=_lambda.StartingPosition.TRIM_HORIZON,
                batch_size=1
            )
        )

        # Lambda to produce data
        self.produce_fake_data = _lambda.Function(
            self, 'produce_data',
            runtime=_lambda.Runtime.PYTHON_3_7,
            timeout=core.Duration.seconds(90),
            handler='index.lambda_handler',
            code=_lambda.Code.inline(get_code('produce_data.py')),
            environment={
                'STREAM_NAME' : self.stream_lambda.kinesis_stream.stream_name
            }
        )
        self.stream_lambda.kinesis_stream.grant_read_write(self.produce_fake_data)

        # EventBridge to activate my function above
        self.event_rule = events.Rule(
            self, 'scheduledRule',
            schedule=events.Schedule.expression('rate(1 minute)')
        )

        self.event_rule.add_target(targets.LambdaFunction(self.produce_fake_data))


        # S3 Bucket
        self.bucket = s3.Bucket(
            self, 'data-clicks-lake',
            removal_policy=core.RemovalPolicy.DESTROY
        )
        

        # Glue
        self.glue_db_analytical = glue.Database(
            self,
            'analytic_clickstream',
            database_name='clickstream_db',
            location_uri=None,
        )

        self.glue_table_analytical = glue.Table(
            self,
            'analytical-table',
            table_name='analytical-table',
            columns=[
                glue_column('custid', 'int'),
                glue_column('trafficfrom', 'string'),
                glue_column('url', 'string'),
                glue_column('device', 'string'),
                glue_column('touchproduct', 'int'),
                glue_column('trans_timestamp', 'string')
            ],
            database=self.glue_db_analytical,
            data_format=glue.DataFormat.PARQUET,
            bucket=self.bucket,
            s3_prefix='kinesis/',
        )

        # Firehose
        Stream = firehose.CfnDeliveryStream
        ExtendedS3DestConfProp = Stream.ExtendedS3DestinationConfigurationProperty
        FormatConversionProp = Stream.DataFormatConversionConfigurationProperty
        InputFormatConfProp = Stream.InputFormatConfigurationProperty
        OutputFormatConfProp = Stream.OutputFormatConfigurationProperty
        DeserializerProperty = Stream.DeserializerProperty
        SerializerProperty = Stream.SerializerProperty
        OpenXJsonSerDeProperty = Stream.OpenXJsonSerDeProperty
        ParquetSerDeProperty = Stream.ParquetSerDeProperty
        BufferingHintsProp = Stream.BufferingHintsProperty
        SchemaConfigProp = Stream.SchemaConfigurationProperty
        SourceStreamProp = Stream.KinesisStreamSourceConfigurationProperty

        iam_role_firehose_analytical = self.create_firehose_role()
        self.bucket.grant_read_write(iam_role_firehose_analytical)

        self.delivery_stream = firehose.CfnDeliveryStream(
            self, 'deliveryClickstream',
            delivery_stream_name='deliveryClickStream',
            delivery_stream_type='KinesisStreamAsSource',
            kinesis_stream_source_configuration=SourceStreamProp(
                kinesis_stream_arn=self.stream_lambda.kinesis_stream.stream_arn,
                role_arn=iam_role_firehose_analytical.role_arn
                ),
            extended_s3_destination_configuration=ExtendedS3DestConfProp(
                bucket_arn=self.bucket.bucket_arn,
                role_arn=iam_role_firehose_analytical.role_arn,
                buffering_hints=BufferingHintsProp(
                    interval_in_seconds=60,
                    size_in_m_bs=128,
                ),
                data_format_conversion_configuration=FormatConversionProp(
                    enabled=True,
                    input_format_configuration=InputFormatConfProp(
                        deserializer=DeserializerProperty(
                            open_x_json_ser_de=OpenXJsonSerDeProperty(),
                        ),
                    ),
                    output_format_configuration=OutputFormatConfProp(
                        serializer=SerializerProperty(
                            parquet_ser_de=ParquetSerDeProperty(
                                compression='UNCOMPRESSED',
                                enable_dictionary_compression=False,
                            ),
                        )
                    ),
                    schema_configuration=SchemaConfigProp(
                        database_name=self.glue_db_analytical.database_name,
                        table_name=self.glue_table_analytical.table_name,
                        role_arn=iam_role_firehose_analytical.role_arn,
                    )
                ),
                prefix='kinesis/'
            ),
        )

        # Elasticsearch
        iam_es_statement = self.create_iam_statement_for_elasticsearch()

        self.es_domain = elasticsearch.Domain(
            self, 'ES_Domain',
            version=elasticsearch.ElasticsearchVersion.V6_8,
            access_policies=[iam_es_statement],
            capacity=elasticsearch.CapacityConfig(
                data_node_instance_type='m3.medium.elasticsearch',
                data_nodes=2,
                master_node_instance_type='m3.large.elasticsearch',
                master_nodes=2)
        )

        # Lambda to send data to Elasticsearch
        self.send_data_to_elasticsearch = lambda_python.PythonFunction(
            self, 'clickstream_to_es',
            entry='./analytics_ml_flow/lambda/lambda_with_requirements/',
            handler='handler',
            timeout=core.Duration.seconds(180),
            index='Kinesis_ES.py',
            environment={
                'ES_HOST_HTTP': self.es_domain.domain_endpoint,
                'ES_INDEX': 'clickstream',
                'ES_IND_TYPE': 'transactions',
                'ES_REGION': 'us-west-2',
            }
        )
        self.es_domain.grant_index_read_write('clickstream',self.send_data_to_elasticsearch)
        self.es_domain.grant_read_write(self.send_data_to_elasticsearch)

        stream_source = lambda_sources.KinesisEventSource(
            self.stream_lambda.kinesis_stream, 
            starting_position=_lambda.StartingPosition.TRIM_HORIZON,
            batch_size=1
        )

        self.stream_lambda.kinesis_stream.grant_read(self.send_data_to_elasticsearch)
        self.send_data_to_elasticsearch.add_event_source(stream_source)

        crawler_role = self.create_crawler_permissions()
        # Glue Cralwer Properties 
        crawlerCfn = glue.CfnCrawler
        targetProperty = crawlerCfn.TargetsProperty
        S3TargetProperty = crawlerCfn.S3TargetProperty
        ScheduleProperty = crawlerCfn.ScheduleProperty

        # Glue Crawler
        self.glue_crawler = glue.CfnCrawler(
            self, 'clickstream_crawler',
            role=crawler_role.role_arn,
            targets=targetProperty(
                s3_targets=[S3TargetProperty(
                    path=f's3://{self.bucket.bucket_name}/kinesis/'
                )]
            ),
            database_name='clickstream_db',
            name='clickstream',
            schedule=ScheduleProperty(schedule_expression='cron(0 * ? * * *)')
        )


    def create_iam_statement_for_elasticsearch(self):
        iam_es_statement = iam.PolicyStatement(
            actions=[
                'es:*'
            ],
            effect=iam.Effect.ALLOW
        )
        iam_es_statement.add_any_principal()
        iam_es_statement.add_all_resources()
        iam_es_statement.add_condition(key='IpAddress', value={"aws:SourceIp": "181.221.240.151/32"})
        return iam_es_statement

    def create_firehose_role(self):
        # Principal
        firehose_service_principal = iam.ServicePrincipal(
        service='firehose.amazonaws.com',
        )

        # Statement
        iam_analytical_statement = iam.PolicyStatement(
            actions=[
                'glue:GetTable',
                'glue:GetTableVersion',
                'glue:GetTableVersions',
            ],
            effect=iam.Effect.ALLOW,
            resources=[
                self.glue_db_analytical.catalog_arn,
                self.glue_db_analytical.database_arn,
                self.glue_table_analytical.table_arn,
            ],
        )
        # Statement
        iam_datastreams_firehose_statement = iam.PolicyStatement(
            actions=[
                'kinesis:*'
            ],
            effect=iam.Effect.ALLOW,
            resources=[
                self.stream_lambda.kinesis_stream.stream_arn
            ]
        )
        # Statement
        iam_s3_firehose_statement = iam.PolicyStatement(
            actions=[
                's3:*'
            ],
            effect=iam.Effect.ALLOW,
            resources=[
                self.bucket.bucket_arn
            ]
        )
        # Document with previous statements
        analytical_policy_document = iam.PolicyDocument(
            statements=[
                iam_analytical_statement,
                iam_datastreams_firehose_statement,
                iam_s3_firehose_statement
            ],
        )
        # Creation of a policy using the document
        analytical_policy = iam.ManagedPolicy(
            self,
            'sls-blog-analytical-glue-permissions',
            description='Permissions for a Kinesis Firehose Stream to access '
                        'the Glue "analytical" Database and Table',
            document=analytical_policy_document,
        )
        # Creating the Role using the policy
        iam_role_firehose_analytical = iam.Role(
            self,
            'self-firehose-to-s3',
            assumed_by=firehose_service_principal,
            managed_policies=[
                analytical_policy,
            ],
        )
        return iam_role_firehose_analytical

    # Glue Crawler Permissions
    def create_crawler_permissions(self):
        iam_glue_principal = iam.ServicePrincipal(
            service='glue.amazonaws.com',
        )
        iam_crawler_policy = iam.PolicyStatement(
            actions=[
                "s3:GetObject",
                "s3:PutObject"
            ],
            effect=iam.Effect.ALLOW,
            resources=[
                self.bucket.bucket_arn + '/kinesis*'
            ]
        )
        iam_crawler_policy_document = iam.PolicyDocument(statements=[iam_crawler_policy])

        crawler_policy = iam.ManagedPolicy(
            self,
            'clickstream_s3_permission',
            description='Permission from glue to put and get s3 data'
                        'the Glue "analytical" Database and Table',
            document=iam_crawler_policy_document,
        )

        crawler_role = iam.Role(
            self, 'clickstream_crawler_role',
            assumed_by=iam_glue_principal,
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(managed_policy_name='service-role/AWSGlueServiceRole'),
                crawler_policy
            ]
        )
        return crawler_role