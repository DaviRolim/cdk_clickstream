from aws_cdk import (
    aws_iam as iam
)

def create_firehose_role(self):
    firehose_service_principal = iam.ServicePrincipal(
        service='firehose.amazonaws.com',
    )
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

    iam_datastreams_firehose_statement = iam.PolicyStatement(
        actions=[
            'kinesis:*'
        ],
        effect=iam.Effect.ALLOW,
        resources=[
            self.stream_lambda.kinesis_stream.stream_arn
        ]
    )

    iam_s3_firehose_statement = iam.PolicyStatement(
        actions=[
            's3:*'
        ],
        effect=iam.Effect.ALLOW,
        resources=[
            self.bucket.bucket_arn
        ]
    )

    analytical_policy_document = iam.PolicyDocument(
        statements=[
            iam_analytical_statement,
            iam_datastreams_firehose_statement,
            iam_s3_firehose_statement
        ],
    )

    analytical_policy = iam.ManagedPolicy(
        self,
        'sls-blog-analytical-glue-permissions',
        description='Permissions for a Kinesis Firehose Stream to access '
                    'the Glue "analytical" Database and Table',
        document=analytical_policy_document,
    )

    iam_role_firehose_analytical = iam.Role(
        self,
        'self-firehose-to-s3',
        assumed_by=firehose_service_principal,
        managed_policies=[
            analytical_policy,
        ],
    )
    return iam_role_firehose_analytical
