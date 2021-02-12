from aws_cdk import (
    aws_glue as glue,
    aws_s3,
    aws_iam,
    core
)

class GlueCrawlerProps:
    def __init__(self, bucket: aws_s3.Bucket, role: aws_iam.Role):
        self.bucket = bucket
        self.role = role

class GlueCrawlerLib:
    def __init__(self, scope: core.Construct, id: str, props: GlueCrawlerProps, **kwargs) -> None:

        # Glue Cralwer Properties 
        crawlerCfn = glue.CfnCrawler
        targetProperty = crawlerCfn.TargetsProperty
        S3TargetProperty = crawlerCfn.S3TargetProperty
        ScheduleProperty = crawlerCfn.ScheduleProperty

        # Glue Crawler
        self.glue_crawler = glue.CfnCrawler(
            scope, 'clickstream_crawler',
            role=props.role.role_arn,
            targets=targetProperty(
                s3_targets=[S3TargetProperty(
                    path=f's3://{props.bucket.bucket_name}/kinesis/'
                )]
            ),
            database_name='clickstream_db',
            name='clickstream',
            schedule=ScheduleProperty(schedule_expression='cron(0 * ? * * *)')
        )