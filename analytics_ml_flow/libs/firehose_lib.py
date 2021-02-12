from aws_cdk import (
    aws_kinesisfirehose as firehose,
    aws_kinesis as kinesis,
    aws_glue,
    aws_s3,
    aws_iam,
    core
)

class FirehoseProps:
    def __init__(self, bucket: aws_s3.Bucket, role: aws_iam.Role, stream: kinesis.Stream, glue_db: aws_glue.Database, glue_table: aws_glue.Table):
        self.bucket = bucket
        self.role = role
        self.stream = stream
        self.glue_db = glue_db
        self.glue_table = glue_table

class FirehoseLib:
    def __init__(self, scope: core.Construct, id: str, props: FirehoseProps, **kwargs) -> None:

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

        iam_role_firehose_analytical = props.role

        self.delivery_stream = firehose.CfnDeliveryStream(
            scope, 'deliveryClickstream',
            delivery_stream_name='deliveryClickStream',
            delivery_stream_type='KinesisStreamAsSource',
            kinesis_stream_source_configuration=SourceStreamProp(
                kinesis_stream_arn=props.stream.stream_arn,
                role_arn=iam_role_firehose_analytical.role_arn
                ),
            extended_s3_destination_configuration=ExtendedS3DestConfProp(
                bucket_arn=props.bucket.bucket_arn,
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
                        database_name=props.glue_db.database_name,
                        table_name=props.glue_table.table_name,
                        role_arn=iam_role_firehose_analytical.role_arn,
                    )
                ),
                prefix='kinesis/'
            ),
        )
