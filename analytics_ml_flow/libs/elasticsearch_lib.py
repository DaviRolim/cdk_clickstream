from aws_cdk import (
    aws_iam as iam,
    aws_elasticsearch as elasticsearch,
    core
)

class ElasticsearchLib:
    def __init__(self, scope: core.Construct, id: str, **kwargs) -> None:

        # Elasticsearch
        iam_es_statement = self.create_iam_statement_for_elasticsearch()

        self.es_domain = elasticsearch.Domain(
            scope, 'ES_Domain',
            version=elasticsearch.ElasticsearchVersion.V6_8,
            access_policies=[iam_es_statement],
            capacity=elasticsearch.CapacityConfig(
                data_node_instance_type='m3.medium.elasticsearch',
                data_nodes=2,
                master_node_instance_type='m3.large.elasticsearch',
                master_nodes=2)
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
