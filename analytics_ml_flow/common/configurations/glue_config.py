from aws_cdk import (
    aws_glue as glue,
    core
)

from enum import Enum

# class COL_TYPES(Enum):
#     INT = 'int'
#     STRING = 'string'
#     DATE = 'date'
#     TIMESTAMP = 'timestamp'

def glue_column(name, col_type, is_primitive=True):
    return glue.Column(name=name, type=glue.Type(input_string=col_type, is_primitive=is_primitive))
