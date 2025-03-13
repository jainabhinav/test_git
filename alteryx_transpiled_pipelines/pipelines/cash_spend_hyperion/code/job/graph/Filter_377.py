from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Filter_377(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter(
        ~ (
          ((lower(col("`Regrouped Level 4`")).contains(lower(lit("FE&O"))).cast(BooleanType()) | lower(col("`Regrouped Level 4`")).contains(lower(lit("Outside Expenses"))).cast(BooleanType())).cast(BooleanType()) | lower(col("`Regrouped Level 4`")).contains(lower(lit("Staff Support"))).cast(BooleanType())).cast(BooleanType())
          | lower(col("`Regrouped Level 4`")).contains(lower(lit("Other"))).cast(BooleanType())
        )\
          .cast(
          BooleanType()
        )
    )
