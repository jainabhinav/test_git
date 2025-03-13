from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def CrossTab_59(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("Saving Type", when(col("`Saving Type`").isNull(), lit("_Null_")).otherwise(col("`Saving Type`")))\
        .withColumn(
        "Saving Type",
        regexp_replace(
          col("`Saving Type`"), 
          "[\\s!@#$%^&*(),.?\":{}|<>\\[\\]=;/\\-+]", 
          "_"
        )
    )
