from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Join_453_inner(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(
          in1.alias("in1"),
          (
            col("in0.`Planning Account`")
            == col(
              "in1.#PA"
            )
          ),
          "inner"
        )\
        .select(col("in0.Category").alias("Category"), col("in0.Percent_of_Account").alias("Percent_of_Account"), col("in1.`Planning Account`").alias("Planning Account"))
