from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Join_12_right(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(
          in1.alias("in1"),
          (
            col("in1.GL")
            == col(
              "in0.`Gl Account (# only)`"
            )
          ),
          "leftanti"
        )\
        .select(
        col("in0.`Planning Account`").alias("Planning Account"), 
        col("in0.Category").alias("Category"), 
        col("in0.`Regrouped Level 4`").alias("Regrouped Level 4"), 
        col(
            "in0.`Gl Account (# only)`"
          )\
          .alias(
          "Gl Account (# only)"
        ), 
        col("in0.`Gl Account`").alias("Gl Account")
    )
