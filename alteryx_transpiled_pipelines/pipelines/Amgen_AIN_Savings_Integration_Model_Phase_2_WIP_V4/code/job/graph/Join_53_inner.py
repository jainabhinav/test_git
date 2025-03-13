from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Join_53_inner(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(in1.alias("in1"), (col("in0.Key") == col("in1.Key")), "inner")\
        .select(col("in0.`Sum_Sum_Sum_Spend ($)`").alias("Sum_Sum_Sum_Spend ($)"), col("in1.`Sum_Sum_Sum_Sum_Spend ($)`").alias("Sum_Sum_Sum_Sum_Spend ($)"), col("in0.Key").alias("Key"), col("in0.`tableau display mega category`").alias("tableau display mega category"), col("in0.`hyperion evp`").alias("hyperion evp"), col("in0.`source_cost center number`").alias("source_cost center number"), col("in0.`CTS v3`").alias("CTS v3"), col("in0.`hyperion hlmc`").alias("hyperion hlmc"))
