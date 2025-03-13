from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Join_143_inner(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(
          in1.alias("in1"),
          (
            (col("in0.Key") == col("in1.Key"))
            & (col("in0.`tableau display mega category`") == col("in1.`tableau display mega category`"))
          ),
          "inner"
        )\
        .select(col("in0.`Sum_Sum_Sum_Spend ($)`").alias("Sum_Sum_Sum_Spend ($)"), col("in1.Cumulative/Incremental").alias("Cumulative/Incremental"), col("in1.AIN_Savings").alias("AIN_Savings"), col("in1.`ADJ 2`").alias("ADJ 2"), col("in1.`tableau display mega category`").alias("Right_Right_tableau display mega category"), col("in1.Year").alias("Year"), col("in0.`Sum_Sum_Sum_Sum_Spend ($)`").alias("Sum_Sum_Sum_Sum_Spend ($)"), col("in1.`tableau display mega category`").alias("Right_tableau display mega category"), col("in1.Key").alias("Right_Right_Key"), col("in0.Key").alias("Key"), col("in0.`tableau display mega category`").alias("tableau display mega category"), col("in1.`ADJ 1`").alias("ADJ 1"), col("in1.Key").alias("Right_Key"), col("in0.`hyperion evp`").alias("hyperion evp"), col("in1.EW_Reduction").alias("EW_Reduction"), col("in1.Function").alias("Function"), col("in0.`% Split`").alias("% Split"), col("in0.`source_cost center number`").alias("source_cost center number"), col("in1.EVP").alias("EVP"), col("in0.`CTS v3`").alias("CTS v3"), col("in1.Flag").alias("Flag"), col("in1.HLMC").alias("HLMC"), col("in1.AIN_Resource_Cost").alias("AIN_Resource_Cost"), col("in1.`Cost Center`").alias("Cost Center"), col("in0.`hyperion hlmc`").alias("hyperion hlmc"))
