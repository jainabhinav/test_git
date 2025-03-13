from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Join_150_inner(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(
          in1.alias("in1"),
          (
            (col("in0.`ADJ 1`") == col("in1.`ADJ 1`"))
            & (col("in0.`tableau display mega category`") == col("in1.`tableau display mega category`"))
          ),
          "inner"
        )\
        .select(col("in1.Cumulative/Incremental").alias("Cumulative/Incremental"), col("in1.AIN_Savings").alias("AIN_Savings"), col("in1.`ADJ 2`").alias("ADJ 2"), col("in1.Year").alias("Year"), col("in1.`tableau display mega category`").alias("Right_tableau display mega category"), col("in1.Key").alias("Key"), col("in0.`tableau display mega category`").alias("tableau display mega category"), col("in0.`ADJ 1`").alias("ADJ 1"), col("in0.`Avg_% Split`").alias("Avg_% Split"), col("in1.EW_Reduction").alias("EW_Reduction"), col("in1.Function").alias("Function"), col("in1.EVP").alias("EVP"), col("in1.Flag").alias("Flag"), col("in1.HLMC").alias("HLMC"), col("in1.`ADJ 1`").alias("Right_ADJ 1"), col("in1.AIN_Resource_Cost").alias("AIN_Resource_Cost"), col("in1.`Cost Center`").alias("Cost Center"))
