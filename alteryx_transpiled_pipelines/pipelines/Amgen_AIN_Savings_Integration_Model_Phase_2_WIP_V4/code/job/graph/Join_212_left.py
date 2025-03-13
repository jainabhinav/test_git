from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Join_212_left(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(in1.alias("in1"), (col("in0.Key") == col("in1.Key")), "leftanti")\
        .select(col("in0.EVP").alias("EVP"), col("in0.`Right_Right_tableau display mega category`").alias("Right_Right_tableau display mega category"), col("in0.`ADJ 2`").alias("ADJ 2"), col("in0.`Cost Center`").alias("Cost Center"), col("in0.`tableau display mega category`").alias("tableau display mega category"), col("in0.Function").alias("Function"), col("in0.`Right_ADJ 1`").alias("Right_ADJ 1"), col("in0.Cumulative/Incremental").alias("Cumulative/Incremental"), col("in0.AIN_Resource_Cost").alias("AIN_Resource_Cost"), col("in0.Right_Function").alias("Right_Function"), col("in0.`Avg_% Split`").alias("Avg_% Split"), col("in0.`Right_ADJ 2`").alias("Right_ADJ 2"), col("in0.Year").alias("Year"), col("in0.EW_Reduction").alias("EW_Reduction"), col("in0.`Right_Right_Right_tableau display mega category`")\
        .alias("Right_Right_Right_tableau display mega category"), col("in0.HLMC").alias("HLMC"), col("in0.AIN_Savings").alias("AIN_Savings"), col("in0.`ADJ 1`").alias("ADJ 1"), col("in0.`Right_tableau display mega category`").alias("Right_tableau display mega category"), col("in0.Key").alias("Key"), col("in0.Flag").alias("Flag"))
