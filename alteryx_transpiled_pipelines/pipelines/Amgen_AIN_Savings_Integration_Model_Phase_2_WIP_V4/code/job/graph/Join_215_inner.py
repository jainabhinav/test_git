from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Join_215_inner(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(
          in1.alias("in1"),
          (
            ((col("in0.Key") == col("in1.Key")) & (col("in0.Year") == col("in1.Year")))
            & (col("in0.Cumulative/Incremental") == col("in1.Cumulative/Incremental"))
          ),
          "inner"
        )\
        .select(col("in0.Cumulative/Incremental").alias("Cumulative/Incremental"), col("in1.`Multiplying Factor`").alias("Multiplying Factor"), col("in0.AIN_Savings").alias("AIN_Savings"), col("in0.`ADJ 2`").alias("ADJ 2"), col("in1.Cumulative/Incremental").alias("Right_Cumulative/Incremental"), col("in0.Year").alias("Year"), col("in0.Key").alias("Key"), col("in0.`tableau display mega category`").alias("tableau display mega category"), col("in0.`ADJ 1`").alias("ADJ 1"), col("in1.Key").alias("Right_Key"), col("in1.`Sum_% Split`").alias("Sum_% Split"), col("in0.EW_Reduction").alias("EW_Reduction"), col("in0.Function").alias("Function"), col("in0.`% Split`").alias("% Split"), col("in0.EVP").alias("EVP"), col("in0.HLMC").alias("HLMC"), col("in0.AIN_Resource_Cost").alias("AIN_Resource_Cost"), col("in0.`Cost Center`").alias("Cost Center"), col("in1.Year").alias("Right_Year"))
