from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Join_328_inner(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(in1.alias("in1"), (col("in0.`Unique Key`") == col("in1.`Unique Key`")), "inner")\
        .select(col("in0.`Cost Center Exists in Baseline?`").alias("Cost Center Exists in Baseline?"), col("in0.Cumulative/Incremental").alias("Cumulative/Incremental"), col("in1.Count").alias("Count"), col("in0.`OSE Labor V2 Mapped in Data`").alias("OSE Labor V2 Mapped in Data"), col("in0.Sum_EW_Reduction").alias("Sum_EW_Reduction"), col("in0.`ASHB Mapped in Data`").alias("ASHB Mapped in Data"), col("in0.Year").alias("Year"), col("in0.`Hyperion CTS <> AIN Input CTS`").alias("Hyperion CTS <> AIN Input CTS"), col("in0.Sum_AIN_Savings").alias("Sum_AIN_Savings"), col("in0.`OSE Labor V2 <> Labor?`").alias("OSE Labor V2 <> Labor?"), col("in0.`Cost Center Missing in Baseline?`").alias("Cost Center Missing in Baseline?"), col("in0.`ASHB = 0 or 'Not Found'?`").alias("ASHB = 0 or 'Not Found'?"), col("in0.Sum_AIN_Resource_Cost").alias("Sum_AIN_Resource_Cost"), col("in0.`cost center number`").alias("cost center number"), col("in0.Function").alias("Function"), col("in0.`CTS Mapped in Data`").alias("CTS Mapped in Data"), col("in0.EVP").alias("EVP"), col("in1.`Unique Key`").alias("Right_Unique Key"), col("in0.HLMC").alias("HLMC"), col("in0.`Unique Key`").alias("Unique Key"), col("in0.`Cost Center`").alias("Cost Center"), col("in0.Right_year").alias("Right_year"))
