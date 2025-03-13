from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Join_312_left_UnionLeftOuter(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(
          in1.alias("in1"),
          ((col("in0.`Cost Center`") == col("in1.`cost center number`")) & (col("in0.Year") == col("in1.year"))),
          "leftouter"
        )\
        .select(col("in0.Cumulative/Incremental").alias("Cumulative/Incremental"), col("in1.`OSE Labor V2 Mapped in Data`").alias("OSE Labor V2 Mapped in Data"), col("in0.Sum_EW_Reduction").alias("Sum_EW_Reduction"), col("in1.`ASHB Mapped in Data`").alias("ASHB Mapped in Data"), col("in0.Year").alias("Year"), col("in0.Sum_AIN_Savings").alias("Sum_AIN_Savings"), col("in0.Sum_AIN_Resource_Cost").alias("Sum_AIN_Resource_Cost"), col("in1.`cost center number`").alias("cost center number"), col("in0.Function").alias("Function"), col("in1.`CTS Mapped in Data`").alias("CTS Mapped in Data"), col("in0.EVP").alias("EVP"), col("in0.HLMC").alias("HLMC"), col("in0.`Cost Center`").alias("Cost Center"), col("in1.year").alias("Right_year"))
