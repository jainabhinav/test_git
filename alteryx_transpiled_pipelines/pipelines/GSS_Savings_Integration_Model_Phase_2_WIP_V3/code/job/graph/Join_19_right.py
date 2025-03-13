from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Join_19_right(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(
          in1.alias("in1"),
          (
            ((col("in1.Category") == col("in0.`Mega Category`")) & (col("in1.year") == col("in0.Year")))
            & (col("in1.evp") == col("in0.EVP"))
          ),
          "leftanti"
        )\
        .select(col("in0.`% Allocation - Incremental`").alias("% Allocation - Incremental"), col("in0.EVP").alias("EVP"), col("in0.`Mega Category`").alias("Mega Category"), col("in0.`Incremental Savings High Split`").alias("Incremental Savings High Split"), col("in0.Right_Year").alias("Right_Year"), col("in0.`BAU Savings`").alias("BAU Savings"), col("in0.`Incremental Savings High`").alias("Incremental Savings High"), col("in0.`Right_Mega Category`").alias("Right_Mega Category"), col("in0.`Incremental Savings Low`").alias("Incremental Savings Low"), col("in0.`% Allocation - BAU`").alias("% Allocation - BAU"), col("in0.`Incremental Savings Low Split`").alias("Incremental Savings Low Split"), col("in0.`BAU Savings Split`").alias("BAU Savings Split"), col("in0.Year").alias("Year"))
