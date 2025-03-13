from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Join_3_inner(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(
          in1.alias("in1"),
          ((col("in0.Year") == col("in1.Year")) & (col("in0.`Mega Category`") == col("in1.`Mega Category`"))),
          "inner"
        )\
        .select(col("in0.`Mega Category`").alias("Mega Category"), col("in0.`Incremental Savings High`").alias("Incremental Savings High"), col("in0.Year").alias("Year"), col("in0.`BAU Savings`").alias("BAU Savings"), col("in1.`% Allocation - BAU`").alias("% Allocation - BAU"), col("in0.`Incremental Savings Low`").alias("Incremental Savings Low"), col("in1.`% Allocation - Incremental`").alias("% Allocation - Incremental"), col("in1.EVP").alias("EVP"), col("in1.`Mega Category`").alias("Right_Mega Category"), col("in1.Year").alias("Right_Year"))
