from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Join_46_left_UnionFullOuter(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(
          in1.alias("in1"),
          (
            ((col("in0.`Mega Category`") == col("in1.`Mega Category`")) & (col("in0.EVP") == col("in1.EVP")))
            & (col("in0.Year") == col("in1.Year"))
          ),
          "fullouter"
        )\
        .select(col("in0.`Mega Category`").alias("Mega Category"), col("in0.Year").alias("Year"), col("in0.`% Allocation - BAU`").alias("% Allocation - BAU"), col("in1.EVP").alias("Right_EVP"), col("in1.`% Allocation - Incremental`").alias("% Allocation - Incremental"), col("in0.EVP").alias("EVP"), col("in1.`Mega Category`").alias("Right_Mega Category"), col("in1.Year").alias("Right_Year"))
