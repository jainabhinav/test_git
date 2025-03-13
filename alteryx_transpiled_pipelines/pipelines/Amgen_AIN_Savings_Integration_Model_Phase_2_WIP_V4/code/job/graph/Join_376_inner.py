from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Join_376_inner(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(
          in1.alias("in1"),
          (
            ((((col("in0.year") == col("in1.year")) & (col("in0.Category") == col("in1.Category"))) & (col("in0.evp") == col("in1.evp"))) & (col("in0.`cost center number`") == col("in1.`cost center number`")))
            & (col("in0.hlmc") == col("in1.hlmc"))
          ),
          "inner"
        )\
        .select(col("in1.Category").alias("Right_Category"), col("in0.Sum_EW_Reduction").alias("Sum_EW_Reduction"), col("in1.Sum_AIN_Savings").alias("Right_Sum_AIN_Savings"), col("in0.year").alias("year"), col("in0.Sum_AIN_Savings").alias("Sum_AIN_Savings"), col("in1.hlmc").alias("Right_hlmc"), col("in1.evp").alias("Right_evp"), col("in0.`cost center number`").alias("cost center number"), col("in0.evp").alias("evp"), col("in0.Category").alias("Category"), col("in1.`cost center number`").alias("Right_cost center number"), col("in0.hlmc").alias("hlmc"), col("in1.Sum_EW_Reduction").alias("Right_Sum_EW_Reduction"), col("in1.year").alias("Right_year"))
