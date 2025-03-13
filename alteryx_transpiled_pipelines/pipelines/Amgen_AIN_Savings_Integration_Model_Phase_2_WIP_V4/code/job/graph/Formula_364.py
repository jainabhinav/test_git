from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Formula_364(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("Pulled Using", col("`Adjustment Required?`").cast(StringType()))\
        .withColumn(
          "Adjustment Required?",
          when((col("`Pulled Using`") == lit("Function + EVP + HLMC + Cost Center")).cast(BooleanType()), lit("No"))\
            .otherwise(lit("Yes"))\
            .cast(StringType())
        )\
        .withColumn("AIN_Savings", when((col("Year") == lit("2024")).cast(BooleanType()), (col("AIN_Savings") * lit(2)))\
        .otherwise(col("AIN_Savings"))\
        .cast(DoubleType()))
