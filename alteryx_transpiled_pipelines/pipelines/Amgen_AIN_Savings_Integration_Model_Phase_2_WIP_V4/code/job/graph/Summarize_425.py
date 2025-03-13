from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Summarize_425(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(
        col("Function"), 
        col("`tableau display mega category`").alias("tableau display mega category"), 
        col("`Cost Center`").alias("Cost Center"), 
        col("`New % Split`").alias("New % Split"), 
        col("EVP"), 
        col("HLMC")
    )

    return df1.agg(lit(1).alias("_dummy_"))
