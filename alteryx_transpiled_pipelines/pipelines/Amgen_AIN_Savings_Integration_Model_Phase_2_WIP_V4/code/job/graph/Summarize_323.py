from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Summarize_323(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(
        col("`OSE Labor V2`").alias("OSE Labor V2"), 
        col("ASHB"), 
        col("year"), 
        col("`cost center number`").alias("cost center number"), 
        col("`CTS v3`").alias("CTS v3")
    )

    return df1.agg(lit(1).alias("_dummy_"))
