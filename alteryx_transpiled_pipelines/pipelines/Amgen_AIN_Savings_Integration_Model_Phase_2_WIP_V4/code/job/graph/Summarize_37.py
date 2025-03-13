from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Summarize_37(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(
        col("`tableau display mega category`").alias("tableau display mega category"), 
        col("`hyperion evp`").alias("hyperion evp"), 
        col("`source_cost center number`").alias("source_cost center number"), 
        col("`CTS v3`").alias("CTS v3"), 
        col("`hyperion hlmc`").alias("hyperion hlmc")
    )

    return df1.agg(sum(col("`Sum_Sum_Spend ($)`")).alias("Sum_Sum_Sum_Spend ($)"))
