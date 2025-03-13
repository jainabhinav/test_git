from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AlteryxSelect_38(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("`Mega Category Old`").alias("Mega Category Old"), 
        col("`company code number`").alias("company code number"), 
        col("`source_cost center`").alias("source_cost center"), 
        col("`source_accounting date - quarter`").alias("source_accounting date - quarter"), 
        col("`source_mg code description`").alias("source_mg code description"), 
        col("`Sum_Spend ($)`").alias("Sum_Spend ($)"), 
        col("GL"), 
        col("Site"), 
        col("`tableau display category`").alias("tableau display category"), 
        col("`hyperion sub mc`").alias("hyperion sub mc"), 
        col("`tableau display mega category`").alias("tableau display mega category"), 
        col("`Category Old`").alias("Category Old"), 
        col("`hyperion evp`").alias("hyperion evp"), 
        col("`GL Account Description Updated`").alias("GL Account Description Updated"), 
        col("`source_cost center number`").alias("source_cost center number"), 
        col("`CTS v3`").alias("CTS v3"), 
        col("`source_mg code`").alias("source_mg code"), 
        col("`source_accounting date - year`").alias("source_accounting date - year"), 
        col("`hyperion hlmc`").alias("hyperion hlmc"), 
        col("`company code name`").alias("company code name"), 
        col("`hyperion mc`").alias("hyperion mc")
    )
