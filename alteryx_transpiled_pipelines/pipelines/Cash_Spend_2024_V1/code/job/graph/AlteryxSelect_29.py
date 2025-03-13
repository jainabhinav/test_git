from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AlteryxSelect_29(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("`GL Account Description Updated`").alias("GL Account Description Updated"), 
        col("`source_accounting date - year`").alias("source_accounting date - year"), 
        col("Flag"), 
        col("`tableau display category`").alias("tableau display category"), 
        col("`source_mg code description`").alias("source_mg code description"), 
        col("`hyperion evp`").alias("hyperion evp"), 
        col("GL"), 
        col("Site"), 
        col("`Category Old`").alias("Category Old"), 
        col("`hyperion sub mc`").alias("hyperion sub mc"), 
        col("`source_cost center`").alias("source_cost center"), 
        col("`source_cost center number`").alias("source_cost center number"), 
        col("`hyperion mc`").alias("hyperion mc"), 
        col("`company code name`").alias("company code name"), 
        col("`company code number`").alias("company code number"), 
        col("`source_mg code`").alias("source_mg code"), 
        col("`CTS v3`").alias("CTS v3"), 
        col("`Mega Category Old`").alias("Mega Category Old"), 
        col("`hyperion hlmc`").alias("hyperion hlmc"), 
        col("`tableau display mega category`").alias("tableau display mega category"), 
        col("`source_accounting date - quarter`").alias("source_accounting date - quarter"), 
        col("`Sum_Spend ($)`").alias("Non-Opex Adjustments")
    )
