from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AlteryxSelect_262(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("`company code number`").cast(StringType()).alias("company code number"), 
        col("`source_cost center`").alias("source_cost center"), 
        col("`source_accounting date - quarter`").alias("source_accounting date - quarter"), 
        col("`source_mg code description`").alias("source_mg code description"), 
        col("`Sum_Spend ($)`").alias("Sum_Spend ($)"), 
        col("GL"), 
        col("Site"), 
        col("`Non-Opex Adjustments`").alias("Non-Opex Adjustments"), 
        col("`company code name`").alias("Company Description"), 
        col("`hyperion sub mc`").alias("hyperion sub mc"), 
        col("`tableau display mega category`").alias("tableau display mega category"), 
        col("`hyperion evp`").alias("hyperion evp"), 
        col("`Gl Account`").alias("Gl Account"), 
        col("`GL Account Description Updated`").alias("GL Account Description Updated"), 
        col("`source_cost center number`").alias("source_cost center number"), 
        col("`CTS v3`").alias("CTS v3"), 
        col("Category"), 
        col("`source_mg code`").alias("source_mg code"), 
        col("`Regrouped Level 4`").alias("Regrouped Level 4"), 
        col("`source_accounting date - year`").alias("source_accounting date - year"), 
        col("`Planning Account`").alias("Planning Account"), 
        col("`hyperion hlmc`").alias("hyperion hlmc"), 
        col(
          "PA#"
        ), 
        col("`hyperion mc`").alias("hyperion mc")
    )
