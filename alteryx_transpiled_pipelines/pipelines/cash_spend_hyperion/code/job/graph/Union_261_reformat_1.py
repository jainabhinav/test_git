from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Union_261_reformat_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("`_CTS v3`").alias("_CTS v3"), 
        lit(None).cast(StringType()).alias("_Category"), 
        col("`_Category Old`").alias("_Category Old"), 
        col("_Flag").cast(StringType()).alias("_Flag"), 
        col("_GL"), 
        col("`_GL Account Description Updated`").alias("_GL Account Description Updated"), 
        lit(None).cast(StringType()).alias("_Gl Account"), 
        lit(None)\
          .cast(StringType())\
          .alias(
          "_Gl Account (# only)"
        ), 
        col("`_Mega Category Old`").alias("_Mega Category Old"), 
        col("`_Non-Opex Adjustments`").cast(DoubleType()).alias("_Non-Opex Adjustments"), 
        lit(None)\
          .cast(StringType())\
          .alias(
          "_PA#"
        ), 
        lit(None).cast(StringType()).alias("_Planning Account"), 
        lit(None).cast(StringType()).alias("_Regrouped Level 4"), 
        col("_Site"), 
        lit(None).cast(DoubleType()).alias("_Sum_Spend ($)"), 
        col("`_company code name`").alias("_company code name"), 
        col("`_company code number`").alias("_company code number"), 
        col("`_hyperion evp`").alias("_hyperion evp"), 
        col("`_hyperion hlmc`").alias("_hyperion hlmc"), 
        col("`_hyperion mc`").alias("_hyperion mc"), 
        col("`_hyperion sub mc`").alias("_hyperion sub mc"), 
        col("`_source_accounting date - quarter`").alias("_source_accounting date - quarter"), 
        col("`_source_accounting date - year`").alias("_source_accounting date - year"), 
        col("`_source_cost center`").alias("_source_cost center"), 
        col("`_source_cost center number`").alias("_source_cost center number"), 
        col("`_source_mg code`").alias("_source_mg code"), 
        col("`_source_mg code description`").alias("_source_mg code description"), 
        col("`_tableau display category`").alias("_tableau display category"), 
        col("`_tableau display mega category`").alias("_tableau display mega category")
    )
