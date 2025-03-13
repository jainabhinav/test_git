from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AlteryxSelect_34(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("`tableau display category`").alias("tableau display category"), 
        col("Site"), 
        col("`scenario name`").alias("scenario name"), 
        col("`Regrouped Level 4`").alias("Regrouped Level 4"), 
        col("year"), 
        col("evp"), 
        col("`CTS v3`").alias("CTS v3"), 
        col("Sum_Adjusted_Sum_LCLCFX"), 
        col("`Non-Opex Adjustments`").alias("Non-Opex Adjustments"), 
        col("quarter"), 
        col("category"), 
        col("`hyperion hlmc`").alias("hlmc"), 
        col("`hyperion mc`").alias("mc"), 
        col("`source_cost center number`").alias("cost center number"), 
        col("`source_cost center`").alias("cost center"), 
        col("`company code number`").alias("company code"), 
        col("`Company Description`").alias("company"), 
        col("`Planning Account Description`").alias("pa account desc"), 
        col("`Planning Account`").alias("pa account"), 
        col("`GL Account Description Updated`").alias("gl account"), 
        col("`hyperion sub mc`").alias("sub mc"), 
        col("`source_mg code`").alias("material group"), 
        col("`source_mg code description`").alias("material group description"), 
        col("GL").alias("gl account number")
    )
