from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AlteryxSelect_106(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("`In-Scope Final`").alias("In-Scope Final"), 
        col("mc"), 
        col("ASHB"), 
        col("`gl account number`").alias("gl account number"), 
        col("`tableau display category`").alias("tableau display category"), 
        col("Site"), 
        col("`scenario name`").alias("scenario name"), 
        col("hlmc"), 
        col("`Regrouped Level 4`").alias("Regrouped Level 4"), 
        col("year"), 
        col("`gl account`").alias("gl account"), 
        col("evp"), 
        col("`cost center number`").alias("cost center number"), 
        col("`CTS v3`").alias("CTS v3"), 
        col("`cost center`").alias("cost center"), 
        col("`sub mc`").alias("sub mc"), 
        col("`material group`").alias("material group"), 
        col("Sum_Adjusted_Sum_LCLCFX"), 
        col("`Non-Opex Adjustments`").alias("Non-Opex Adjustments"), 
        col("quarter"), 
        col("category"), 
        col("`material group description`").alias("material group description"), 
        col("`company code`").cast(DoubleType()).alias("company code"), 
        col("company").alias("Company Description"), 
        col("`pa account`").alias("Planning Account"), 
        col("`pa account desc`").alias("Planning Account Description"), 
        col("`OSE Labor`").alias("OSE Labor V2")
    )
