from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Formula_386(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("`pa account desc`").alias("pa account desc"), 
        col("Function"), 
        col("AIN_Savings"), 
        col("ASHB"), 
        col("Category"), 
        col("`scenario name`").alias("scenario name"), 
        col("`company code`").alias("company code"), 
        col("`sub mc`").alias("sub mc"), 
        col("site"), 
        col("AIN_Resource_Cost"), 
        col("`material group`").alias("material group"), 
        col("`OSE Labor`").alias("OSE Labor"), 
        col("quarter"), 
        col("Year"), 
        col("Sum_Sum_Adjusted_Sum_LCLCFX"), 
        col("`hyperion category code`").alias("hyperion category code"), 
        col("company"), 
        col("version"), 
        col("`scenario type`").alias("scenario type"), 
        col("Group"), 
        col("`planning sku`").alias("planning sku"), 
        col("`cost center number`").alias("cost center number"), 
        col("`gl account`").alias("gl account"), 
        col("Right_year"), 
        col("`hyperion category`").alias("hyperion category"), 
        col("evp"), 
        col("`Split Divide`").alias("Split Divide"), 
        col("Sum_Sum_Sum_Adjusted_Sum_LCLCFX"), 
        col("`pa account`").alias("pa account"), 
        col("`material group description`").alias("material group description"), 
        col("mc"), 
        col("hlmc"), 
        col("Cumulative/Incremental"), 
        col("`regrouped level 4`").alias("regrouped level 4"), 
        col("`CTS v3`").alias("CTS v3"), 
        col("`cost center`").alias("cost center"), 
        col("`product description`").alias("product description"), 
        col("EW_Reduction"), 
        col("`DTI % Split`").alias("DTI % Split"), 
        col("`Right_scenario name`").alias("Right_scenario name"), 
        col("`gl account number`").alias("gl account number"), 
        lit("DTI").cast(StringType()).alias("Pulled Using"), 
        lit("No").cast(StringType()).alias("Adjustment Required?")
    )
