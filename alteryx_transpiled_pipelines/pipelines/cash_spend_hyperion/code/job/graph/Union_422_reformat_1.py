from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Union_422_reformat_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("`_Exclusion Type`").alias("_Exclusion Type"), 
        col("`_Regrouped Level 4`").alias("_Regrouped Level 4"), 
        col("_Site"), 
        col("_Sum_LCL").cast(DoubleType()).alias("_Sum_LCL"), 
        col("_Sum_LCLCFX"), 
        col("_Sum_USDAFX").cast(DoubleType()).alias("_Sum_USDAFX"), 
        col("_category"), 
        col("`_category code`").cast(StringType()).alias("_category code"), 
        col("_company"), 
        col("`_company code`").cast(DoubleType()).alias("_company code"), 
        col("`_cost center`").alias("_cost center"), 
        col("`_cost center number`").alias("_cost center number"), 
        col("_evp"), 
        col("`_gl account`").alias("_gl account"), 
        col("`_gl account number`").alias("_gl account number"), 
        col("_hlmc"), 
        col("`_material group`").alias("_material group"), 
        lit(None).cast(StringType()).alias("_material group description"), 
        col("_mc"), 
        col("`_pa account`").alias("_pa account"), 
        col("`_pa account desc`").alias("_pa account desc"), 
        col("`_planning sku`").cast(DoubleType()).alias("_planning sku"), 
        col("`_product description`").cast(StringType()).alias("_product description"), 
        col("_quarter"), 
        col("`_scenario name`").alias("_scenario name"), 
        col("`_scenario type`").cast(StringType()).alias("_scenario type"), 
        col("`_sub mc`").alias("_sub mc"), 
        col("_version").cast(StringType()).alias("_version"), 
        col("_year")
    )
