from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Union_92_reformat_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("_Sum_LCL"), 
        col("_Sum_LCLCFX"), 
        col("_Sum_USDAFX"), 
        col("_company"), 
        col("`_company code`").alias("_company code"), 
        col("`_cost center`").alias("_cost center"), 
        col("`_cost center number`").alias("_cost center number"), 
        col("_evp"), 
        col("`_gl account`").cast(StringType()).alias("_gl account"), 
        col("`_gl account number`").cast(StringType()).alias("_gl account number"), 
        col("_hlmc"), 
        col("`_hyperion category`").alias("_hyperion category"), 
        col("`_hyperion category code`").alias("_hyperion category code"), 
        col("`_material group`").cast(StringType()).alias("_material group"), 
        col("_mc"), 
        col("`_pa account`").alias("_pa account"), 
        col("`_pa account desc`").alias("_pa account desc"), 
        col("`_planning sku`").cast(StringType()).alias("_planning sku"), 
        col("`_product description`").alias("_product description"), 
        col("_quarter"), 
        col("`_regrouped level 4`").alias("_regrouped level 4"), 
        col("`_scenario name`").alias("_scenario name"), 
        col("`_scenario type`").alias("_scenario type"), 
        col("_site"), 
        col("`_sub mc`").alias("_sub mc"), 
        col("_version"), 
        col("_year")
    )
