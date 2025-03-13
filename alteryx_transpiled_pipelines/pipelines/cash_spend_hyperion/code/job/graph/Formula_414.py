from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Formula_414(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("`pa account desc`").alias("pa account desc"), 
        col("`sub mc2`").alias("sub mc2"), 
        col("`acct level 5`").alias("acct level 5"), 
        col("`site detail`").alias("site detail"), 
        col("`scenario name`").alias("scenario name"), 
        col("`company code`").alias("company code"), 
        col("`sub mc`").alias("sub mc"), 
        col("site"), 
        col("`local currency`").alias("local currency"), 
        col("LCL"), 
        col("year"), 
        col("`category code`").alias("category code"), 
        col("`material group`").alias("material group"), 
        col("`acct level 3`").alias("acct level 3"), 
        col("quarter"), 
        col("company"), 
        col("version"), 
        col("`scenario type`").alias("scenario type"), 
        col("`planning sku`").alias("planning sku"), 
        col("site_nonsite"), 
        col("`cost center number`").alias("cost center number"), 
        col("`gl account`").alias("gl account"), 
        col("USDAFX"), 
        col("evp"), 
        col("`pa account`").alias("pa account"), 
        col("category"), 
        col("`acct level 4`").alias("acct level 4"), 
        col("LCLCFX"), 
        col("mc"), 
        col("hlmc"), 
        col("`_material group description`").alias("_material group description"), 
        col("`regrouped level 4`").alias("regrouped level 4"), 
        col("`cost center`").alias("cost center"), 
        col("`product description`").alias("product description"), 
        col("`gl account number`").alias("gl account number"), 
        lit("Non-Addressable Hyperion Category").cast(StringType()).alias("Exclusion Type")
    )
