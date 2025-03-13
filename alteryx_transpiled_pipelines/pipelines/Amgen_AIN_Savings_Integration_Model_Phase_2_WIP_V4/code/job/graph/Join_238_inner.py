from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Join_238_inner(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(
          in1.alias("in1"),
          (
            (((((col("in0.`tableau display mega category`") == col("in1.Category")) & (col("in0.Function") == col("in1.`CTS v3`"))) & (col("in0.EVP") == col("in1.evp"))) & (col("in0.HLMC") == col("in1.hlmc"))) & (col("in0.`Cost Center`") == col("in1.`cost center number`")))
            & (col("in0.Year") == col("in1.year"))
          ),
          "inner"
        )\
        .select(col("in0.Cumulative/Incremental").alias("Cumulative/Incremental"), col("in1.`pa account desc`").alias("pa account desc"), col("in1.`Split Divide`").alias("Split Divide"), col("in0.AIN_Savings").alias("AIN_Savings"), col("in1.`OSE Labor`").alias("OSE Labor"), col("in1.`scenario name`").alias("scenario name"), col("in1.`company code`").alias("company code"), col("in1.`sub mc`").alias("sub mc"), col("in1.site").alias("site"), col("in1.`cost center`").alias("Right_cost center"), col("in0.Year").alias("Year"), col("in1.`material group`").alias("material group"), col("in1.ASHB").alias("ASHB"), col("in1.quarter").alias("quarter"), col("in0.`tableau display mega category`").alias("tableau display mega category"), col("in1.`hyperion category code`").alias("hyperion category code"), col("in1.company").alias("company"), col("in1.version").alias("version"), col("in1.`scenario type`").alias("scenario type"), col("in1.`planning sku`").alias("planning sku"), col("in1.hlmc").alias("Right_hlmc"), col("in1.evp").alias("Right_evp"), col("in0.EW_Reduction").alias("EW_Reduction"), col("in1.`cost center number`").alias("cost center number"), col("in0.Function").alias("Function"), col("in1.`gl account`").alias("gl account"), col("in1.`hyperion category`").alias("hyperion category"), col("in0.EVP").alias("EVP"), col("in1.`CTS v3`").alias("CTS v3"), col("in1.`pa account`").alias("pa account"), col("in1.Category").alias("Category"), col("in1.`material group description`").alias("material group description"), col("in1.mc").alias("mc"), col("in0.HLMC").alias("HLMC"), col("in0.AIN_Resource_Cost").alias("AIN_Resource_Cost"), col("in1.`regrouped level 4`").alias("regrouped level 4"), col("in0.`Cost Center`").alias("Cost Center"), col("in1.`product description`").alias("product description"), col("in1.year").alias("Right_year"), col("in1.Sum_Sum_Adjusted_Sum_LCLCFX").alias("Sum_Sum_Adjusted_Sum_LCLCFX"), col("in1.`gl account number`").alias("gl account number"))
