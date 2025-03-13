from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Join_340_left_UnionLeftOuter(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(in1.alias("in1"), (col("in0.`source_cost center number`") == col("in1.`CC Lookup`")), "leftouter")\
        .select(col("in0.`Mega Category Old`").alias("Mega Category Old"), col("in0.`company code number`").alias("company code number"), col("in0.`source_cost center`").alias("source_cost center"), col("in0.`source_accounting date - quarter`").alias("source_accounting date - quarter"), col("in0.`source_mg code description`").alias("source_mg code description"), col("in0.`Sum_Spend ($)`").alias("Sum_Spend ($)"), col("in0.GL").alias("GL"), col("in1.`CC Lookup`").alias("CC Lookup"), col("in0.Site").alias("Site"), col("in0.`tableau display category`").alias("tableau display category"), col("in0.`hyperion sub mc`").alias("hyperion sub mc"), col("in0.`tableau display mega category`").alias("tableau display mega category"), col("in0.`Category Old`").alias("Category Old"), col("in0.`hyperion evp`").alias("hyperion evp"), col("in0.`GL Account Description Updated`").alias("GL Account Description Updated"), col("in0.`source_cost center number`").alias("source_cost center number"), col("in0.`CTS v3`").alias("CTS v3"), col("in0.`source_mg code`").alias("source_mg code"), col("in0.`GL Number`").alias("GL Number"), col("in0.`source_accounting date - year`").alias("source_accounting date - year"), col("in1.`CTS v3 CC`").alias("CTS v3 CC"), col("in0.`hyperion hlmc`").alias("hyperion hlmc"), col("in0.`company code name`").alias("company code name"), col("in0.`hyperion mc`").alias("hyperion mc"))
