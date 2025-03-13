from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AlteryxSelect_184(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(col("EPM_ESS_Function_ASHB_Attr"), col("`CC Lookup`").cast(StringType()).alias("CC Lookup"))
