from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Transpose_300_schemaTransform(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("`2026 Non-Opex Adjustment`").alias("2026 Non-Opex Adjustment"), 
        col("`2024 Non-Opex Adjustment`").alias("2024 Non-Opex Adjustment"), 
        col("`2027 Non-Opex Adjustment`").alias("2027 Non-Opex Adjustment"), 
        col("`Mega Category`").alias("Mega Category"), 
        col("`2025 Non-Opex Adjustment`").alias("2025 Non-Opex Adjustment")
    )
