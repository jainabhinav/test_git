from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def TextInput_38(spark: SparkSession) -> DataFrame:
    schemaFields = StructType([
        StructField("AIN Ext. Lab. Net Savings Stage", StringType(), True), StructField("GSS Enterprise Savings", StringType(), True), StructField("In-Scope", StringType(), True)
    ])\
        .fields
    readSchema = StructType([StructField(f.name, StringType(), True) for f in schemaFields])
    castExpressions = [col(f.name).cast(f.dataType) for f in schemaFields]
    df1 = spark.createDataFrame(
        [Row("Value Confirmation", "Baseline Initiative", "No"),          Row("Value Planning", "Develop Execution Strategy", "No"),          Row("Value Extraction", "Execute Initiative", "No"),          Row("Value Realization", "Finalize Initiative", "No"),          Row("Value Realization", "Implement Aligned Outcome", "No")],
        readSchema
    )

    return df1.select(castExpressions)
