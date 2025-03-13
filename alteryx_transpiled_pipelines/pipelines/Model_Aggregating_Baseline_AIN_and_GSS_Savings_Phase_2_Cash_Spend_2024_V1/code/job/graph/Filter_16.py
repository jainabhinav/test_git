from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Filter_16(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter(
        (
          ((((((((((col("evp") == col("Right_evp")).cast(BooleanType()) & (col("`scenario name`") == col("`Right_scenario name`")).cast(BooleanType())).cast(BooleanType()) & (col("year") == col("Right_year")).cast(BooleanType())).cast(BooleanType()) & (col("Category") == col("Right_Category")).cast(BooleanType())).cast(BooleanType()) & (col("`cost center number`") == col("`Right_cost center number`")).cast(BooleanType())).cast(BooleanType()) & (col("`cost center`") == col("`Right_cost center`")).cast(BooleanType())).cast(BooleanType()) & (col("hlmc") == col("Right_hlmc")).cast(BooleanType())).cast(BooleanType()) & (col("mc") == col("Right_mc")).cast(BooleanType())).cast(BooleanType()) & (col("site") == col("Right_site")).cast(BooleanType())).cast(BooleanType()) & (col("quarter") == col("Right_quarter")).cast(BooleanType())).cast(BooleanType())
          & (col("`Cash Spend`") == col("`Right_Cash Spend`")).cast(BooleanType())
        )
    )
