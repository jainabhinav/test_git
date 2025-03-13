from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def MultiFieldFormula_75(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        *(
          [expr("(`Sum_Adjusted_Sum_USDAFX` * `%Split`)").alias("Sum_Adjusted_Sum_USDAFX"),            expr("(`Sum_Adjusted_Sum_LCLCFX` * `%Split`)").alias("Sum_Adjusted_Sum_LCLCFX"),            expr("(`Sum_Adjusted_Sum_LCL` * `%Split`)").alias("Sum_Adjusted_Sum_LCL"),            expr("(`Non-Opex Adjustments` * `%Split`)").alias("Non-Opex Adjustments"),            expr("(`Cash Spend` * `%Split`)").alias("Cash Spend"),            expr("(`GSS BAU Savings` * `%Split`)").alias("GSS BAU Savings"),            expr("(`GSS Incremental Low Savings` * `%Split`)").alias("GSS Incremental Low Savings"),            expr("(`GSS Incremental High Savings` * `%Split`)").alias("GSS Incremental High Savings"),            expr("(`AIN_Resource_Cost` * `%Split`)").alias("AIN_Resource_Cost"),            expr("(`AIN_Savings` * `%Split`)").alias("AIN_Savings"),            expr("(`AIN EW_Reduction` * `%Split`)").alias("AIN EW_Reduction")]
          + [
            col("`" + colName + "`")
            for colName in sorted(
              (
                set(in0.columns)
                - {"Sum_Adjusted_Sum_USDAFX",  "Sum_Adjusted_Sum_LCLCFX",  "Sum_Adjusted_Sum_LCL",                                      "Non-Opex Adjustments",  "Cash Spend",                                      "GSS BAU Savings",                                      "GSS Incremental Low Savings",                                      "GSS Incremental High Savings",                                      "AIN_Resource_Cost",  "AIN_Savings",                                      "AIN EW_Reduction"}
              )
            )
          ]
          + []
        )
    )
