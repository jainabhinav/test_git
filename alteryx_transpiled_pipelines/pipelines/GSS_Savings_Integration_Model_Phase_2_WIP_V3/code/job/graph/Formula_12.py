from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Formula_12(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("`Cash Spend`").alias("Cash Spend"), 
        col("`Planning Account Description`").alias("Planning Account Description"), 
        col("ASHB"), 
        col("Category"), 
        col("`Non-Opex Adjustments`").alias("Non-Opex Adjustments"), 
        col("`Planning Account Actual`").alias("Planning Account Actual"), 
        col("`Planning Account Description Actual`").alias("Planning Account Description Actual"), 
        col("`scenario name`").alias("scenario name"), 
        col("`sub mc`").alias("sub mc"), 
        col("site"), 
        col("Sum_Adjusted_Sum_LCL"), 
        col("year"), 
        col("`material group`").alias("material group"), 
        col("`OSE Labor`").alias("OSE Labor"), 
        col("quarter"), 
        col("`Company Code`").alias("Company Code"), 
        col("Sum_Adjusted_Sum_LCLCFX"), 
        col("`Hyperion Category`").alias("Hyperion Category"), 
        col("version"), 
        col("`scenario type`").alias("scenario type"), 
        col("`Hyperion Category Code`").alias("Hyperion Category Code"), 
        col("`planning sku`").alias("planning sku"), 
        col("`Planning Account`").alias("Planning Account"), 
        col("`cost center number`").alias("cost center number"), 
        col("`gl account`").alias("gl account"), 
        col("evp"), 
        col("`material group description`").alias("material group description"), 
        col("mc"), 
        col("hlmc"), 
        col("`regrouped level 4`").alias("regrouped level 4"), 
        col("`CTS v3`").alias("CTS v3"), 
        col("`cost center`").alias("cost center"), 
        col("RecordID"), 
        col("`product description`").alias("product description"), 
        col("Sum_Adjusted_Sum_USDAFX"), 
        col("`Company Description`").alias("Company Description"), 
        col("`gl account number`").alias("gl account number"), 
        ((col("`scenario name`") + col("evp")) + col("Category")).cast(StringType()).alias("Split Divide"), 
        (
          (
            (
              (
                (
                  (
                    (
                      (
                        (
                          (
                            (
                              (
                                (
                                  (
                                    (
                                      (
                                        (
                                          (
                                            (
                                              (
                                                (
                                                  (
                                                    (
                                                      (
                                                        (
                                                          (
                                                            (
                                                              col("year")
                                                              + col(
                                                                "`cost center number`"
                                                              )
                                                            )
                                                            + col(
                                                              "hlmc"
                                                            )
                                                          )
                                                          + col(
                                                            "mc"
                                                          )
                                                        )
                                                        + col(
                                                          "`cost center`"
                                                        )
                                                      )
                                                      + col(
                                                        "quarter"
                                                      )
                                                    )
                                                    + col("site")
                                                  )
                                                  + col(
                                                    "`Planning Account`"
                                                  )
                                                )
                                                + col(
                                                  "`regrouped level 4`"
                                                )
                                              )
                                              + col("`Company Code`")
                                            )
                                            + col("`Company Description`")
                                          )
                                          + col(
                                            "`Planning Account Description`"
                                          )
                                        )
                                        + col("`Hyperion Category`")
                                      )
                                      + col("`Hyperion Category Code`")
                                    )
                                    + col("`CTS v3`")
                                  )
                                  + col("`Planning Account Actual`")
                                )
                                + col("`Planning Account Description Actual`")
                              )
                              + col("`scenario type`")
                            )
                            + col("version")
                          )
                          + col("`gl account`")
                        )
                        + col("`gl account number`")
                      )
                      + col("`sub mc`")
                    )
                    + col("`material group`")
                  )
                  + col("`material group description`")
                )
                + col("`planning sku`")
              )
              + col("`product description`")
            )
            + col("`OSE Labor`")
          )
          + col("ASHB")
        )\
          .cast(StringType())\
          .alias("Group"), 
        (
          (
            (
              (
                (
                  (
                    (
                      (
                        (
                          (
                            (
                              (
                                (
                                  (
                                    (
                                      (
                                        (
                                          (
                                            (
                                              col("year")
                                              + col(
                                                "`cost center number`"
                                              )
                                            )
                                            + col("hlmc")
                                          )
                                          + col("mc")
                                        )
                                        + col("`cost center`")
                                      )
                                      + col("quarter")
                                    )
                                    + col("site")
                                  )
                                  + col("`Planning Account`")
                                )
                                + col("`regrouped level 4`")
                              )
                              + col("`Company Code`")
                            )
                            + col("`Company Description`")
                          )
                          + col("`Planning Account Description`")
                        )
                        + col("`Hyperion Category`")
                      )
                      + col("`Hyperion Category Code`")
                    )
                    + col("`CTS v3`")
                  )
                  + col("`Planning Account Actual`")
                )
                + col("`Planning Account Description Actual`")
              )
              + col("`scenario name`")
            )
            + col("evp")
          )
          + col("Category")
        )\
          .cast(StringType())\
          .alias("Group 2")
    )
