from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def MultiFieldFormula_19(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        *(
          [expr("CASE WHEN CAST(isnull(`Cash Spend`) AS BOOLEAN) THEN 0 ELSE `Cash Spend` END").alias("Cash Spend"),            expr(
               "CASE WHEN CAST(isnull(`Planning Account Description`) AS BOOLEAN) THEN 0 ELSE `Planning Account Description` END"
             )\
             .alias("Planning Account Description"),            expr(
               "CASE WHEN CAST(isnull(`AIN EW_Reduction (Realised)`) AS BOOLEAN) THEN 0 ELSE `AIN EW_Reduction (Realised)` END"
             )\
             .alias("AIN EW_Reduction (Realised)"),            expr("CASE WHEN CAST(isnull(`AIN_Savings`) AS BOOLEAN) THEN 0 ELSE `AIN_Savings` END").alias("AIN_Savings"),            expr("CASE WHEN CAST(isnull(`ASHB`) AS BOOLEAN) THEN 0 ELSE `ASHB` END").alias("ASHB"),            expr("CASE WHEN CAST(isnull(`Category`) AS BOOLEAN) THEN 0 ELSE `Category` END").alias("Category"),            expr("CASE WHEN CAST(isnull(`Non-Opex Adjustments`) AS BOOLEAN) THEN 0 ELSE `Non-Opex Adjustments` END")\
             .alias("Non-Opex Adjustments"),            expr("CASE WHEN CAST(isnull(`Planning Account Actual`) AS BOOLEAN) THEN 0 ELSE `Planning Account Actual` END")\
             .alias("Planning Account Actual"),            expr(
               "CASE WHEN CAST(isnull(`Planning Account Description Actual`) AS BOOLEAN) THEN 0 ELSE `Planning Account Description Actual` END"
             )\
             .alias("Planning Account Description Actual"),            expr(
               "CASE WHEN CAST(isnull(`AIN_Resource_Cost (Realised)`) AS BOOLEAN) THEN 0 ELSE `AIN_Resource_Cost (Realised)` END"
             )\
             .alias("AIN_Resource_Cost (Realised)"),            expr("CASE WHEN CAST(isnull(`scenario name`) AS BOOLEAN) THEN 0 ELSE `scenario name` END")\
             .alias("scenario name"),            expr("CASE WHEN CAST(isnull(`GSS Enterprise Savings`) AS BOOLEAN) THEN 0 ELSE `GSS Enterprise Savings` END")\
             .alias("GSS Enterprise Savings"),            expr("CASE WHEN CAST(isnull(`sub mc`) AS BOOLEAN) THEN 0 ELSE `sub mc` END").alias("sub mc"),            expr("CASE WHEN CAST(isnull(`site`) AS BOOLEAN) THEN 0 ELSE `site` END").alias("site"),            expr("CASE WHEN CAST(isnull(`Sum_Adjusted_Sum_LCL`) AS BOOLEAN) THEN 0 ELSE `Sum_Adjusted_Sum_LCL` END")\
             .alias("Sum_Adjusted_Sum_LCL"),            expr(
               "CASE WHEN CAST(isnull(`AIN Ext. Lab. Net Savings Stage`) AS BOOLEAN) THEN 0 ELSE `AIN Ext. Lab. Net Savings Stage` END"
             )\
             .alias("AIN Ext. Lab. Net Savings Stage"),            expr("CASE WHEN CAST(isnull(`AIN_Resource_Cost`) AS BOOLEAN) THEN 0 ELSE `AIN_Resource_Cost` END")\
             .alias("AIN_Resource_Cost"),            expr("CASE WHEN CAST(isnull(`year`) AS BOOLEAN) THEN 0 ELSE `year` END").alias("year"),            expr("CASE WHEN CAST(isnull(`material group`) AS BOOLEAN) THEN 0 ELSE `material group` END")\
             .alias("material group"),            expr(
               "CASE WHEN CAST(isnull(`GSS BAU Savings (Realised)`) AS BOOLEAN) THEN 0 ELSE `GSS BAU Savings (Realised)` END"
             )\
             .alias("GSS BAU Savings (Realised)"),            expr("CASE WHEN CAST(isnull(`AIN EW_Reduction`) AS BOOLEAN) THEN 0 ELSE `AIN EW_Reduction` END")\
             .alias("AIN EW_Reduction"),            expr(
               "CASE WHEN CAST(isnull(`GSS Incremental High Savings (Realised)`) AS BOOLEAN) THEN 0 ELSE `GSS Incremental High Savings (Realised)` END"
             )\
             .alias("GSS Incremental High Savings (Realised)"),            expr("CASE WHEN CAST(isnull(`OSE Labor`) AS BOOLEAN) THEN 0 ELSE `OSE Labor` END").alias("OSE Labor"),            expr("CASE WHEN CAST(isnull(`quarter`) AS BOOLEAN) THEN 0 ELSE `quarter` END").alias("quarter"),            expr("CASE WHEN CAST(isnull(`Company Code`) AS BOOLEAN) THEN 0 ELSE `Company Code` END")\
             .alias("Company Code"),            expr("CASE WHEN CAST(isnull(`Sum_Adjusted_Sum_LCLCFX`) AS BOOLEAN) THEN 0 ELSE `Sum_Adjusted_Sum_LCLCFX` END")\
             .alias("Sum_Adjusted_Sum_LCLCFX"),            expr("CASE WHEN CAST(isnull(`AIN_Savings (Realised)`) AS BOOLEAN) THEN 0 ELSE `AIN_Savings (Realised)` END")\
             .alias("AIN_Savings (Realised)"),            expr("CASE WHEN CAST(isnull(`Hyperion Category`) AS BOOLEAN) THEN 0 ELSE `Hyperion Category` END")\
             .alias("Hyperion Category"),            expr("CASE WHEN CAST(isnull(`version`) AS BOOLEAN) THEN 0 ELSE `version` END").alias("version"),            expr("CASE WHEN CAST(isnull(`scenario type`) AS BOOLEAN) THEN 0 ELSE `scenario type` END")\
             .alias("scenario type"),            expr("CASE WHEN CAST(isnull(`Hyperion Category Code`) AS BOOLEAN) THEN 0 ELSE `Hyperion Category Code` END")\
             .alias("Hyperion Category Code"),            expr("CASE WHEN CAST(isnull(`planning sku`) AS BOOLEAN) THEN 0 ELSE `planning sku` END")\
             .alias("planning sku"),            expr("CASE WHEN CAST(isnull(`Planning Account`) AS BOOLEAN) THEN 0 ELSE `Planning Account` END")\
             .alias("Planning Account"),            expr("CASE WHEN CAST(isnull(`cost center number`) AS BOOLEAN) THEN 0 ELSE `cost center number` END")\
             .alias("cost center number"),            expr("CASE WHEN CAST(isnull(`GSS BAU Savings`) AS BOOLEAN) THEN 0 ELSE `GSS BAU Savings` END")\
             .alias("GSS BAU Savings"),            expr(
               "CASE WHEN CAST(isnull(`GSS Incremental High Savings`) AS BOOLEAN) THEN 0 ELSE `GSS Incremental High Savings` END"
             )\
             .alias("GSS Incremental High Savings"),            expr("CASE WHEN CAST(isnull(`gl account`) AS BOOLEAN) THEN 0 ELSE `gl account` END").alias("gl account"),            expr("CASE WHEN CAST(isnull(`evp`) AS BOOLEAN) THEN 0 ELSE `evp` END").alias("evp"),            expr(
               "CASE WHEN CAST(isnull(`material group description`) AS BOOLEAN) THEN 0 ELSE `material group description` END"
             )\
             .alias("material group description"),            expr("CASE WHEN CAST(isnull(`mc`) AS BOOLEAN) THEN 0 ELSE `mc` END").alias("mc"),            expr("CASE WHEN CAST(isnull(`In-Scope`) AS BOOLEAN) THEN 0 ELSE `In-Scope` END").alias("In-Scope"),            expr("CASE WHEN CAST(isnull(`hlmc`) AS BOOLEAN) THEN 0 ELSE `hlmc` END").alias("hlmc"),            expr("CASE WHEN CAST(isnull(`regrouped level 4`) AS BOOLEAN) THEN 0 ELSE `regrouped level 4` END")\
             .alias("regrouped level 4"),            expr(
               "CASE WHEN CAST(isnull(`GSS Incremental Low Savings (Realised)`) AS BOOLEAN) THEN 0 ELSE `GSS Incremental Low Savings (Realised)` END"
             )\
             .alias("GSS Incremental Low Savings (Realised)"),            expr("CASE WHEN CAST(isnull(`CTS v3`) AS BOOLEAN) THEN 0 ELSE `CTS v3` END").alias("CTS v3"),            expr("CASE WHEN CAST(isnull(`cost center`) AS BOOLEAN) THEN 0 ELSE `cost center` END").alias("cost center"),            expr("CASE WHEN CAST(isnull(`product description`) AS BOOLEAN) THEN 0 ELSE `product description` END")\
             .alias("product description"),            expr(
               "CASE WHEN CAST(isnull(`GSS Incremental Low Savings`) AS BOOLEAN) THEN 0 ELSE `GSS Incremental Low Savings` END"
             )\
             .alias("GSS Incremental Low Savings"),            expr("CASE WHEN CAST(isnull(`Sum_Adjusted_Sum_USDAFX`) AS BOOLEAN) THEN 0 ELSE `Sum_Adjusted_Sum_USDAFX` END")\
             .alias("Sum_Adjusted_Sum_USDAFX"),            expr("CASE WHEN CAST(isnull(`Company Description`) AS BOOLEAN) THEN 0 ELSE `Company Description` END")\
             .alias("Company Description"),            expr("CASE WHEN CAST(isnull(`gl account number`) AS BOOLEAN) THEN 0 ELSE `gl account number` END")\
             .alias("gl account number")]
          + [
            col("`" + colName + "`")
            for colName in sorted(
              (
                set(in0.columns)
                - {"Cash Spend",  "Planning Account Description",  "AIN EW_Reduction (Realised)",                                      "AIN_Savings",  "ASHB",  "Category",                                      "Non-Opex Adjustments",                                      "Planning Account Actual",                                      "Planning Account Description Actual",                                      "AIN_Resource_Cost (Realised)",  "scenario name",                                      "GSS Enterprise Savings",  "sub mc",  "site",                                      "Sum_Adjusted_Sum_LCL",                                      "AIN Ext. Lab. Net Savings Stage",                                      "AIN_Resource_Cost",  "year",  "material group",                                      "GSS BAU Savings (Realised)",                                      "AIN EW_Reduction",                                      "GSS Incremental High Savings (Realised)",                                      "OSE Labor",  "quarter",  "Company Code",                                      "Sum_Adjusted_Sum_LCLCFX",                                      "AIN_Savings (Realised)",  "Hyperion Category",                                      "version",  "scenario type",                                      "Hyperion Category Code",  "planning sku",                                      "Planning Account",  "cost center number",                                      "GSS BAU Savings",                                      "GSS Incremental High Savings",  "gl account",                                      "evp",  "material group description",  "mc",                                      "In-Scope",  "hlmc",  "regrouped level 4",                                      "GSS Incremental Low Savings (Realised)",                                      "CTS v3",  "cost center",  "product description",                                      "GSS Incremental Low Savings",                                      "Sum_Adjusted_Sum_USDAFX",                                      "Company Description",  "gl account number"}
              )
            )
          ]
          + []
        )
    )
