from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Union_74_reformat_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("_Category").cast(StringType()).alias("_Category"), 
        col("`_Category Old`").alias("_Category Old"), 
        col("`_Country Old`").alias("_Country Old"), 
        col("`_Country Short`").alias("_Country Short"), 
        col("_Country_match_flag"), 
        col("`_Exclusion Type`").alias("_Exclusion Type"), 
        col("_GL"), 
        col("`_GL Account Description Updated`").alias("_GL Account Description Updated"), 
        col("`_Gl Account`").cast(StringType()).alias("_Gl Account"), 
        col(
            "`_Gl Account (# only)`"
          )\
          .cast(StringType())\
          .alias(
          "_Gl Account (# only)"
        ), 
        col("`_Mega Category Old`").alias("_Mega Category Old"), 
        col("_ParentVendor_match_flag"), 
        col("`_Planning Account`").cast(StringType()).alias("_Planning Account"), 
        col("`_Regrouped Level 4`").cast(StringType()).alias("_Regrouped Level 4"), 
        col("`_SUB GROUP`").alias("_SUB GROUP"), 
        col("_Site"), 
        col("`_Sum of Commitment Amount`").alias("_Sum of Commitment Amount"), 
        col("`_company code name`").alias("_company code name"), 
        col("`_company code number`").cast(DoubleType()).alias("_company code number"), 
        col("`_fit supplier diversity flag`").alias("_fit supplier diversity flag"), 
        col("`_hyperion cc description`").alias("_hyperion cc description"), 
        col("`_hyperion cc_function_descr`").alias("_hyperion cc_function_descr"), 
        col("`_hyperion cc_function_mapping`").alias("_hyperion cc_function_mapping"), 
        col("`_hyperion evp`").alias("_hyperion evp"), 
        col("`_hyperion function description`").alias("_hyperion function description"), 
        col("`_hyperion function level 2`").alias("_hyperion function level 2"), 
        col("`_hyperion function level 3`").alias("_hyperion function level 3"), 
        col("`_hyperion function level 4`").alias("_hyperion function level 4"), 
        col("`_hyperion hlmc`").alias("_hyperion hlmc"), 
        col("`_hyperion mc`").alias("_hyperion mc"), 
        col("`_hyperion sub mc`").alias("_hyperion sub mc"), 
        col("`_hyperion sub mc2`").alias("_hyperion sub mc2"), 
        col("`_hyperion sub mc3`").alias("_hyperion sub mc3"), 
        col("`_invoice linenumber`").alias("_invoice linenumber"), 
        col("`_po description`").alias("_po description"), 
        col("`_po id`").alias("_po id"), 
        col("`_po line number`").alias("_po line number"), 
        col("`_sap/concur source`").alias("_sap/concur source"), 
        col("`_source_accounting date`").alias("_source_accounting date"), 
        col("`_source_accounting date - month`").alias("_source_accounting date - month"), 
        col("`_source_accounting date - quarter`").alias("_source_accounting date - quarter"), 
        col("`_source_accounting date - year`").alias("_source_accounting date - year"), 
        col("`_source_amgen region/country - country`").alias("_source_amgen region/country - country"), 
        col("`_source_amgen region/country - sap company code`")\
          .alias("_source_amgen region/country - sap company code"), 
        col("`_source_amgen region/country - world region`").alias("_source_amgen region/country - world region"), 
        col("`_source_amgen region/country - world sub-region`")\
          .alias("_source_amgen region/country - world sub-region"), 
        col("`_source_amgen region/country - world sub-region Old`")\
          .alias("_source_amgen region/country - world sub-region Old"), 
        col("`_source_amgen region/country - world sub-region_temp`")\
          .alias("_source_amgen region/country - world sub-region_temp"), 
        col("`_source_cost center`").alias("_source_cost center"), 
        col("`_source_cost center number`").alias("_source_cost center number"), 
        col("`_source_gl number`").alias("_source_gl number"), 
        col("`_source_invoice number`").alias("_source_invoice number"), 
        col("`_source_invoice quantity`").alias("_source_invoice quantity"), 
        col("`_source_invoice spend (actual fx rate)`").alias("_source_invoice spend (actual fx rate)"), 
        col("`_source_invoice spend (budget fx rate)`").alias("_source_invoice spend (budget fx rate)"), 
        col("`_source_invoice spend (original currency)`").alias("_source_invoice spend (original currency)"), 
        col("`_source_mg code`").alias("_source_mg code"), 
        col("`_source_mg code description`").alias("_source_mg code description"), 
        col("`_source_procurement channel code - procurement channel code`")\
          .alias("_source_procurement channel code - procurement channel code"), 
        col("`_source_requester - user/buyer name responsible for spend`")\
          .alias("_source_requester - user/buyer name responsible for spend"), 
        col("`_source_sap_vendor description`").alias("_source_sap_vendor description"), 
        col("`_source_vendor - cc1 - indicator`").alias("_source_vendor - cc1 - indicator"), 
        col("`_source_vendor - cc1 - type`").alias("_source_vendor - cc1 - type"), 
        col("`_source_vendor - cc2 - women owned small bus`").alias("_source_vendor - cc2 - women owned small bus"), 
        col("`_source_vendor - cc3 - svc disabled vet owned sb`")\
          .alias("_source_vendor - cc3 - svc disabled vet owned sb"), 
        col("`_source_vendor - cc3 - veteran owned small bus`").alias("_source_vendor - cc3 - veteran owned small bus"), 
        col("`_source_vendor - cc4 - hubzone small business`").alias("_source_vendor - cc4 - hubzone small business"), 
        col("`_source_vendor - cc4 - obs-foreign supplier`").alias("_source_vendor - cc4 - obs-foreign supplier"), 
        col("`_source_vendor - cc5 - obs-large business`").alias("_source_vendor - cc5 - obs-large business"), 
        col("`_source_vendor - cc5 - small business`").alias("_source_vendor - cc5 - small business"), 
        col("`_source_vendor - vendor (enriched)`").alias("_source_vendor - vendor (enriched)"), 
        col("`_source_vendor - vendor global ultimate parent (enriched)`")\
          .alias("_source_vendor - vendor global ultimate parent (enriched)"), 
        col("`_source_vendor - vendor global ultimate parent (enriched) old`")\
          .alias("_source_vendor - vendor global ultimate parent (enriched) old"), 
        col("`_source_vendor - vendor global ultimate parent (enriched) temp`")\
          .alias("_source_vendor - vendor global ultimate parent (enriched) temp"), 
        col("_source_vendor_number"), 
        col("`_source_wbs - accounting treatment`").alias("_source_wbs - accounting treatment"), 
        col("`_source_wbs - description`").alias("_source_wbs - description"), 
        col("`_source_wbs - product name`").alias("_source_wbs - product name"), 
        col("`_tableau display category`").alias("_tableau display category"), 
        col("`_tableau display material group`").alias("_tableau display material group"), 
        col("`_tableau display mega category`").alias("_tableau display mega category"), 
        col("`_tableau display meta category`").alias("_tableau display meta category"), 
        col("`_vendor country`").alias("_vendor country"), 
        col("`_vendor country code`").alias("_vendor country code")
    )
