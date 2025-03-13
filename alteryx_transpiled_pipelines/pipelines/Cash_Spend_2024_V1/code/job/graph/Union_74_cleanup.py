from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Union_74_cleanup(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("`_Exclusion Type`").alias("Exclusion Type"), 
        col("`_fit supplier diversity flag`").alias("fit supplier diversity flag"), 
        col("`_po line number`").alias("po line number"), 
        col("`_source_procurement channel code - procurement channel code`")\
          .alias("source_procurement channel code - procurement channel code"), 
        col("`_Mega Category Old`").alias("Mega Category Old"), 
        col("`_company code number`").alias("company code number"), 
        col("`_source_cost center`").alias("source_cost center"), 
        col("`_hyperion sub mc3`").alias("hyperion sub mc3"), 
        col("`_hyperion cc_function_descr`").alias("hyperion cc_function_descr"), 
        col("`_hyperion function description`").alias("hyperion function description"), 
        col("`_source_accounting date - quarter`").alias("source_accounting date - quarter"), 
        col("`_source_amgen region/country - world sub-region_temp`")\
          .alias("source_amgen region/country - world sub-region_temp"), 
        col("`_source_mg code description`").alias("source_mg code description"), 
        col("`_source_wbs - accounting treatment`").alias("source_wbs - accounting treatment"), 
        col("`_source_amgen region/country - world sub-region`").alias("source_amgen region/country - world sub-region"), 
        col("`_source_sap_vendor description`").alias("source_sap_vendor description"), 
        col("_GL").alias("GL"), 
        col("`_Country Short`").alias("Country Short"), 
        col("`_tableau display material group`").alias("tableau display material group"), 
        col("_Site").alias("Site"), 
        col("`_Sum of Commitment Amount`").alias("Sum of Commitment Amount"), 
        col("`_po id`").alias("po id"), 
        col("`_source_vendor - vendor global ultimate parent (enriched)`")\
          .alias("source_vendor - vendor global ultimate parent (enriched)"), 
        col("`_source_vendor - cc3 - veteran owned small bus`").alias("source_vendor - cc3 - veteran owned small bus"), 
        col("`_source_accounting date - month`").alias("source_accounting date - month"), 
        col("`_source_wbs - description`").alias("source_wbs - description"), 
        col("`_source_vendor - cc1 - type`").alias("source_vendor - cc1 - type"), 
        col("`_source_invoice spend (actual fx rate)`").alias("source_invoice spend (actual fx rate)"), 
        col("`_hyperion function level 4`").alias("hyperion function level 4"), 
        col("`_vendor country`").alias("vendor country"), 
        col("_source_vendor_number").alias("source_vendor_number"), 
        col("`_source_invoice number`").alias("source_invoice number"), 
        col("`_source_vendor - cc3 - svc disabled vet owned sb`")\
          .alias("source_vendor - cc3 - svc disabled vet owned sb"), 
        col("`_tableau display category`").alias("tableau display category"), 
        col("`_source_vendor - cc4 - obs-foreign supplier`").alias("source_vendor - cc4 - obs-foreign supplier"), 
        col("`_hyperion sub mc2`").alias("hyperion sub mc2"), 
        col("`_hyperion sub mc`").alias("hyperion sub mc"), 
        col("`_source_vendor - cc2 - women owned small bus`").alias("source_vendor - cc2 - women owned small bus"), 
        col("`_tableau display mega category`").alias("tableau display mega category"), 
        col("`_source_vendor - vendor global ultimate parent (enriched) old`")\
          .alias("source_vendor - vendor global ultimate parent (enriched) old"), 
        col("`_Category Old`").alias("Category Old"), 
        col("`_source_vendor - cc5 - small business`").alias("source_vendor - cc5 - small business"), 
        col("_ParentVendor_match_flag").alias("ParentVendor_match_flag"), 
        col("`_vendor country code`").alias("vendor country code"), 
        col("`_source_vendor - vendor (enriched)`").alias("source_vendor - vendor (enriched)"), 
        col("`_hyperion function level 3`").alias("hyperion function level 3"), 
        col("`_hyperion evp`").alias("hyperion evp"), 
        col("`_source_invoice spend (budget fx rate)`").alias("source_invoice spend (budget fx rate)"), 
        col("`_source_requester - user/buyer name responsible for spend`")\
          .alias("source_requester - user/buyer name responsible for spend"), 
        col("`_Country Old`").alias("Country Old"), 
        col("`_SUB GROUP`").alias("SUB GROUP"), 
        col("`_source_vendor - cc4 - hubzone small business`").alias("source_vendor - cc4 - hubzone small business"), 
        col("`_sap/concur source`").alias("sap/concur source"), 
        col("`_source_amgen region/country - world region`").alias("source_amgen region/country - world region"), 
        col("`_Gl Account`").alias("Gl Account"), 
        col("`_GL Account Description Updated`").alias("GL Account Description Updated"), 
        col("`_source_invoice spend (original currency)`").alias("source_invoice spend (original currency)"), 
        col("`_source_cost center number`").alias("source_cost center number"), 
        col("`_source_vendor - cc1 - indicator`").alias("source_vendor - cc1 - indicator"), 
        col("`_hyperion cc description`").alias("hyperion cc description"), 
        col("_Category").alias("Category"), 
        col("`_invoice linenumber`").alias("invoice linenumber"), 
        col("`_source_amgen region/country - sap company code`").alias("source_amgen region/country - sap company code"), 
        col("`_hyperion function level 2`").alias("hyperion function level 2"), 
        col("`_source_amgen region/country - world sub-region Old`")\
          .alias("source_amgen region/country - world sub-region Old"), 
        col("`_hyperion cc_function_mapping`").alias("hyperion cc_function_mapping"), 
        col("`_source_vendor - vendor global ultimate parent (enriched) temp`")\
          .alias("source_vendor - vendor global ultimate parent (enriched) temp"), 
        col("`_source_gl number`").alias("source_gl number"), 
        col("`_source_wbs - product name`").alias("source_wbs - product name"), 
        col("`_po description`").alias("po description"), 
        col("`_source_mg code`").alias("source_mg code"), 
        col("`_Regrouped Level 4`").alias("Regrouped Level 4"), 
        col("`_tableau display meta category`").alias("tableau display meta category"), 
        col(
            "`_Gl Account (# only)`"
          )\
          .alias(
          "Gl Account (# only)"
        ), 
        col("`_source_accounting date - year`").alias("source_accounting date - year"), 
        col("_Country_match_flag").alias("Country_match_flag"), 
        col("`_source_amgen region/country - country`").alias("source_amgen region/country - country"), 
        col("`_Planning Account`").alias("Planning Account"), 
        col("`_hyperion hlmc`").alias("hyperion hlmc"), 
        col("`_company code name`").alias("company code name"), 
        col("`_source_invoice quantity`").alias("source_invoice quantity"), 
        col("`_hyperion mc`").alias("hyperion mc"), 
        col("`_source_vendor - cc5 - obs-large business`").alias("source_vendor - cc5 - obs-large business"), 
        col("`_source_accounting date`").alias("source_accounting date")
    )
