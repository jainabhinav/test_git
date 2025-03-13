from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Join_375_inner(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(
          in1.alias("in1"),
          (
            col("in0.GL")
            == col(
              "in1.`Gl Account (# only)`"
            )
          ),
          "inner"
        )\
        .select(
        col("in0.`fit supplier diversity flag`").alias("fit supplier diversity flag"), 
        col("in0.`po line number`").alias("po line number"), 
        col("in0.`source_procurement channel code - procurement channel code`")\
          .alias("source_procurement channel code - procurement channel code"), 
        col("in0.`Mega Category Old`").alias("Mega Category Old"), 
        col("in0.`company code number`").alias("company code number"), 
        col("in0.`source_cost center`").alias("source_cost center"), 
        col("in0.Sub_group_flag").alias("Sub_group_flag"), 
        col("in0._version").alias("_version"), 
        col("in0.`hyperion sub mc3`").alias("hyperion sub mc3"), 
        col("in0.`hyperion cc_function_descr`").alias("hyperion cc_function_descr"), 
        col("in0.`hyperion function description`").alias("hyperion function description"), 
        col("in0.`source_accounting date - quarter`").alias("source_accounting date - quarter"), 
        col("in0.`_planning sku`").alias("_planning sku"), 
        col("in0.`source_amgen region/country - world sub-region_temp`")\
          .alias("source_amgen region/country - world sub-region_temp"), 
        col("in0.`_Regrouped Level 4`").alias("_Regrouped Level 4"), 
        col("in0._GL").alias("_GL"), 
        col("in0.`source_mg code description`").alias("source_mg code description"), 
        col("in0.`source_wbs - accounting treatment`").alias("source_wbs - accounting treatment"), 
        col("in0.`source_amgen region/country - world sub-region`")\
          .alias("source_amgen region/country - world sub-region"), 
        col("in0.`source_sap_vendor description`").alias("source_sap_vendor description"), 
        col("in0.GL").alias("GL"), 
        col("in0.`Country Short`").alias("Country Short"), 
        col("in0._Sum_USDAFX").alias("_Sum_USDAFX"), 
        col("in0.`tableau display material group`").alias("tableau display material group"), 
        col("in0.Site").alias("Site"), 
        col("in0.`Sum of Commitment Amount`").alias("Sum of Commitment Amount"), 
        col("in0.`po id`").alias("po id"), 
        col("in0.`_scenario type`").alias("_scenario type"), 
        col("in0.`source_vendor - vendor global ultimate parent (enriched)`")\
          .alias("source_vendor - vendor global ultimate parent (enriched)"), 
        col("in0.`source_vendor - cc3 - veteran owned small bus`")\
          .alias("source_vendor - cc3 - veteran owned small bus"), 
        col("in0.`source_accounting date - month`").alias("source_accounting date - month"), 
        col("in0.`source_wbs - description`").alias("source_wbs - description"), 
        col("in0.`source_vendor - cc1 - type`").alias("source_vendor - cc1 - type"), 
        col("in0.`source_invoice spend (actual fx rate)`").alias("source_invoice spend (actual fx rate)"), 
        col("in0.`hyperion function level 4`").alias("hyperion function level 4"), 
        col("in0.`vendor country`").alias("vendor country"), 
        col("in0.source_vendor_number").alias("source_vendor_number"), 
        col("in0.`source_invoice number`").alias("source_invoice number"), 
        col("in0.`Company Description`").alias("Company Description"), 
        col("in0.`source_vendor - cc3 - svc disabled vet owned sb`")\
          .alias("source_vendor - cc3 - svc disabled vet owned sb"), 
        col("in0.`tableau display category`").alias("tableau display category"), 
        col("in0.Country").alias("Country"), 
        col("in0.`_Planning Account`").alias("_Planning Account"), 
        col("in0.`source_vendor - cc4 - obs-foreign supplier`").alias("source_vendor - cc4 - obs-foreign supplier"), 
        col("in0._Sum_LCL").alias("_Sum_LCL"), 
        col("in0.`source_vendor - vendor global ultimate parent (enriched) temp1`")\
          .alias("source_vendor - vendor global ultimate parent (enriched) temp1"), 
        col("in0.`hyperion sub mc2`").alias("hyperion sub mc2"), 
        col("in0.`hyperion sub mc`").alias("hyperion sub mc"), 
        col("in0.`source_vendor - cc2 - women owned small bus`").alias("source_vendor - cc2 - women owned small bus"), 
        col("in0.`tableau display mega category`").alias("tableau display mega category"), 
        col("in0.`source_vendor - vendor global ultimate parent (enriched) old`")\
          .alias("source_vendor - vendor global ultimate parent (enriched) old"), 
        col("in0.`Category Old`").alias("Category Old"), 
        col("in0.`source_vendor - cc5 - small business`").alias("source_vendor - cc5 - small business"), 
        col("in0.ParentVendor_match_flag").alias("ParentVendor_match_flag"), 
        col("in0.`vendor country code`").alias("vendor country code"), 
        col("in0.`source_vendor - vendor (enriched)`").alias("source_vendor - vendor (enriched)"), 
        col("in0.`hyperion function level 3`").alias("hyperion function level 3"), 
        col("in0.`hyperion evp`").alias("hyperion evp"), 
        col("in0.`source_invoice spend (budget fx rate)`").alias("source_invoice spend (budget fx rate)"), 
        col("in0.`source_requester - user/buyer name responsible for spend`")\
          .alias("source_requester - user/buyer name responsible for spend"), 
        col("in0.`Country Old`").alias("Country Old"), 
        col("in0.`SUB GROUP`").alias("SUB GROUP"), 
        col("in0.`source_vendor - cc4 - hubzone small business`").alias("source_vendor - cc4 - hubzone small business"), 
        col("in0.`sap/concur source`").alias("sap/concur source"), 
        col("in0.`source_amgen region/country - world region`").alias("source_amgen region/country - world region"), 
        col("in1.`Gl Account`").alias("Gl Account"), 
        col("in0.`_category code`").alias("_category code"), 
        col("in0.`_Gl Account`").alias("_Gl Account"), 
        col("in0.`GL Account Description Updated`").alias("GL Account Description Updated"), 
        col("in0.`source_invoice spend (original currency)`").alias("source_invoice spend (original currency)"), 
        col("in0.`_product description`").alias("_product description"), 
        col("in0.`source_cost center number`").alias("source_cost center number"), 
        col("in0.`source_vendor - cc1 - indicator`").alias("source_vendor - cc1 - indicator"), 
        col("in0.`hyperion cc description`").alias("hyperion cc description"), 
        col("in1.Category").alias("Category"), 
        col(
            "in0.`_Gl Account (# only)`"
          )\
          .alias(
          "_Gl Account (# only)"
        ), 
        col("in0.`invoice linenumber`").alias("invoice linenumber"), 
        col("in0.`source_amgen region/country - sap company code`")\
          .alias("source_amgen region/country - sap company code"), 
        col("in0.`hyperion function level 2`").alias("hyperion function level 2"), 
        col("in0.`source_amgen region/country - world sub-region Old`")\
          .alias("source_amgen region/country - world sub-region Old"), 
        col("in0.`hyperion cc_function_mapping`").alias("hyperion cc_function_mapping"), 
        col("in0.`source_vendor - vendor global ultimate parent (enriched) temp`")\
          .alias("source_vendor - vendor global ultimate parent (enriched) temp"), 
        col("in0.`source_gl number`").alias("source_gl number"), 
        col("in0.`source_wbs - product name`").alias("source_wbs - product name"), 
        col("in0.`po description`").alias("po description"), 
        col("in0.`source_mg code`").alias("source_mg code"), 
        col("in1.`Regrouped Level 4`").alias("Regrouped Level 4"), 
        col("in0.`tableau display meta category`").alias("tableau display meta category"), 
        col(
            "in1.`Gl Account (# only)`"
          )\
          .alias(
          "Gl Account (# only)"
        ), 
        col("in0.`source_accounting date - year`").alias("source_accounting date - year"), 
        col("in0.`SUB-GROUP_Temp`").alias("SUB-GROUP_Temp"), 
        col("in0.Country_match_flag").alias("Country_match_flag"), 
        col("in0.`source_amgen region/country - country`").alias("source_amgen region/country - country"), 
        col("in1.`Planning Account`").alias("Planning Account"), 
        col("in0.`hyperion hlmc`").alias("hyperion hlmc"), 
        col("in0.`company code name`").alias("company code name"), 
        col("in0.`source_invoice quantity`").alias("source_invoice quantity"), 
        col("in0.`hyperion mc`").alias("hyperion mc"), 
        col("in0.`source_vendor - cc5 - obs-large business`").alias("source_vendor - cc5 - obs-large business"), 
        col("in0.`source_accounting date`").alias("source_accounting date"), 
        col("in0._Category").alias("_Category")
    )
