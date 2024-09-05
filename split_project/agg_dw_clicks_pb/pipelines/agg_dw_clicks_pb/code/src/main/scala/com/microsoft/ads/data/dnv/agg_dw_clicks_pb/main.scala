package com.microsoft.ads.data.dnv.agg_dw_clicks_pb

import io.prophecy.libs._
import com.microsoft.ads.data.dnv.agg_dw_clicks_pb.config._
import com.microsoft.ads.data.dnv.agg_dw_clicks_pb.udfs.UDFs._
import com.microsoft.ads.data.dnv.agg_dw_clicks_pb.udfs.ColumnFunctions._
import com.microsoft.ads.data.dnv.agg_dw_clicks_pb.udfs.PipelineInitCode._
import com.microsoft.ads.data.dnv.agg_dw_clicks_pb.graph._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Main {

  def apply(context: Context): Unit = {
    val df_Read_Proto_Range_agg_dw_impressions_pq_hour_2 =
      Read_Proto_Range_agg_dw_impressions_pq_hour_2(context)
    val (df_agg_dw_clicks_initialize_lookups_out4,
         df_agg_dw_clicks_initialize_lookups_out3,
         df_agg_dw_clicks_initialize_lookups_out2,
         df_agg_dw_clicks_initialize_lookups_out1,
         df_agg_dw_clicks_initialize_lookups_out
    ) = agg_dw_clicks_initialize_lookups.apply(
      agg_dw_clicks_initialize_lookups.config
        .Context(context.spark, context.config.agg_dw_clicks_initialize_lookups)
    )
    val df_Read_Proto_Range_agg_dw_curator_impressions_agg_dw_curator_impressions_2 =
      Read_Proto_Range_agg_dw_curator_impressions_agg_dw_curator_impressions_2(
        context
      )
    val df_Filter_by_Expression_to_filter_clicks_with_auction_id_64_lkpj0_dedup1 =
      Filter_by_Expression_to_filter_clicks_with_auction_id_64_lkpj0_dedup1(
        context,
        df_agg_dw_clicks_initialize_lookups_out
      )
    val df_Read_Proto_Range_log_dw_click_pb_log_dw_click_2 =
      Read_Proto_Range_log_dw_click_pb_log_dw_click_2(context)
    val df_Filter_by_Expression_to_filter_clicks_with_auction_id_64_lkpj0 =
      Filter_by_Expression_to_filter_clicks_with_auction_id_64_lkpj0(
        context,
        df_Read_Proto_Range_log_dw_click_pb_log_dw_click_2,
        df_Filter_by_Expression_to_filter_clicks_with_auction_id_64_lkpj0_dedup1
      )
    val df_Filter_by_Expression_to_filter_clicks_with_auction_id_64_lkp =
      Filter_by_Expression_to_filter_clicks_with_auction_id_64_lkp(
        context,
        df_Filter_by_Expression_to_filter_clicks_with_auction_id_64_lkpj0
      )
    val df_Filter_by_Expression_to_filter_clicks_with_auction_id_64_lkp_DropExtraColumns =
      Filter_by_Expression_to_filter_clicks_with_auction_id_64_lkp_DropExtraColumns(
        context,
        df_Filter_by_Expression_to_filter_clicks_with_auction_id_64_lkp
      )
    val df_Rollup = Rollup(
      context,
      df_Filter_by_Expression_to_filter_clicks_with_auction_id_64_lkp_DropExtraColumns
    )
    val df_Rollup_Reformat = Rollup_Reformat(context, df_Rollup)
    val df_Partition_by_auction_id_64_key =
      Partition_by_auction_id_64_key(context, df_Rollup_Reformat)
    val df_Filter_by_Expression_to_filter_imps_with_auction_id_64_lkpj0_dedup1 =
      Filter_by_Expression_to_filter_imps_with_auction_id_64_lkpj0_dedup1(
        context,
        df_agg_dw_clicks_initialize_lookups_out
      )
    val df_Partition_by_auction_id_64_agg_dw_impressions_in =
      Partition_by_auction_id_64_agg_dw_impressions_in(
        context,
        df_Read_Proto_Range_agg_dw_impressions_pq_hour_2
      )
    val df_Filter_by_Expression_to_filter_imps_with_auction_id_64_lkpj0 =
      Filter_by_Expression_to_filter_imps_with_auction_id_64_lkpj0(
        context,
        df_Partition_by_auction_id_64_agg_dw_impressions_in,
        df_Filter_by_Expression_to_filter_imps_with_auction_id_64_lkpj0_dedup1
      )
    val df_Filter_by_Expression_to_filter_imps_with_auction_id_64_lkp =
      Filter_by_Expression_to_filter_imps_with_auction_id_64_lkp(
        context,
        df_Filter_by_Expression_to_filter_imps_with_auction_id_64_lkpj0
      )
    val df_Filter_by_Expression_to_filter_imps_with_auction_id_64_lkp_DropExtraColumns =
      Filter_by_Expression_to_filter_imps_with_auction_id_64_lkp_DropExtraColumns(
        context,
        df_Filter_by_Expression_to_filter_imps_with_auction_id_64_lkp
      )
    val df_Partition_by_auction_id_64_agg_dw_impressions_2 =
      Partition_by_auction_id_64_agg_dw_impressions_2(
        context,
        df_Filter_by_Expression_to_filter_imps_with_auction_id_64_lkp_DropExtraColumns
      )
    val df_Filter_by_Expression_isCrossNetCPC =
      Filter_by_Expression_isCrossNetCPC(
        context,
        df_Partition_by_auction_id_64_agg_dw_impressions_2
      )
    val df_Read_Proto_Range_log_impbus_auction_event_pb_log_impbus_auction_event_2 =
      Read_Proto_Range_log_impbus_auction_event_pb_log_impbus_auction_event_2(
        context
      )
    val df_Filter_by_transaction_event_type_id_3 =
      Filter_by_transaction_event_type_id_3(
        context,
        df_Read_Proto_Range_log_impbus_auction_event_pb_log_impbus_auction_event_2
      )
    val df_Partition_by_auction_id_64_log_impbus_auction_events =
      Partition_by_auction_id_64_log_impbus_auction_events(
        context,
        df_Filter_by_transaction_event_type_id_3
      )
    val df_Join_AE_with_CrossNetCPC_agg_dw_impressions =
      Join_AE_with_CrossNetCPC_agg_dw_impressions(
        context,
        df_Filter_by_Expression_isCrossNetCPC,
        df_Partition_by_auction_id_64_log_impbus_auction_events
      )
    val df_Join_add_dw_impressions_with_CrossNetCPC_agg_dw_impessions =
      Join_add_dw_impressions_with_CrossNetCPC_agg_dw_impessions(
        context,
        df_Partition_by_auction_id_64_agg_dw_impressions_2,
        df_Join_AE_with_CrossNetCPC_agg_dw_impressions
      )
    val df_Join_aggDwClicksPb = Join_aggDwClicksPb(
      context,
      df_Partition_by_auction_id_64_key,
      df_Join_add_dw_impressions_with_CrossNetCPC_agg_dw_impessions,
      df_Partition_by_auction_id_64_log_impbus_auction_events
    )
    val df_Join_aggDwClicksPb_AddLookupResultsj0_dedup1 =
      Join_aggDwClicksPb_AddLookupResultsj0_dedup1(
        context,
        df_agg_dw_clicks_initialize_lookups_out1
      )
    val df_Join_aggDwClicksPb_AddLookupResultsj0 =
      Join_aggDwClicksPb_AddLookupResultsj0(
        context,
        df_Join_aggDwClicksPb,
        df_Join_aggDwClicksPb_AddLookupResultsj0_dedup1
      )
    val df_Join_aggDwClicksPb_AddLookupResults =
      Join_aggDwClicksPb_AddLookupResults(
        context,
        df_Join_aggDwClicksPb_AddLookupResultsj0
      )
    val df_Join_aggDwClicksPb_DropExtraColumns =
      Join_aggDwClicksPb_DropExtraColumns(context,
                                          df_Join_aggDwClicksPb_AddLookupResults
      )
    val df_Filter_by_Expression_aggDwClicksPb =
      Filter_by_Expression_aggDwClicksPb(context,
                                         df_Join_aggDwClicksPb_DropExtraColumns
      )
    val df_SchemaTransform_DefaultToNull_agg_dw_clicks =
      SchemaTransform_DefaultToNull_agg_dw_clicks(
        context,
        df_Filter_by_Expression_aggDwClicksPb
      )
    Write_Proto_HDFS_agg_dw_clicks_agg_dw_clicks_pb(
      context,
      df_SchemaTransform_DefaultToNull_agg_dw_clicks
    )
    val df_External_Clicks = External_Clicks.apply(
      External_Clicks.config
        .Context(context.spark, context.config.External_Clicks),
      df_agg_dw_clicks_initialize_lookups_out2,
      df_Partition_by_auction_id_64_agg_dw_impressions_in,
      df_agg_dw_clicks_initialize_lookups_out3,
      df_agg_dw_clicks_initialize_lookups_out4
    )
    val df_Read_Proto_Range_log_impbus_clicktracker_pb_log_impbus_clicktracker =
      Read_Proto_Range_log_impbus_clicktracker_pb_log_impbus_clicktracker(
        context
      )
    val df_Partition_by_auction_id_64_log_impbus_clicktracker_in =
      Partition_by_auction_id_64_log_impbus_clicktracker_in(
        context,
        df_Read_Proto_Range_log_impbus_clicktracker_pb_log_impbus_clicktracker
      )
    val df_Read_Proto_Range_log_dw_clicktracker_pb_log_dw_clicktracker =
      Read_Proto_Range_log_dw_clicktracker_pb_log_dw_clicktracker(context)
    val df_Rollup_To_Dedup = Rollup_To_Dedup(
      context,
      df_Read_Proto_Range_log_dw_clicktracker_pb_log_dw_clicktracker
    )
    val df_Rollup_To_Dedup_Reformat =
      Rollup_To_Dedup_Reformat(context, df_Rollup_To_Dedup)
    val df_Join_aggDwClicktrackerPb = Join_aggDwClicktrackerPb(
      context,
      df_Partition_by_auction_id_64_log_impbus_clicktracker_in,
      df_Rollup_To_Dedup_Reformat
    )
    Write_Proto_HDFS_agg_dw_clicktracker_agg_dw_clicktracker_pb(
      context,
      df_Join_aggDwClicktrackerPb
    )
    val df_Partition_by_auction_id_64_agg_dw_curator_impressions_in =
      Partition_by_auction_id_64_agg_dw_curator_impressions_in(
        context,
        df_Read_Proto_Range_agg_dw_curator_impressions_agg_dw_curator_impressions_2
      )
    val df_Join_Impbus_Clicks_with_dw_curator_impressions =
      Join_Impbus_Clicks_with_dw_curator_impressions(
        context,
        df_Partition_by_auction_id_64_agg_dw_curator_impressions_in,
        df_External_Clicks
      )
    val df_SchemaTransform_DefaultToNull_agg_impbus_curator_clicks =
      SchemaTransform_DefaultToNull_agg_impbus_curator_clicks(
        context,
        df_Join_Impbus_Clicks_with_dw_curator_impressions
      )
    Write_Proto_HDFS_agg_impbus_curator_clicks_agg_impbus_curator_clicks(
      context,
      df_SchemaTransform_DefaultToNull_agg_impbus_curator_clicks
    )
    val df_Filter_on_imp_type_in_6_7 =
      Filter_on_imp_type_in_6_7(context, df_Filter_by_Expression_aggDwClicksPb)
    val df_Dedup_Sorted_auction_id_64 =
      Dedup_Sorted_auction_id_64(context, df_Filter_on_imp_type_in_6_7)
    val df_Sort_auction_id_64 =
      Sort_auction_id_64(context, df_Dedup_Sorted_auction_id_64)
    val df_Join_dw_clicks_with_curator_impressions =
      Join_dw_clicks_with_curator_impressions(
        context,
        df_Partition_by_auction_id_64_agg_dw_curator_impressions_in,
        df_Sort_auction_id_64
      )
    val df_SchemaTransform_DefaultToNull_agg_dw_curator_clicks =
      SchemaTransform_DefaultToNull_agg_dw_curator_clicks(
        context,
        df_Join_dw_clicks_with_curator_impressions
      )
    Write_Proto_HDFS_agg_dw_curator_clicks_agg_dw_curator_clicks(
      context,
      df_SchemaTransform_DefaultToNull_agg_dw_curator_clicks
    )
    val df_SchemaTransform_DefaultToNull_agg_impbus_clicks =
      SchemaTransform_DefaultToNull_agg_impbus_clicks(context,
                                                      df_External_Clicks
      )
    Write_Proto_HDFS_agg_impbus_clicks_agg_impbus_clicks_pb(
      context,
      df_SchemaTransform_DefaultToNull_agg_impbus_clicks
    )
  }

  def main(args: Array[String]): Unit = {
    val config = ConfigurationFactoryImpl.getConfig(args)
    val spark: SparkSession = SparkSession
      .builder()
      .appName("Prophecy Pipeline")
      .config("spark.default.parallelism",             "4")
      .config("spark.sql.legacy.allowUntypedScalaUDF", "true")
      .enableHiveSupport()
      .getOrCreate()
    val context = Context(spark, config)
    spark.conf
      .set("prophecy.metadata.pipeline.uri", "pipelines/agg_dw_clicks_pb")
    registerUDFs(spark)
    MetricsCollector.instrument(spark, "pipelines/agg_dw_clicks_pb") {
      apply(context)
    }
  }

}
