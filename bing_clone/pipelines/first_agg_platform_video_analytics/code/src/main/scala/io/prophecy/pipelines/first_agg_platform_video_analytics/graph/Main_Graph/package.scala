package io.prophecy.pipelines.first_agg_platform_video_analytics.graph

import io.prophecy.libs._
import io.prophecy.pipelines.first_agg_platform_video_analytics.udfs.PipelineInitCode._
import io.prophecy.pipelines.first_agg_platform_video_analytics.graph.Main_Graph.config._
import io.prophecy.pipelines.first_agg_platform_video_analytics.graph.Main_Graph.Validate_Pick_Video_Events
import io.prophecy.pipelines.first_agg_platform_video_analytics.graph.Main_Graph.xr_partition_key_filter_checkpointed_sort_agg_dw_clicks
import io.prophecy.pipelines.first_agg_platform_video_analytics.graph.Main_Graph.Validate_Pick_Clicks
import io.prophecy.pipelines.first_agg_platform_video_analytics.graph.Main_Graph.xr_partition_key_filter_checkpointed_sort_agg_dw_pixels
import io.prophecy.pipelines.first_agg_platform_video_analytics.graph.Main_Graph.Validate_Pick_Pixels
import io.prophecy.pipelines.first_agg_platform_video_analytics.graph.Main_Graph.xr_partition_key_filter_checkpointed_sort_agg_impbus_clicks
import io.prophecy.pipelines.first_agg_platform_video_analytics.graph.Main_Graph.Validate_Pick_impbus_Clicks_imp_type_6
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._
package object Main_Graph {

  def apply(context: Context): DataFrame = {
    val df_Read_Proto_Range_agg_dw_video_events_pq_agg_dw_video_events =
      Read_Proto_Range_agg_dw_video_events_pq_agg_dw_video_events(context)
    val df_repartition_by_auction_id = repartition_by_auction_id(
      context,
      df_Read_Proto_Range_agg_dw_video_events_pq_agg_dw_video_events
    )
    val df_Read_Proto_Range_agg_platform_video_impressions_pq_agg_platform_video_impressions =
      Read_Proto_Range_agg_platform_video_impressions_pq_agg_platform_video_impressions(
        context
      )
    val df_repartition_dataframe_1 = repartition_dataframe_1(
      context,
      df_Read_Proto_Range_agg_platform_video_impressions_pq_agg_platform_video_impressions
    )
    val df_Read_Proto_Range_agg_platform_video_requests_pq_agg_platform_video_requests =
      Read_Proto_Range_agg_platform_video_requests_pq_agg_platform_video_requests(
        context
      )
    val df_Read_Proto_Range_agg_platform_video_requests_transactable_pq_agg_platform_video_requests_transactable_0_to_5 =
      Read_Proto_Range_agg_platform_video_requests_transactable_pq_agg_platform_video_requests_transactable_0_to_5(
        context
      )
    val df_Filter_by_Expression_Transactable =
      Filter_by_Expression_Transactable(
        context,
        df_Read_Proto_Range_agg_platform_video_requests_transactable_pq_agg_platform_video_requests_transactable_0_to_5
      )
    val df_Partition_by_Key_UnionAll = Partition_by_Key_UnionAll(
      context,
      df_Read_Proto_Range_agg_platform_video_requests_pq_agg_platform_video_requests,
      df_Filter_by_Expression_Transactable
    )
    val df_repartition_by_auction_id_1 =
      repartition_by_auction_id_1(context, df_Partition_by_Key_UnionAll)
    val df_Filter_by_Expression_3 =
      Filter_by_Expression_3(context, df_repartition_by_auction_id_1)
    val df_Dedup_Sorted_1 = Dedup_Sorted_1(context, df_Filter_by_Expression_3)
    val df_Reformat_1_4   = Reformat_1_4(context,   df_Dedup_Sorted_1)
    val df_Filter_by_Expression_6 =
      Filter_by_Expression_6(context, df_repartition_by_auction_id)
    val df_Validate_Pick_Video_Events = Validate_Pick_Video_Events.apply(
      Validate_Pick_Video_Events.config
        .Context(context.spark, context.config.Validate_Pick_Video_Events),
      df_Filter_by_Expression_6
    )
    val df_Reformat_2_1 = Reformat_2_1(context, df_Validate_Pick_Video_Events)
    val df_repartition_by_auction_id_1_1 =
      repartition_by_auction_id_1_1(context, df_Reformat_2_1)
    val df_join_by_auction_id = join_by_auction_id(
      context,
      df_Reformat_1_4,
      df_repartition_by_auction_id_1_1
    )
    val df_Filter_by_Expression_1 =
      Filter_by_Expression_1(context, df_repartition_dataframe_1)
    val df_Reformat_3_1 = Reformat_3_1(context, df_Filter_by_Expression_1)
    val df_join_and_project_auction_data = join_and_project_auction_data(
      context,
      df_join_by_auction_id,
      df_Reformat_3_1
    )
    val df_Read_Proto_Range_agg_dw_clicks_pb_agg_dw_clicks =
      Read_Proto_Range_agg_dw_clicks_pb_agg_dw_clicks(context)
    val df_xr_partition_key_filter_checkpointed_sort_agg_dw_clicks =
      xr_partition_key_filter_checkpointed_sort_agg_dw_clicks.apply(
        xr_partition_key_filter_checkpointed_sort_agg_dw_clicks.config.Context(
          context.spark,
          context.config.xr_partition_key_filter_checkpointed_sort_agg_dw_clicks
        ),
        df_Read_Proto_Range_agg_dw_clicks_pb_agg_dw_clicks
      )
    val df_Validate_Pick_Clicks = Validate_Pick_Clicks.apply(
      Validate_Pick_Clicks.config
        .Context(context.spark, context.config.Validate_Pick_Clicks),
      df_xr_partition_key_filter_checkpointed_sort_agg_dw_clicks
    )
    val df_Reformat_4 = Reformat_4(context, df_Validate_Pick_Clicks)
    val df_Read_Proto_Range_agg_dw_pixels_pq_agg_dw_pixels =
      Read_Proto_Range_agg_dw_pixels_pq_agg_dw_pixels(context)
    val df_xr_partition_key_filter_checkpointed_sort_agg_dw_pixels =
      xr_partition_key_filter_checkpointed_sort_agg_dw_pixels.apply(
        xr_partition_key_filter_checkpointed_sort_agg_dw_pixels.config.Context(
          context.spark,
          context.config.xr_partition_key_filter_checkpointed_sort_agg_dw_pixels
        ),
        df_Read_Proto_Range_agg_dw_pixels_pq_agg_dw_pixels
      )
    val df_Validate_Pick_Pixels = Validate_Pick_Pixels.apply(
      Validate_Pick_Pixels.config
        .Context(context.spark, context.config.Validate_Pick_Pixels),
      df_xr_partition_key_filter_checkpointed_sort_agg_dw_pixels
    )
    val df_Reformat_5 = Reformat_5(context, df_Validate_Pick_Pixels)
    val df_Read_Proto_Range_agg_impbus_clicks_pb_agg_impbus_clicks =
      Read_Proto_Range_agg_impbus_clicks_pb_agg_impbus_clicks(context)
    val df_xr_partition_key_filter_checkpointed_sort_agg_impbus_clicks =
      xr_partition_key_filter_checkpointed_sort_agg_impbus_clicks.apply(
        xr_partition_key_filter_checkpointed_sort_agg_impbus_clicks.config
          .Context(
            context.spark,
            context.config.xr_partition_key_filter_checkpointed_sort_agg_impbus_clicks
          ),
        df_Read_Proto_Range_agg_impbus_clicks_pb_agg_impbus_clicks
      )
    val df_Validate_Pick_impbus_Clicks_imp_type_6 =
      Validate_Pick_impbus_Clicks_imp_type_6.apply(
        Validate_Pick_impbus_Clicks_imp_type_6.config.Context(
          context.spark,
          context.config.Validate_Pick_impbus_Clicks_imp_type_6
        ),
        df_xr_partition_key_filter_checkpointed_sort_agg_impbus_clicks
      )
    val df_Reformat_6 =
      Reformat_6(context, df_Validate_Pick_impbus_Clicks_imp_type_6)
    val df_spark_context_setup = spark_context_setup(
      context,
      df_join_and_project_auction_data,
      df_Reformat_4,
      df_Reformat_5,
      df_Reformat_6
    )
    df_spark_context_setup
  }

}
