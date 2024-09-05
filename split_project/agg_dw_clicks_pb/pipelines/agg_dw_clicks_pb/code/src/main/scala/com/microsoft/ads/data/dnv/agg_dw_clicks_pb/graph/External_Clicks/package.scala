package com.microsoft.ads.data.dnv.agg_dw_clicks_pb.graph

import io.prophecy.libs._
import com.microsoft.ads.data.dnv.agg_dw_clicks_pb.udfs.PipelineInitCode._
import com.microsoft.ads.data.dnv.agg_dw_clicks_pb.graph.External_Clicks.config._
import com.microsoft.ads.data.dnv.agg_dw_clicks_pb.graph.External_Clicks.LookupToJoin20
import com.microsoft.ads.data.dnv.agg_dw_clicks_pb.graph.External_Clicks.LookupToJoin30
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._
package object External_Clicks {

  def apply(
    context: Context,
    in3:     DataFrame,
    in2:     DataFrame,
    in1:     DataFrame,
    in:      DataFrame
  ): DataFrame = {
    val df_External_Clicks__Read_Proto_Range_log_impbus_click_pb_2 =
      External_Clicks__Read_Proto_Range_log_impbus_click_pb_2(context)
    val df_LookupToJoin20 = LookupToJoin20.apply(
      LookupToJoin20.config
        .Context(context.spark, context.config.LookupToJoin20),
      in2,
      in,
      in3
    )
    val df_Filter_by_Expression_to_filter_imps_with_auction_id_64_lkp_and_imp_type_6 =
      Filter_by_Expression_to_filter_imps_with_auction_id_64_lkp_and_imp_type_6(
        context,
        df_LookupToJoin20
      )
    val df_Filter_by_Expression_to_filter_imps_with_auction_id_64_lkp_and_imp_type_6_DropExtraColumns =
      Filter_by_Expression_to_filter_imps_with_auction_id_64_lkp_and_imp_type_6_DropExtraColumns(
        context,
        df_Filter_by_Expression_to_filter_imps_with_auction_id_64_lkp_and_imp_type_6
      )
    val df_Partition_by_auction_id_64_agg_dw_impressions_1 =
      Partition_by_auction_id_64_agg_dw_impressions_1(
        context,
        df_Filter_by_Expression_to_filter_imps_with_auction_id_64_lkp_and_imp_type_6_DropExtraColumns
      )
    val df_LookupToJoin30 = LookupToJoin30.apply(
      LookupToJoin30.config
        .Context(context.spark, context.config.LookupToJoin30),
      in,
      in1,
      df_External_Clicks__Read_Proto_Range_log_impbus_click_pb_2
    )
    val df_Filter_by_Expression_filter_log_impbus_clicks_with_auction_id_64_lkp_and_bidder_id_2_and_non_msan_map_clicks =
      Filter_by_Expression_filter_log_impbus_clicks_with_auction_id_64_lkp_and_bidder_id_2_and_non_msan_map_clicks(
        context,
        df_LookupToJoin30
      )
    val df_Filter_by_Expression_filter_log_impbus_clicks_with_auction_id_64_lkp_and_bidder_id_2_and_non_msan_map_clicks_DropExtraColumns =
      Filter_by_Expression_filter_log_impbus_clicks_with_auction_id_64_lkp_and_bidder_id_2_and_non_msan_map_clicks_DropExtraColumns(
        context,
        df_Filter_by_Expression_filter_log_impbus_clicks_with_auction_id_64_lkp_and_bidder_id_2_and_non_msan_map_clicks
      )
    val df_Rollup_1 = Rollup_1(
      context,
      df_Filter_by_Expression_filter_log_impbus_clicks_with_auction_id_64_lkp_and_bidder_id_2_and_non_msan_map_clicks_DropExtraColumns
    )
    val df_Rollup_1_Reformat = Rollup_1_Reformat(context, df_Rollup_1)
    val df_Partition_by_auction_id_64_key_1 =
      Partition_by_auction_id_64_key_1(context, df_Rollup_1_Reformat)
    val df_Join_aggImpbusClickPb = Join_aggImpbusClickPb(
      context,
      df_Partition_by_auction_id_64_key_1,
      df_Partition_by_auction_id_64_agg_dw_impressions_1
    )
    val df_Filter_by_Expression_aggImpbusClicksPb =
      Filter_by_Expression_aggImpbusClicksPb(context, df_Join_aggImpbusClickPb)
    df_Filter_by_Expression_aggImpbusClicksPb
  }

}
