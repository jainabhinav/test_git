package com.microsoft.ads.data.dnv.agg_dw_clicks_pb.graph.External_Clicks

import io.prophecy.libs._
import com.microsoft.ads.data.dnv.agg_dw_clicks_pb.udfs.PipelineInitCode._
import com.microsoft.ads.data.dnv.agg_dw_clicks_pb.graph.External_Clicks.LookupToJoin30.config._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._
package object LookupToJoin30 {

  def apply(
    context: Context,
    in2:     DataFrame,
    in1:     DataFrame,
    in:      DataFrame
  ): DataFrame = {
    val df_External_Clicks__Filter_by_Expression_filter_log_impbus_clicks_with_auction_id_64_lkp_and_bidder_id_2_and_non_msan_map_clicksj0_dedup1 =
      External_Clicks__Filter_by_Expression_filter_log_impbus_clicks_with_auction_id_64_lkp_and_bidder_id_2_and_non_msan_map_clicksj0_dedup1(
        context,
        in1
      )
    val df_External_Clicks__Filter_by_Expression_filter_log_impbus_clicks_with_auction_id_64_lkp_and_bidder_id_2_and_non_msan_map_clicksj0_dedup2 =
      External_Clicks__Filter_by_Expression_filter_log_impbus_clicks_with_auction_id_64_lkp_and_bidder_id_2_and_non_msan_map_clicksj0_dedup2(
        context,
        in2
      )
    val df_External_Clicks__Filter_by_Expression_filter_log_impbus_clicks_with_auction_id_64_lkp_and_bidder_id_2_and_non_msan_map_clicksj0 =
      External_Clicks__Filter_by_Expression_filter_log_impbus_clicks_with_auction_id_64_lkp_and_bidder_id_2_and_non_msan_map_clicksj0(
        context,
        in,
        df_External_Clicks__Filter_by_Expression_filter_log_impbus_clicks_with_auction_id_64_lkp_and_bidder_id_2_and_non_msan_map_clicksj0_dedup1,
        df_External_Clicks__Filter_by_Expression_filter_log_impbus_clicks_with_auction_id_64_lkp_and_bidder_id_2_and_non_msan_map_clicksj0_dedup2
      )
    df_External_Clicks__Filter_by_Expression_filter_log_impbus_clicks_with_auction_id_64_lkp_and_bidder_id_2_and_non_msan_map_clicksj0
  }

}
