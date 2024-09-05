package com.microsoft.ads.data.dnv.agg_dw_clicks_pb.graph

import io.prophecy.libs._
import com.microsoft.ads.data.dnv.agg_dw_clicks_pb.udfs.PipelineInitCode._
import com.microsoft.ads.data.dnv.agg_dw_clicks_pb.graph.agg_dw_clicks_initialize_lookups.config._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._
package object agg_dw_clicks_initialize_lookups {

  def apply(context: Context): Subgraph5 = {
    val df_Read_Proto_Range_sup_bidder_member_sales_tax_rate_pb_sup_bidder_member_sales_tax_rate =
      Read_Proto_Range_sup_bidder_member_sales_tax_rate_pb_sup_bidder_member_sales_tax_rate(
        context
      )
    val df_Read_Proto_Range_log_impbus_click_pb_log_impbus_click =
      Read_Proto_Range_log_impbus_click_pb_log_impbus_click(context)
    val df_Reformat_to_select_auction_id_64_1 =
      Reformat_to_select_auction_id_64_1(
        context,
        df_Read_Proto_Range_log_impbus_click_pb_log_impbus_click
      )
    val df_Read_Proto_Range_msan_map_pb_msan_map =
      Read_Proto_Range_msan_map_pb_msan_map(context)
    val df_Partition_by_Key =
      Partition_by_Key(context, df_Read_Proto_Range_msan_map_pb_msan_map)
    val df_Filter_by_Expression_Msan_Clicks =
      Filter_by_Expression_Msan_Clicks(context, df_Partition_by_Key)
    val df_Reformat_to_select_auction_id_64_msan =
      Reformat_to_select_auction_id_64_msan(context,
                                            df_Filter_by_Expression_Msan_Clicks
      )
    val df_Dedup_Sorted_auctions =
      Dedup_Sorted_auctions(context, df_Reformat_to_select_auction_id_64_msan)
    val df_Filter_by_Expression = Filter_by_Expression(
      context,
      df_Read_Proto_Range_sup_bidder_member_sales_tax_rate_pb_sup_bidder_member_sales_tax_rate
    )
    val df_Read_Proto_Range_log_dw_click_pb_log_dw_click =
      Read_Proto_Range_log_dw_click_pb_log_dw_click(context)
    val df_Reformat_to_select_auction_id_64 = Reformat_to_select_auction_id_64(
      context,
      df_Read_Proto_Range_log_dw_click_pb_log_dw_click
    )
    val df_Filter_by_Expression_Msan_buyer_member_id =
      Filter_by_Expression_Msan_buyer_member_id(context, df_Partition_by_Key)
    val df_Sort_1     = Sort_1(context,     df_Reformat_to_select_auction_id_64_1)
    val df_Sort       = Sort(context,       df_Reformat_to_select_auction_id_64)
    val df_Rollup_1_2 = Rollup_1_2(context, df_Sort)
    val df_Reformat_to_select_buyer_member_id_msan =
      Reformat_to_select_buyer_member_id_msan(
        context,
        df_Filter_by_Expression_Msan_buyer_member_id
      )
    val df_Dedup_Sorted_buyer_member = Dedup_Sorted_buyer_member(
      context,
      df_Reformat_to_select_buyer_member_id_msan
    )
    val df_Rollup_1_1          = Rollup_1_1(context,          df_Sort_1)
    val df_Rollup_1_Reformat_1 = Rollup_1_Reformat_1(context, df_Rollup_1_1)
    val df_Rollup_Reformat_1   = Rollup_Reformat_1(context,   df_Rollup_1_2)
    val df_Sort_msan_map_auctions =
      Sort_msan_map_auctions(context, df_Dedup_Sorted_auctions)
    val df_Sort_msan_map_buyer_member =
      Sort_msan_map_buyer_member(context, df_Dedup_Sorted_buyer_member)
    (df_Rollup_1_Reformat_1,
     df_Sort_msan_map_auctions,
     df_Sort_msan_map_buyer_member,
     df_Filter_by_Expression,
     df_Rollup_Reformat_1
    )
  }

}
