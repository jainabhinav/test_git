package io.prophecy.pipelines.first_agg_platform_video_analytics.graph

import io.prophecy.libs._
import io.prophecy.pipelines.first_agg_platform_video_analytics.udfs.PipelineInitCode._
import io.prophecy.pipelines.first_agg_platform_video_analytics.graph.Create_sup_lookup_files.config._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._
package object Create_sup_lookup_files {

  def apply(context: Context): DataFrame = {
    val df_Read_Proto_Range_sup_bidder_fx_rates =
      Read_Proto_Range_sup_bidder_fx_rates(context)
    val df_Reformat_sup_bidder_fx_rates = Reformat_sup_bidder_fx_rates(
      context,
      df_Read_Proto_Range_sup_bidder_fx_rates
    )
    sup_bidder_fx_rates_lookup(context, df_Reformat_sup_bidder_fx_rates)
    val df_Read_Proto_Range_sup_code_fx_rate =
      Read_Proto_Range_sup_code_fx_rate(context)
    val df_Reformat_sup_code_fx_rate =
      Reformat_sup_code_fx_rate(context, df_Read_Proto_Range_sup_code_fx_rate)
    val df_Read_Proto_Range_sup_api_member_pb =
      Read_Proto_Range_sup_api_member_pb(context)
    val df_Reformat_sup_api_member_pb =
      Reformat_sup_api_member_pb(context, df_Read_Proto_Range_sup_api_member_pb)
    sup_api_member_pb_lookup(context,     df_Reformat_sup_api_member_pb)
    val df_Read_Proto_Range_sup_bidder_advertiser_pb =
      Read_Proto_Range_sup_bidder_advertiser_pb(context)
    val df_Dedup_Sorted = Dedup_Sorted(context, df_Reformat_sup_code_fx_rate)
    sup_sup_code_fx_rate_lookup(context, df_Dedup_Sorted)
    val df_Reformat_sup_bidder_advertiser_pb =
      Reformat_sup_bidder_advertiser_pb(
        context,
        df_Read_Proto_Range_sup_bidder_advertiser_pb
      )
    val df_sup_bidder_advertiser_pb_lookup_dedup1 =
      sup_bidder_advertiser_pb_lookup_dedup1(
        context,
        df_Reformat_sup_bidder_advertiser_pb
      )
    df_sup_bidder_advertiser_pb_lookup_dedup1
  }

}