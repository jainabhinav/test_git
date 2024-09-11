package io.prophecy.pipelines.third_agg_platform_video_analytics_hourly.graph

import io.prophecy.libs._
import io.prophecy.pipelines.third_agg_platform_video_analytics_hourly.udfs.PipelineInitCode._
import io.prophecy.pipelines.third_agg_platform_video_analytics_hourly.graph.Create_sup_lookup_files.config._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._
package object Create_sup_lookup_files {

  def apply(context: Context): Subgraph2 = {
    val df_Read_Proto_Range_sup_api_member_pb =
      Read_Proto_Range_sup_api_member_pb(context)
    val df_Reformat_sup_api_member_pb =
      Reformat_sup_api_member_pb(context, df_Read_Proto_Range_sup_api_member_pb)
    sup_api_member_pb_lookup(context,     df_Reformat_sup_api_member_pb)
    val df_Read_Proto_Range_sup_member_attributes_pb =
      Read_Proto_Range_sup_member_attributes_pb(context)
    val df_Reformat_sup_member_attributes_pb =
      Reformat_sup_member_attributes_pb(
        context,
        df_Read_Proto_Range_sup_member_attributes_pb
      )
    val df_Dedup_Sorted_sup_member_attributes_pb =
      Dedup_Sorted_sup_member_attributes_pb(context,
                                            df_Reformat_sup_member_attributes_pb
      )
    sup_member_attributes_pb_lookup(context,
                                    df_Dedup_Sorted_sup_member_attributes_pb
    )
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
    val df_Dedup_Sorted_sup_code_fx_rate =
      Dedup_Sorted_sup_code_fx_rate(context, df_Reformat_sup_code_fx_rate)
    create_fx_rate_lookup(context,           df_Dedup_Sorted_sup_code_fx_rate)
    SetCheckpointDir_1(context)
    val df_Read_DML_Range_sup_api_inventory_url_content_category_tsv_sup_api_inventory_url_content_category =
      Read_DML_Range_sup_api_inventory_url_content_category_tsv_sup_api_inventory_url_content_category(
        context
      )
    val df_Reformat_sup_api_inventory_url_content_category_tsv_sup_api_inventory_url_content_category_with_header_filter_Filter_select =
      Reformat_sup_api_inventory_url_content_category_tsv_sup_api_inventory_url_content_category_with_header_filter_Filter_select(
        context,
        df_Read_DML_Range_sup_api_inventory_url_content_category_tsv_sup_api_inventory_url_content_category
      )
    val df_Reformat_sup_api_inventory_url_content_category_tsv_sup_api_inventory_url_content_category_with_header_filter_Reformat =
      Reformat_sup_api_inventory_url_content_category_tsv_sup_api_inventory_url_content_category_with_header_filter_Reformat(
        context,
        df_Reformat_sup_api_inventory_url_content_category_tsv_sup_api_inventory_url_content_category_with_header_filter_Filter_select
      )
    val df_Read_Proto_Range_sup_bidder_advertiser_pb =
      Read_Proto_Range_sup_bidder_advertiser_pb(context)
    val df_Reformat_sup_bidder_advertiser_pb =
      Reformat_sup_bidder_advertiser_pb(
        context,
        df_Read_Proto_Range_sup_bidder_advertiser_pb
      )
    val df_sup_api_inventory_url_content_category_lookup_dedup1 =
      sup_api_inventory_url_content_category_lookup_dedup1(
        context,
        df_Reformat_sup_api_inventory_url_content_category_tsv_sup_api_inventory_url_content_category_with_header_filter_Reformat
      )
    val df_sup_bidder_advertiser_pb_lookup_dedup4 =
      sup_bidder_advertiser_pb_lookup_dedup4(
        context,
        df_Reformat_sup_bidder_advertiser_pb
      )
    (df_sup_api_inventory_url_content_category_lookup_dedup1,
     df_sup_bidder_advertiser_pb_lookup_dedup4
    )
  }

}
