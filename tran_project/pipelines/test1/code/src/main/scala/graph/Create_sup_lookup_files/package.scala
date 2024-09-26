package graph

import io.prophecy.libs._
import udfs.PipelineInitCode._
import graph.Create_sup_lookup_files.config._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._
package object Create_sup_lookup_files {

  def apply(context: Context): Subgraph12 = {
    val df_Read_Proto_Range_sup_bidder_member_sales_tax_rate_pb_sup_bidder_member_sales_tax_rate =
      Read_Proto_Range_sup_bidder_member_sales_tax_rate_pb_sup_bidder_member_sales_tax_rate(
        context
      )
    val df_filter_deleted_records = filter_deleted_records(
      context,
      df_Read_Proto_Range_sup_bidder_member_sales_tax_rate_pb_sup_bidder_member_sales_tax_rate
    )
    val df_by_member_id_asc =
      by_member_id_asc(context, df_filter_deleted_records)
    val df_aggregate_sales_tax_rate =
      aggregate_sales_tax_rate(context, df_by_member_id_asc)
    val df_member_id_reformat =
      member_id_reformat(context,                    df_aggregate_sales_tax_rate)
    sup_bidder_member_sales_tax_rate_lookup(context, df_member_id_reformat)
    val df_df_dbg_tran_campaign_lkp_1  = df_dbg_tran_campaign_lkp_1(context)
    val df_df_dbg_tran_publisher_id_1  = df_dbg_tran_publisher_id_1(context)
    val df_df_dbg_tran_api_url_1       = df_dbg_tran_api_url_1(context)
    val df_df_dbg_tran_advertiser_id_1 = df_dbg_tran_advertiser_id_1(context)
    val df_df_dbg_tran_site_id_1       = df_dbg_tran_site_id_1(context)
    val df_df_dbg_tran_sup_member_id_1 = df_dbg_tran_sup_member_id_1(context)
    val df_df_dbg_tran_adv_id_by_campaign_id_1 =
      df_dbg_tran_adv_id_by_campaign_id_1(context)
    val df_df_dbg_tran_adv_campaign_1 = df_dbg_tran_adv_campaign_1(context)
    val df_df_dbg_tran_ip_range_1     = df_dbg_tran_ip_range_1(context)
    val df_df_dbg_tran_deal_lookup_1  = df_dbg_tran_deal_lookup_1(context)
    val df_df_dbg_tran_placement_video_attributes_1 =
      df_dbg_tran_placement_video_attributes_1(context)
    val df_df_dbg_tran_member_adv_1 = df_dbg_tran_member_adv_1(context)
    (df_df_dbg_tran_placement_video_attributes_1,
     df_df_dbg_tran_api_url_1,
     df_df_dbg_tran_publisher_id_1,
     df_df_dbg_tran_adv_id_by_campaign_id_1,
     df_df_dbg_tran_advertiser_id_1,
     df_df_dbg_tran_site_id_1,
     df_df_dbg_tran_ip_range_1,
     df_df_dbg_tran_deal_lookup_1,
     df_df_dbg_tran_campaign_lkp_1,
     df_df_dbg_tran_member_adv_1,
     df_df_dbg_tran_adv_campaign_1,
     df_df_dbg_tran_sup_member_id_1
    )
  }

}
