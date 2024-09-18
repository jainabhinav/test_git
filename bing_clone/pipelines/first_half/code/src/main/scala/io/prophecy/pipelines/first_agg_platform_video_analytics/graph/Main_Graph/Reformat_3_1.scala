package io.prophecy.pipelines.first_agg_platform_video_analytics.graph.Main_Graph

import io.prophecy.libs._
import io.prophecy.pipelines.first_agg_platform_video_analytics.udfs.PipelineInitCode._
import io.prophecy.pipelines.first_agg_platform_video_analytics.udfs.UDFs._
import io.prophecy.pipelines.first_agg_platform_video_analytics.udfs.ColumnFunctions._
import io.prophecy.pipelines.first_agg_platform_video_analytics.graph.Main_Graph.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Reformat_3_1 {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(
      col("date_time"),
      col("auction_id_64"),
      col("seller_member_id"),
      col("buyer_member_id"),
      col("is_delivered"),
      col("cleared_direct"),
      col("transaction_type"),
      col("advertiser_id"),
      col("insertion_order_id"),
      col("campaign_group_id"),
      col("campaign_id"),
      col("publisher_id"),
      col("bidder_id"),
      col("creative_id"),
      col("brand_id"),
      col("is_dw_buyer"),
      col("is_dw_seller"),
      col("has_dw_buy"),
      col("has_dw_sell"),
      col("booked_revenue_dollars"),
      col("buyer_media_cost_cpm"),
      col("auction_service_fees"),
      col("auction_service_deduction"),
      col("clear_fees"),
      col("creative_overage_fees"),
      col("seller_media_cost_cpm"),
      col("seller_revenue"),
      col("seller_deduction"),
      col("discrepancy_allowance"),
      col("commission_cpm"),
      col("commission_revshare"),
      col("serving_fees_cpm"),
      col("serving_fees_revshare"),
      col("advertiser_currency"),
      col("advertiser_exchange_rate"),
      col("publisher_currency"),
      col("publisher_exchange_rate"),
      col("deal_id"),
      col("geo_country"),
      col("imp_blacklist_or_fraud"),
      col("pricing_media_type"),
      col("imp_ignored"),
      col("is_prebid"),
      col("is_unit_of_buyer_trx"),
      col("is_unit_of_seller_trx"),
      col("custom_model_id"),
      col("media_type"),
      col("device_type"),
      col("two_phase_reduction_applied"),
      col("buyer_charges"),
      col("seller_charges"),
      col("seller_transaction_def"),
      col("buyer_transaction_def"),
      col("view_measurable"),
      col("viewable"),
      col("imp_type"),
      col("viewdef_definition_id"),
      col("viewdef_viewed_imps"),
      col("imp_rejecter_do_auction"),
      col("buyer_trx_event_id"),
      col("seller_trx_event_id"),
      col("inventory_url_id"),
      when(col("mobile_app_instance_id") === lit(0),
           lit(null).cast(IntegerType)
      ).otherwise(col("mobile_app_instance_id")).as("mobile_app_instance_id"),
      col("user_id_64"),
      col("split_id"),
      col("campaign_group_type_id"),
      col("advertiser_default_currency"),
      col("advertiser_default_exchange_rate"),
      col("member_currency"),
      col("member_exchange_rate"),
      col("billing_currency"),
      col("billing_exchange_rate"),
      col("fx_rate_snapshot_id"),
      col("bidder_seat_id"),
      col("region_id")
    )

}
