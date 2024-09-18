package io.prophecy.pipelines.first_agg_platform_video_analytics.graph.Main_Graph

import io.prophecy.libs._
import io.prophecy.pipelines.first_agg_platform_video_analytics.graph.Main_Graph.config.Context
import io.prophecy.pipelines.first_agg_platform_video_analytics.udfs.UDFs._
import io.prophecy.pipelines.first_agg_platform_video_analytics.udfs.ColumnFunctions._
import io.prophecy.pipelines.first_agg_platform_video_analytics.udfs.PipelineInitCode._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object join_and_project_auction_data {
  def apply(context: Context, in0: DataFrame, in2: DataFrame): DataFrame = {
    val spark = context.spark
    val Config = context.config
    import org.apache.spark.storage.StorageLevel
    
    // Persist the DataFrame to disk only
    in0.persist(StorageLevel.DISK_ONLY)
    
    // Trigger an action to materialize the persistence
    in0.count() // or any other action like show(), collect(), etc.
    
    val out0 = in0
      .as("in0")
      .join(
        in2.as("in2"),
        col("in0.auction_id_64") === col("in2.auction_id_64"),
        "left_outer"
      )
      .select(
        col("in0.auction_id_64").as("auction_id_64"),
        col("in0.date_time").as("date_time"),
        col("in0.agg_platform_video_requests").as("agg_platform_video_requests"),
        col("in0.agg_dw_video_events").as("agg_dw_video_events"),
        when(
          is_not_null(col("in2.auction_id_64")).cast(BooleanType),
          struct(
            col("in2.date_time").as("date_time"),
            col("in2.auction_id_64").as("auction_id_64"),
            col("in2.seller_member_id").as("seller_member_id"),
            col("in2.buyer_member_id").as("buyer_member_id"),
            col("in2.is_delivered").as("is_delivered"),
            col("in2.cleared_direct").as("cleared_direct"),
            col("in2.transaction_type").as("transaction_type"),
            col("in2.advertiser_id").as("advertiser_id"),
            col("in2.insertion_order_id").as("insertion_order_id"),
            col("in2.campaign_group_id").as("campaign_group_id"),
            col("in2.campaign_id").as("campaign_id"),
            col("in2.publisher_id").as("publisher_id"),
            col("in2.bidder_id").as("bidder_id"),
            col("in2.creative_id").as("creative_id"),
            col("in2.brand_id").as("brand_id"),
            col("in2.is_dw_buyer").as("is_dw_buyer"),
            col("in2.is_dw_seller").as("is_dw_seller"),
            col("in2.has_dw_buy").as("has_dw_buy"),
            col("in2.has_dw_sell").as("has_dw_sell"),
            col("in2.booked_revenue_dollars").as("booked_revenue_dollars"),
            col("in2.buyer_media_cost_cpm").as("buyer_media_cost_cpm"),
            col("in2.auction_service_fees").as("auction_service_fees"),
            col("in2.auction_service_deduction").as("auction_service_deduction"),
            col("in2.clear_fees").as("clear_fees"),
            col("in2.creative_overage_fees").as("creative_overage_fees"),
            col("in2.seller_media_cost_cpm").as("seller_media_cost_cpm"),
            col("in2.seller_revenue").as("seller_revenue"),
            col("in2.seller_deduction").as("seller_deduction"),
            col("in2.discrepancy_allowance").as("discrepancy_allowance"),
            col("in2.commission_cpm").as("commission_cpm"),
            col("in2.commission_revshare").as("commission_revshare"),
            col("in2.serving_fees_cpm").as("serving_fees_cpm"),
            col("in2.serving_fees_revshare").as("serving_fees_revshare"),
            col("in2.advertiser_currency").as("advertiser_currency"),
            col("in2.advertiser_exchange_rate").as("advertiser_exchange_rate"),
            col("in2.publisher_currency").as("publisher_currency"),
            col("in2.publisher_exchange_rate").as("publisher_exchange_rate"),
            col("in2.deal_id").as("deal_id"),
            col("in2.geo_country").as("geo_country"),
            col("in2.imp_blacklist_or_fraud").as("imp_blacklist_or_fraud"),
            col("in2.pricing_media_type").as("pricing_media_type"),
            col("in2.imp_ignored").as("imp_ignored"),
            col("in2.is_prebid").as("is_prebid"),
            col("in2.is_unit_of_buyer_trx").as("is_unit_of_buyer_trx"),
            col("in2.is_unit_of_seller_trx").as("is_unit_of_seller_trx"),
            col("in2.custom_model_id").as("custom_model_id"),
            col("in2.media_type").as("media_type"),
            col("in2.device_type").as("device_type"),
            col("in2.two_phase_reduction_applied")
              .as("two_phase_reduction_applied"),
            col("in2.buyer_charges").as("buyer_charges"),
            col("in2.seller_charges").as("seller_charges"),
            col("in2.seller_transaction_def").as("seller_transaction_def"),
            col("in2.buyer_transaction_def").as("buyer_transaction_def"),
            col("in2.view_measurable").as("view_measurable"),
            col("in2.viewable").as("viewable"),
            col("in2.imp_type").as("imp_type"),
            col("in2.viewdef_definition_id").as("viewdef_definition_id"),
            col("in2.viewdef_viewed_imps").as("viewdef_viewed_imps"),
            col("in2.imp_rejecter_do_auction").as("imp_rejecter_do_auction"),
            col("in2.buyer_trx_event_id").as("buyer_trx_event_id"),
            col("in2.seller_trx_event_id").as("seller_trx_event_id"),
            col("in2.inventory_url_id").as("inventory_url_id"),
            col("in2.mobile_app_instance_id").as("mobile_app_instance_id"),
            col("in2.user_id_64").as("user_id_64"),
            col("in2.split_id").as("split_id"),
            col("in2.campaign_group_type_id").as("campaign_group_type_id"),
            col("in2.advertiser_default_currency")
              .as("advertiser_default_currency"),
            col("in2.advertiser_default_exchange_rate")
              .as("advertiser_default_exchange_rate"),
            col("in2.member_currency").as("member_currency"),
            col("in2.member_exchange_rate").as("member_exchange_rate"),
            col("in2.billing_currency").as("billing_currency"),
            col("in2.billing_exchange_rate").as("billing_exchange_rate"),
            col("in2.fx_rate_snapshot_id").as("fx_rate_snapshot_id"),
            col("in2.bidder_seat_id").as("bidder_seat_id"),
            col("in2.region_id").as("region_id")
          )
        )
          .as("agg_platform_video_impressions")
      )
    out0
  }

}
