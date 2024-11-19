package graph.Router_Reformatter

import io.prophecy.libs._
import udfs.PipelineInitCode._
import udfs.UDFs._
import udfs.ColumnFunctions._
import graph.Router_Reformatter.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Reformat_1 {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(
      col("auction_id_64")
        .cast(LongType)
        .as("auction_id_64")
        .as("auction_id_64"),
      r_is_quarantined(context).as("r_is_quarantined"),
      r_is_dw(context).as("r_is_dw"),
      r_is_curated(context).as("r_is_curated"),
      r_is_transacted(context).as("r_is_transacted"),
      col("_member_id_by_publisher_id_LOOKUP"),
      col("_member_id_by_advertiser_id_LOOKUP"),
      col("_advertiser_id_by_campaign_group_id_LOOKUP"),
      col("date_time"),
      col("is_delivered"),
      col("is_dw"),
      col("seller_member_id"),
      col("buyer_member_id"),
      col("member_id"),
      col("publisher_id"),
      col("site_id"),
      col("tag_id"),
      col("advertiser_id"),
      col("campaign_group_id"),
      col("campaign_id"),
      col("insertion_order_id"),
      col("imp_type"),
      col("is_transactable"),
      col("is_transacted_previously"),
      col("is_deferred_impression"),
      col("has_null_bid"),
      col("additional_clearing_events"),
      col("log_impbus_impressions"),
      col("log_impbus_preempt_count"),
      col("log_impbus_preempt"),
      col("log_impbus_preempt_dup"),
      col("log_impbus_impressions_pricing_count"),
      col("log_impbus_impressions_pricing"),
      col("log_impbus_impressions_pricing_dup"),
      col("log_impbus_view"),
      col("log_impbus_auction_event"),
      col("log_dw_bid_count"),
      col("log_dw_bid"),
      col("log_dw_bid_last"),
      col("log_dw_bid_deal"),
      col("log_dw_bid_curator"),
      col("log_dw_view"),
      col("video_slot")
    )

  def r_is_curated(context: Context) = {
    val spark  = context.spark
    val Config = context.config
    f_router_is_curated(
      struct(
        col("auction_id_64").cast(LongType).as("auction_id_64"),
        col("date_time").cast(LongType).as("date_time"),
        col("is_delivered").cast(IntegerType).as("is_delivered"),
        col("is_dw").cast(IntegerType).as("is_dw"),
        col("seller_member_id").cast(IntegerType).as("seller_member_id"),
        col("buyer_member_id").cast(IntegerType).as("buyer_member_id"),
        col("member_id").cast(IntegerType).as("member_id"),
        col("publisher_id").cast(IntegerType).as("publisher_id"),
        col("site_id").cast(IntegerType).as("site_id"),
        col("tag_id").cast(IntegerType).as("tag_id"),
        col("advertiser_id").cast(IntegerType).as("advertiser_id"),
        col("campaign_group_id").cast(IntegerType).as("campaign_group_id"),
        col("campaign_id").cast(IntegerType).as("campaign_id"),
        col("insertion_order_id").cast(IntegerType).as("insertion_order_id"),
        col("imp_type").cast(IntegerType).as("imp_type"),
        col("is_transactable"),
        col("is_transacted_previously"),
        col("is_deferred_impression"),
        col("has_null_bid"),
        col("additional_clearing_events"),
        col("log_impbus_impressions"),
        col("log_impbus_preempt_count")
          .cast(IntegerType)
          .as("log_impbus_preempt_count"),
        col("log_impbus_preempt"),
        col("log_impbus_preempt_dup"),
        col("log_impbus_impressions_pricing_count")
          .cast(IntegerType)
          .as("log_impbus_impressions_pricing_count"),
        col("log_impbus_impressions_pricing"),
        col("log_impbus_impressions_pricing_dup"),
        col("log_impbus_view"),
        col("log_impbus_auction_event"),
        col("log_dw_bid_count").cast(IntegerType).as("log_dw_bid_count"),
        col("log_dw_bid"),
        col("log_dw_bid_last"),
        col("log_dw_bid_deal"),
        col("log_dw_bid_curator"),
        col("log_dw_view"),
        col("video_slot")
      )
    )
  }

  def r_is_quarantined(context: Context) = {
    val spark  = context.spark
    val Config = context.config
    f_router_is_quarantined(
      struct(
        col("auction_id_64").cast(LongType).as("auction_id_64"),
        col("date_time").cast(LongType).as("date_time"),
        col("is_delivered").cast(IntegerType).as("is_delivered"),
        col("is_dw").cast(IntegerType).as("is_dw"),
        col("seller_member_id").cast(IntegerType).as("seller_member_id"),
        col("buyer_member_id").cast(IntegerType).as("buyer_member_id"),
        col("member_id").cast(IntegerType).as("member_id"),
        col("publisher_id").cast(IntegerType).as("publisher_id"),
        col("site_id").cast(IntegerType).as("site_id"),
        col("tag_id").cast(IntegerType).as("tag_id"),
        col("advertiser_id").cast(IntegerType).as("advertiser_id"),
        col("campaign_group_id").cast(IntegerType).as("campaign_group_id"),
        col("campaign_id").cast(IntegerType).as("campaign_id"),
        col("insertion_order_id").cast(IntegerType).as("insertion_order_id"),
        col("imp_type").cast(IntegerType).as("imp_type"),
        col("is_transactable"),
        col("is_transacted_previously"),
        col("is_deferred_impression"),
        col("has_null_bid"),
        col("additional_clearing_events"),
        col("log_impbus_impressions"),
        col("log_impbus_preempt_count")
          .cast(IntegerType)
          .as("log_impbus_preempt_count"),
        col("log_impbus_preempt"),
        col("log_impbus_preempt_dup"),
        col("log_impbus_impressions_pricing_count")
          .cast(IntegerType)
          .as("log_impbus_impressions_pricing_count"),
        col("log_impbus_impressions_pricing"),
        col("log_impbus_impressions_pricing_dup"),
        col("log_impbus_view"),
        col("log_impbus_auction_event"),
        col("log_dw_bid_count").cast(IntegerType).as("log_dw_bid_count"),
        col("log_dw_bid"),
        col("log_dw_bid_last"),
        col("log_dw_bid_deal"),
        col("log_dw_bid_curator"),
        col("log_dw_view"),
        col("video_slot")
      ),
      col("_advertiser_id_by_campaign_group_id_LOOKUP"),
      col("_member_id_by_advertiser_id_LOOKUP"),
      col("_member_id_by_publisher_id_LOOKUP")
    )
  }

  def r_is_transacted(context: Context) = {
    val spark  = context.spark
    val Config = context.config
    f_router_is_transacted(
      struct(
        col("auction_id_64").cast(LongType).as("auction_id_64"),
        col("date_time").cast(LongType).as("date_time"),
        col("is_delivered").cast(IntegerType).as("is_delivered"),
        col("is_dw").cast(IntegerType).as("is_dw"),
        col("seller_member_id").cast(IntegerType).as("seller_member_id"),
        col("buyer_member_id").cast(IntegerType).as("buyer_member_id"),
        col("member_id").cast(IntegerType).as("member_id"),
        col("publisher_id").cast(IntegerType).as("publisher_id"),
        col("site_id").cast(IntegerType).as("site_id"),
        col("tag_id").cast(IntegerType).as("tag_id"),
        col("advertiser_id").cast(IntegerType).as("advertiser_id"),
        col("campaign_group_id").cast(IntegerType).as("campaign_group_id"),
        col("campaign_id").cast(IntegerType).as("campaign_id"),
        col("insertion_order_id").cast(IntegerType).as("insertion_order_id"),
        col("imp_type").cast(IntegerType).as("imp_type"),
        col("is_transactable"),
        col("is_transacted_previously"),
        col("is_deferred_impression"),
        col("has_null_bid"),
        col("additional_clearing_events"),
        col("log_impbus_impressions"),
        col("log_impbus_preempt_count")
          .cast(IntegerType)
          .as("log_impbus_preempt_count"),
        col("log_impbus_preempt"),
        col("log_impbus_preempt_dup"),
        col("log_impbus_impressions_pricing_count")
          .cast(IntegerType)
          .as("log_impbus_impressions_pricing_count"),
        col("log_impbus_impressions_pricing"),
        col("log_impbus_impressions_pricing_dup"),
        col("log_impbus_view"),
        col("log_impbus_auction_event"),
        col("log_dw_bid_count").cast(IntegerType).as("log_dw_bid_count"),
        col("log_dw_bid"),
        col("log_dw_bid_last"),
        col("log_dw_bid_deal"),
        col("log_dw_bid_curator"),
        col("log_dw_view"),
        col("video_slot")
      )
    )
  }

  def r_is_dw(context: Context) = {
    val spark  = context.spark
    val Config = context.config
    f_router_is_dw(
      struct(
        col("auction_id_64").cast(LongType).as("auction_id_64"),
        col("date_time").cast(LongType).as("date_time"),
        col("is_delivered").cast(IntegerType).as("is_delivered"),
        col("is_dw").cast(IntegerType).as("is_dw"),
        col("seller_member_id").cast(IntegerType).as("seller_member_id"),
        col("buyer_member_id").cast(IntegerType).as("buyer_member_id"),
        col("member_id").cast(IntegerType).as("member_id"),
        col("publisher_id").cast(IntegerType).as("publisher_id"),
        col("site_id").cast(IntegerType).as("site_id"),
        col("tag_id").cast(IntegerType).as("tag_id"),
        col("advertiser_id").cast(IntegerType).as("advertiser_id"),
        col("campaign_group_id").cast(IntegerType).as("campaign_group_id"),
        col("campaign_id").cast(IntegerType).as("campaign_id"),
        col("insertion_order_id").cast(IntegerType).as("insertion_order_id"),
        col("imp_type").cast(IntegerType).as("imp_type"),
        col("is_transactable"),
        col("is_transacted_previously"),
        col("is_deferred_impression"),
        col("has_null_bid"),
        col("additional_clearing_events"),
        col("log_impbus_impressions"),
        col("log_impbus_preempt_count")
          .cast(IntegerType)
          .as("log_impbus_preempt_count"),
        col("log_impbus_preempt"),
        col("log_impbus_preempt_dup"),
        col("log_impbus_impressions_pricing_count")
          .cast(IntegerType)
          .as("log_impbus_impressions_pricing_count"),
        col("log_impbus_impressions_pricing"),
        col("log_impbus_impressions_pricing_dup"),
        col("log_impbus_view"),
        col("log_impbus_auction_event"),
        col("log_dw_bid_count").cast(IntegerType).as("log_dw_bid_count"),
        col("log_dw_bid"),
        col("log_dw_bid_last"),
        col("log_dw_bid_deal"),
        col("log_dw_bid_curator"),
        col("log_dw_view"),
        col("video_slot")
      )
    )
  }

}
