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

object Reformat_TRAN_Router_Reformatter_RowDistributorj0 {

  def apply(
    context: Context,
    in0:     DataFrame,
    in1:     DataFrame,
    in2:     DataFrame,
    in3:     DataFrame
  ): DataFrame =
    in0
      .as("in0")
      .join(
        in1.as("in1"),
        col("in0.publisher_id").cast(IntegerType) === col("in1.publisher_id"),
        "left_outer"
      )
      .join(
        in2.as("in2"),
        col("in0.advertiser_id").cast(IntegerType) === col("in2.advertiser_id"),
        "left_outer"
      )
      .join(in3.as("in3"),
            col("in0.campaign_group_id").cast(IntegerType) === col(
              "in3.campaign_group_id"
            ),
            "left_outer"
      )
      .select(
        when(
          is_not_null(col("in1.publisher_id")),
          struct(
            col("in1.seller_member_id").as("seller_member_id"),
            col("in1.publisher_id").as("publisher_id"),
            col("in1.site_id").as("site_id"),
            col("in1.tag_id").as("tag_id")
          )
        ).as("_member_id_by_publisher_id_LOOKUP"),
        when(
          is_not_null(col("in2.advertiser_id")),
          struct(
            col("in2.buyer_member_id").as("buyer_member_id"),
            col("in2.advertiser_id").as("advertiser_id"),
            col("in2.campaign_group_id").as("campaign_group_id"),
            col("in2.campaign_id").as("campaign_id")
          )
        ).as("_member_id_by_advertiser_id_LOOKUP"),
        when(
          is_not_null(col("in3.campaign_group_id")),
          struct(
            col("in3.buyer_member_id").as("buyer_member_id"),
            col("in3.advertiser_id").as("advertiser_id"),
            col("in3.campaign_group_id").as("campaign_group_id"),
            col("in3.campaign_id").as("campaign_id")
          )
        ).as("_advertiser_id_by_campaign_group_id_LOOKUP"),
        col("in0.auction_id_64").as("auction_id_64"),
        col("in0.date_time").as("date_time"),
        col("in0.is_delivered").as("is_delivered"),
        col("in0.is_dw").as("is_dw"),
        col("in0.seller_member_id").as("seller_member_id"),
        col("in0.buyer_member_id").as("buyer_member_id"),
        col("in0.member_id").as("member_id"),
        col("in0.publisher_id").as("publisher_id"),
        col("in0.site_id").as("site_id"),
        col("in0.tag_id").as("tag_id"),
        col("in0.advertiser_id").as("advertiser_id"),
        col("in0.campaign_group_id").as("campaign_group_id"),
        col("in0.campaign_id").as("campaign_id"),
        col("in0.insertion_order_id").as("insertion_order_id"),
        col("in0.imp_type").as("imp_type"),
        col("in0.is_transactable").as("is_transactable"),
        col("in0.is_transacted_previously").as("is_transacted_previously"),
        col("in0.is_deferred_impression").as("is_deferred_impression"),
        col("in0.has_null_bid").as("has_null_bid"),
        col("in0.additional_clearing_events").as("additional_clearing_events"),
        col("in0.log_impbus_impressions").as("log_impbus_impressions"),
        col("in0.log_impbus_preempt_count").as("log_impbus_preempt_count"),
        col("in0.log_impbus_preempt").as("log_impbus_preempt"),
        col("in0.log_impbus_preempt_dup").as("log_impbus_preempt_dup"),
        col("in0.log_impbus_impressions_pricing_count")
          .as("log_impbus_impressions_pricing_count"),
        col("in0.log_impbus_impressions_pricing")
          .as("log_impbus_impressions_pricing"),
        col("in0.log_impbus_impressions_pricing_dup")
          .as("log_impbus_impressions_pricing_dup"),
        col("in0.log_impbus_view").as("log_impbus_view"),
        col("in0.log_impbus_auction_event").as("log_impbus_auction_event"),
        col("in0.log_dw_bid_count").as("log_dw_bid_count"),
        col("in0.log_dw_bid").as("log_dw_bid"),
        col("in0.log_dw_bid_last").as("log_dw_bid_last"),
        col("in0.log_dw_bid_deal").as("log_dw_bid_deal"),
        col("in0.log_dw_bid_curator").as("log_dw_bid_curator"),
        col("in0.log_dw_view").as("log_dw_view"),
        col("in0.video_slot").as("video_slot")
      )

}
