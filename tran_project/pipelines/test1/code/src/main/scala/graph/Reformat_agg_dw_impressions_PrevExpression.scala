package graph

import io.prophecy.libs._
import udfs.PipelineInitCode._
import udfs.UDFs._
import udfs.ColumnFunctions._
import config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Reformat_agg_dw_impressions_PrevExpression {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(
      col("_sup_ip_range_LOOKUP_COUNT"),
      col("_sup_common_deal_LOOKUP1"),
      col("_sup_common_deal_LOOKUP"),
      col("_sup_bidder_campaign_LOOKUP"),
      col("video_slot"),
      col("log_impbus_impressions_pricing_dup"),
      col("log_dw_bid_count"),
      col("is_transactable"),
      col("insertion_order_id"),
      col("advertiser_id"),
      col("log_impbus_impressions_pricing"),
      col("site_id"),
      col("log_impbus_impressions_pricing_count"),
      col("log_impbus_impressions"),
      col("is_deferred_impression"),
      col("buyer_member_id"),
      col("is_transacted_previously"),
      col("log_dw_view"),
      col("campaign_group_id"),
      col("imp_type"),
      col("tag_id"),
      col("log_impbus_preempt"),
      col("log_impbus_preempt_dup"),
      col("campaign_id"),
      col("is_dw"),
      col("log_dw_bid_curator"),
      col("log_impbus_auction_event"),
      col("has_null_bid"),
      col("publisher_id"),
      col("is_delivered"),
      col("log_impbus_preempt_count"),
      col("seller_member_id"),
      col("log_dw_bid_deal"),
      col("date_time"),
      col("log_impbus_view"),
      col("member_id"),
      col("additional_clearing_events"),
      col("log_dw_bid_last"),
      col("log_dw_bid"),
      col("auction_id_64"),
      f_create_agg_dw_impressions(
        col("_sup_ip_range_LOOKUP_COUNT"),
        col("_sup_common_deal_LOOKUP"),
        col("_sup_common_deal_LOOKUP1"),
        col("_sup_bidder_campaign_LOOKUP"),
        col("_f_view_detection_enabled_var"),
        col("f_transaction_event_type_id_87067_var"),
        col("_f_find_personal_identifier_var"),
        col("_f_viewdef_definition_id_var"),
        col("_f_is_buy_side_var"),
        col("_f_has_transacted_var"),
        col("f_transaction_event_87047_var"),
        col("f_transaction_event_87057_var"),
        col("f_preempt_over_impression_95337_var"),
        col("f_transaction_event_type_id_87077_var"),
        col("f_preempt_over_impression_94298_var"),
        col("f_preempt_over_impression_88439_var"),
        col("f_preempt_over_impression_88639_var"),
        col("_f_is_default_or_error_imp_var"),
        col("_f_is_error_imp_var"),
        col("should_process_views_var")
      ).as("in_f_create_agg_dw_impressions")
    )

}
