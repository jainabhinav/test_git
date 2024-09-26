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

object Reformat_agg_dw_impressions_args_PreCompute {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(
      col("_sup_ip_range_LOOKUP_COUNT"),
      col("_sup_common_deal_LOOKUP"),
      col("_sup_common_deal_LOOKUP1"),
      col("_sup_bidder_campaign_LOOKUP"),
      col("auction_id_64"),
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
      col("video_slot"),
      f_view_detection_enabled(
        col("log_impbus_impressions.view_detection_enabled").cast(IntegerType),
        col("log_impbus_preempt.view_detection_enabled").cast(IntegerType)
      ).as("_f_view_detection_enabled_var"),
      f_transaction_event_type_id(
        col("log_impbus_impressions.seller_transaction_def"),
        col("log_impbus_preempt.seller_transaction_def")
      ).as("f_transaction_event_type_id_87067_var"),
      f_find_personal_identifier(
        lit(9920),
        col("log_impbus_impressions.personal_identifiers")
      ).as("_f_find_personal_identifier_var"),
      f_viewdef_definition_id(
        col("log_impbus_impressions.viewdef_definition_id_buyer_member").cast(
          IntegerType
        ),
        col("log_impbus_preempt.viewdef_definition_id_buyer_member").cast(
          IntegerType
        ),
        col("log_impbus_view.viewdef_definition_id").cast(IntegerType)
      ).as("_f_viewdef_definition_id_var"),
      f_is_buy_side(col("log_dw_bid"),
                    col("buyer_member_id").cast(IntegerType),
                    col("member_id").cast(IntegerType)
      ).as("_f_is_buy_side_var"),
      f_has_transacted(col("log_impbus_impressions.buyer_transaction_def"),
                       col("log_impbus_preempt.buyer_transaction_def")
      ).as("_f_has_transacted_var"),
      f_transaction_event(col("log_impbus_impressions.seller_transaction_def"),
                          col("log_impbus_preempt.seller_transaction_def")
      ).as("f_transaction_event_87047_var"),
      f_transaction_event(col("log_impbus_impressions.buyer_transaction_def"),
                          col("log_impbus_preempt.buyer_transaction_def")
      ).as("f_transaction_event_87057_var"),
      f_preempt_over_impression(
        col("log_impbus_impressions.creative_media_subtype_id").cast(
          IntegerType
        ),
        col("log_impbus_preempt.creative_media_subtype_id").cast(IntegerType)
      ).as("f_preempt_over_impression_95337_var"),
      f_transaction_event_type_id(
        col("log_impbus_impressions.buyer_transaction_def"),
        col("log_impbus_preempt.buyer_transaction_def")
      ).as("f_transaction_event_type_id_87077_var"),
      f_preempt_over_impression(
        col("log_impbus_impressions.external_campaign_id"),
        col("log_impbus_preempt.external_campaign_id")
      ).as("f_preempt_over_impression_94298_var"),
      f_preempt_over_impression(
        col("log_impbus_impressions.deal_type").cast(IntegerType),
        col("log_impbus_preempt.deal_type").cast(IntegerType)
      ).as("f_preempt_over_impression_88439_var"),
      f_preempt_over_impression(
        col("log_impbus_impressions.creative_id").cast(IntegerType),
        col("log_impbus_preempt.creative_id").cast(IntegerType)
      ).as("f_preempt_over_impression_88639_var"),
      f_is_default_or_error_imp(
        coalesce(col("imp_type").cast(IntegerType), lit(1))
      ).as("_f_is_default_or_error_imp_var"),
      f_is_error_imp(coalesce(col("imp_type").cast(IntegerType), lit(1)))
        .as("_f_is_error_imp_var"),
      f_should_process_views(
        col("log_dw_view"),
        f_transaction_event_type_id(
          col("log_impbus_impressions.seller_transaction_def"),
          col("log_impbus_preempt.seller_transaction_def")
        ),
        f_transaction_event_type_id(
          col("log_impbus_impressions.buyer_transaction_def"),
          col("log_impbus_preempt.buyer_transaction_def")
        )
      ).cast(IntegerType).as("should_process_views_var")
    )

}
