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

object Reformat_TRAN_Router_ReformatterReformat_1j0 {

  def apply(
    context: Context,
    in0:     DataFrame,
    in1:     DataFrame,
    in2:     DataFrame,
    in3:     DataFrame
  ): DataFrame =
    in0
      .as("in0")
      .join(in1.as("in1"),
            col("in0.log_impbus_impressions.tag_id").cast(IntegerType) === col(
              "in1.id"
            ),
            "left_outer"
      )
      .join(in2.as("in2"),
            col("in0.log_impbus_preempt.curated_deal_id")
              .cast(IntegerType) === col("in2.id"),
            "left_outer"
      )
      .join(in3.as("in3"),
            col("in0.log_impbus_impressions.inventory_url_id")
              .cast(IntegerType) === col("in3.inventory_url_id"),
            "left_outer"
      )
      .select(
        when(
          is_not_null(col("in1.id")),
          struct(
            col("in1.id").as("id"),
            col("in1.supports_skippable").as("supports_skippable"),
            col("in1.max_duration_secs").as("max_duration_secs"),
            col("in1.max_ad_duration_secs").as("max_ad_duration_secs"),
            col("in1.maximum_number_ads").as("maximum_number_ads"),
            col("in1.start_delay_secs").as("start_delay_secs"),
            col("in1.playback_method").as("playback_method"),
            col("in1.video_context").as("video_context"),
            col("in1.is_mediated").as("is_mediated"),
            col("in1.skip_offset").as("skip_offset")
          )
        ).as("_sup_placement_video_attributes_pb_LOOKUP"),
        when(is_not_null(col("in2.id")),
             struct(col("in2.id").as("id"),
                    col("in2.member_id").as("member_id"),
                    col("in2.deal_type_id").as("deal_type_id")
             )
        ).as("_sup_common_deal_LOOKUP"),
        when(is_not_null(col("in3.inventory_url_id")),
             struct(col("in3.inventory_url_id").as("inventory_url_id"),
                    col("in3.inventory_url").as("inventory_url")
             )
        ).as("_inventory_url_by_id_LOOKUP"),
        f_get_transaction_event_pricing(
          col("in0.log_impbus_impressions_pricing.impression_event_pricing"),
          col("in0.log_impbus_auction_event.auction_event_pricing"),
          col("in0.log_impbus_impressions_pricing.buyer_charges"),
          col("in0.log_impbus_impressions_pricing.seller_charges"),
          f_should_process_views(
            col("in0.log_dw_view"),
            f_transaction_event(
              col("in0.log_impbus_impressions.seller_transaction_def"),
              col("in0.log_impbus_preempt.seller_transaction_def")
            ).cast(IntegerType),
            f_transaction_event(
              col("in0.log_impbus_impressions.buyer_transaction_def"),
              col("in0.log_impbus_preempt.buyer_transaction_def")
            ).cast(IntegerType)
          ).cast(IntegerType)
        ).as("f_get_transaction_event_pricing_var"),
        f_should_zero_seller_revenue(
          col("in0.log_dw_bid"),
          col("in0.imp_type").cast(IntegerType),
          col("in0.log_dw_bid.revenue_type").cast(IntegerType),
          col("in0.seller_member_id").cast(IntegerType),
          col(
            "in0.log_impbus_impressions_pricing.impression_event_pricing.seller_revenue_microcents"
          ).cast(LongType)
        ).as("f_should_zero_seller_revenue_var"),
        col("in0.*")
      )

}
