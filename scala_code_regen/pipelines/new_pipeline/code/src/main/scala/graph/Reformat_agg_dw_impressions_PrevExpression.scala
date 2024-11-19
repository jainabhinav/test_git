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
    in.withColumn(
      "in_f_create_agg_dw_impressions",
      f_create_agg_dw_impressions(
        col("_sup_ip_range_LOOKUP_COUNT"),
        col("_sup_bidder_member_sales_tax_rate_LOOKUP"),
        col("_sup_common_deal_LOOKUP"),
        col("_sup_bidder_member_sales_tax_rate_LOOKUP1"),
        col("_sup_common_deal_LOOKUP1"),
        col("_sup_bidder_campaign_LOOKUP"),
        f_view_detection_enabled(
          col("log_impbus_impressions.view_detection_enabled")
            .cast(IntegerType),
          col("log_impbus_preempt.view_detection_enabled").cast(IntegerType)
        ),
        f_transaction_event_type_id(
          col("log_impbus_impressions.seller_transaction_def"),
          col("log_impbus_preempt.seller_transaction_def")
        ),
        f_viewdef_definition_id(
          col("log_impbus_impressions.viewdef_definition_id_buyer_member")
            .cast(IntegerType),
          col("log_impbus_preempt.viewdef_definition_id_buyer_member")
            .cast(IntegerType),
          col("log_impbus_view.viewdef_definition_id").cast(IntegerType)
        ),
        f_is_buy_side(col("log_dw_bid"),
                      col("buyer_member_id").cast(IntegerType),
                      col("member_id").cast(IntegerType)
        ),
        f_has_transacted(col("log_impbus_impressions.buyer_transaction_def"),
                         col("log_impbus_preempt.buyer_transaction_def")
        ),
        f_transaction_event(
          col("log_impbus_impressions.seller_transaction_def"),
          col("log_impbus_preempt.seller_transaction_def")
        ),
        f_transaction_event(col("log_impbus_impressions.buyer_transaction_def"),
                            col("log_impbus_preempt.buyer_transaction_def")
        ),
        f_preempt_over_impression(
          col("log_impbus_impressions.creative_media_subtype_id")
            .cast(IntegerType),
          col("log_impbus_preempt.creative_media_subtype_id").cast(IntegerType)
        ),
        f_transaction_event_type_id(
          col("log_impbus_impressions.buyer_transaction_def"),
          col("log_impbus_preempt.buyer_transaction_def")
        ),
        f_preempt_over_impression(
          col("log_impbus_impressions.external_campaign_id"),
          col("log_impbus_preempt.external_campaign_id")
        ),
        f_preempt_over_impression(
          col("log_impbus_impressions.deal_type").cast(IntegerType),
          col("log_impbus_preempt.deal_type").cast(IntegerType)
        ),
        f_preempt_over_impression(
          col("log_impbus_impressions.creative_id").cast(IntegerType),
          col("log_impbus_preempt.creative_id").cast(IntegerType)
        )
      )
    )

}
