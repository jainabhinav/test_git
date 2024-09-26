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

object Reformat_log_impbus_impressions_pricing {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(
      col(
        "in_f_convert_log_impbus_imptracker_to_log_impbus_impressions_pricing"
      ).getField("date_time").cast(LongType).as("date_time"),
      col(
        "in_f_convert_log_impbus_imptracker_to_log_impbus_impressions_pricing"
      ).getField("auction_id_64").cast(LongType).as("auction_id_64"),
      col(
        "in_f_convert_log_impbus_imptracker_to_log_impbus_impressions_pricing"
      ).getField("buyer_charges").as("buyer_charges"),
      col(
        "in_f_convert_log_impbus_imptracker_to_log_impbus_impressions_pricing"
      ).getField("seller_charges").as("seller_charges"),
      col(
        "in_f_convert_log_impbus_imptracker_to_log_impbus_impressions_pricing"
      ).getField("buyer_spend").cast(DoubleType).as("buyer_spend"),
      col(
        "in_f_convert_log_impbus_imptracker_to_log_impbus_impressions_pricing"
      ).getField("seller_revenue").cast(DoubleType).as("seller_revenue"),
      col(
        "in_f_convert_log_impbus_imptracker_to_log_impbus_impressions_pricing"
      ).getField("rate_card_auction_type")
        .cast(IntegerType)
        .as("rate_card_auction_type"),
      col(
        "in_f_convert_log_impbus_imptracker_to_log_impbus_impressions_pricing"
      ).getField("rate_card_media_type")
        .cast(IntegerType)
        .as("rate_card_media_type"),
      col(
        "in_f_convert_log_impbus_imptracker_to_log_impbus_impressions_pricing"
      ).getField("direct_clear").cast(BooleanType).as("direct_clear"),
      col(
        "in_f_convert_log_impbus_imptracker_to_log_impbus_impressions_pricing"
      ).getField("auction_timestamp").cast(LongType).as("auction_timestamp"),
      col(
        "in_f_convert_log_impbus_imptracker_to_log_impbus_impressions_pricing"
      ).getField("instance_id").cast(IntegerType).as("instance_id"),
      col(
        "in_f_convert_log_impbus_imptracker_to_log_impbus_impressions_pricing"
      ).getField("two_phase_reduction_applied")
        .cast(BooleanType)
        .as("two_phase_reduction_applied"),
      col(
        "in_f_convert_log_impbus_imptracker_to_log_impbus_impressions_pricing"
      ).getField("trade_agreement_id")
        .cast(IntegerType)
        .as("trade_agreement_id"),
      col(
        "in_f_convert_log_impbus_imptracker_to_log_impbus_impressions_pricing"
      ).getField("log_timestamp").cast(LongType).as("log_timestamp"),
      col(
        "in_f_convert_log_impbus_imptracker_to_log_impbus_impressions_pricing"
      ).getField("trade_agreement_info").as("trade_agreement_info"),
      col(
        "in_f_convert_log_impbus_imptracker_to_log_impbus_impressions_pricing"
      ).getField("is_buy_it_now").cast(BooleanType).as("is_buy_it_now"),
      col(
        "in_f_convert_log_impbus_imptracker_to_log_impbus_impressions_pricing"
      ).getField("net_buyer_spend").cast(DoubleType).as("net_buyer_spend"),
      col(
        "in_f_convert_log_impbus_imptracker_to_log_impbus_impressions_pricing"
      ).getField("impression_event_pricing").as("impression_event_pricing"),
      col(
        "in_f_convert_log_impbus_imptracker_to_log_impbus_impressions_pricing"
      ).getField("counterparty_ruleset_type")
        .cast(IntegerType)
        .as("counterparty_ruleset_type"),
      col(
        "in_f_convert_log_impbus_imptracker_to_log_impbus_impressions_pricing"
      ).getField("estimated_audience_imps")
        .cast(FloatType)
        .as("estimated_audience_imps"),
      col(
        "in_f_convert_log_impbus_imptracker_to_log_impbus_impressions_pricing"
      ).getField("audience_imps").cast(FloatType).as("audience_imps")
    )

}
