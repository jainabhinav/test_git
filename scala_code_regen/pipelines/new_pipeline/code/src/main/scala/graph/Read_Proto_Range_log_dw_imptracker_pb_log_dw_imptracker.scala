package graph

import io.prophecy.libs._
import config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Read_Proto_Range_log_dw_imptracker_pb_log_dw_imptracker {

  def apply(context: Context): DataFrame = {
    val spark = context.spark
    import _root_.io.prophecy.abinitio.dml.DMLSchema.parse
    import _root_.io.prophecy.libs.FFSchemaRecord
    import _root_.io.prophecy.libs.FixedFormatSchemaImplicits._
    import play.api.libs.json.Json
    var df: DataFrame = spark.emptyDataFrame
    try {
      var reader = spark.read
        .option(
          "schema",
          Some("""type fixed32_t = unsigned little endian integer(4);
type fixed64_t = unsigned little endian integer(8);
type sfixed32_t = little endian integer(4);
type sfixed64_t = little endian integer(8);
type real32_t = little endian ieee real(4);
type real64_t = little endian ieee real(8);
type bool_t = unsigned integer(1);
type string_t = utf8 string(unsigned little endian integer(4));
type bytes_t = void(unsigned little endian integer(4));
type enum_t = little endian integer(4);
type length_t = unsigned little endian integer(4);
type revenue_info =
record
  fixed64_t total_partner_fees_microcents = NULL;
  real64_t booked_revenue_dollars = NULL;
  real64_t booked_revenue_adv_curr = NULL;
  fixed64_t total_data_costs_microcents = NULL;
  sfixed64_t total_profit_microcents = NULL;
  fixed64_t total_segment_data_costs_microcents = NULL;
  fixed64_t total_feature_costs_microcents = NULL;
end;
type log_dw_imptracker =
record
  fixed64_t date_time;
  fixed64_t auction_id_64;
  fixed64_t user_id_64 = NULL;
  sfixed32_t tracker_id = NULL;
  sfixed32_t member_id = NULL;
  sfixed32_t advertiser_id = NULL;
  sfixed32_t campaign_group_id = NULL;
  sfixed32_t can_convert = NULL;
  sfixed32_t insertion_order_id = NULL;
  real64_t revenue_value = NULL;
  real64_t revenue_value_adv_curr = NULL;
  real64_t commission_cpm = NULL;
  real64_t commission_revshare = NULL;
  string_t pricing_type = NULL;
  string_t advertiser_currency = NULL;
  real64_t advertiser_exchange_rate = NULL;
  real64_t media_buy_cost = NULL;
  real64_t media_buy_rev_share_pct = NULL;
  enum_t revenue_type = NULL;
  enum_t payment_type = NULL;
  revenue_info revenue_info = NULL;
end;
constant
record
  string(int) name;
  record
    string(int) name;
    int number;
    unsigned integer(1) type_code;
    int message_type;
    unsigned integer(1) optional;
  end[int] field_infos;
end[int] log_dw_imptracker_message_types =
[vector
  [record
    name "revenue_info"
    field_infos [vector
                  [record name "total_partner_fees_microcents" number 1 type_code 3 message_type -1 optional 1],
                  [record name "booked_revenue_dollars" number 2 type_code 1 message_type -1 optional 1],
                  [record name "booked_revenue_adv_curr" number 3 type_code 1 message_type -1 optional 1],
                  [record name "total_data_costs_microcents" number 4 type_code 3 message_type -1 optional 1],
                  [record name "total_profit_microcents" number 5 type_code 3 message_type -1 optional 1],
                  [record name "total_segment_data_costs_microcents" number 6 type_code 3 message_type -1 optional 1],
                  [record name "total_feature_costs_microcents" number 7 type_code 3 message_type -1 optional 1]]],
  [record
    name "log_dw_imptracker"
    field_infos [vector
                  [record name "date_time" number 1 type_code 1 message_type -1 optional 0],
                  [record name "auction_id_64" number 2 type_code 1 message_type -1 optional 0],
                  [record name "user_id_64" number 3 type_code 1 message_type -1 optional 1],
                  [record name "tracker_id" number 4 type_code 4 message_type -1 optional 1],
                  [record name "member_id" number 5 type_code 4 message_type -1 optional 1],
                  [record name "advertiser_id" number 6 type_code 4 message_type -1 optional 1],
                  [record name "campaign_group_id" number 7 type_code 4 message_type -1 optional 1],
                  [record name "can_convert" number 8 type_code 4 message_type -1 optional 1],
                  [record name "insertion_order_id" number 9 type_code 4 message_type -1 optional 1],
                  [record name "revenue_value" number 10 type_code 1 message_type -1 optional 1],
                  [record name "revenue_value_adv_curr" number 11 type_code 1 message_type -1 optional 1],
                  [record name "commission_cpm" number 12 type_code 1 message_type -1 optional 1],
                  [record name "commission_revshare" number 13 type_code 1 message_type -1 optional 1],
                  [record name "pricing_type" number 14 type_code 8 message_type -1 optional 1],
                  [record name "advertiser_currency" number 15 type_code 8 message_type -1 optional 1],
                  [record name "advertiser_exchange_rate" number 16 type_code 1 message_type -1 optional 1],
                  [record name "media_buy_cost" number 17 type_code 1 message_type -1 optional 1],
                  [record name "media_buy_rev_share_pct" number 18 type_code 1 message_type -1 optional 1],
                  [record name "revenue_type" number 19 type_code 4 message_type -1 optional 1],
                  [record name "payment_type" number 20 type_code 4 message_type -1 optional 1],
                  [record name "revenue_info" number 23 type_code 9 message_type 0 optional 1]]]];
metadata type = log_dw_imptracker;
metadata type = "log_dw_imptracker" ;""")
            .map(s => parse(s).asInstanceOf[FFSchemaRecord])
            .map(s => Json.stringify(Json.toJson(s)))
            .getOrElse("")
        )
        .format("io.prophecy.libs.FixedFileFormat")
      df = reader.load("NA")
    } catch {
      case e: Error =>
        println(s"Error occurred while reading dataframe: $e")
        throw new Exception(e.getMessage)
      case e: Throwable =>
        println(s"Throwable occurred while reading dataframe: $e")
        throw new Exception(e.getMessage)
    }
    df
  }

}
