package graph

import io.prophecy.libs._
import config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Read_Proto_Range_log_impbus_preempt_deferred_pb_log_impbus_preempt_deferred_5 {

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
type log_product_ads =
record
  sfixed32_t product_feed_id = NULL;
  sfixed32_t item_selection_strategy_id = NULL;
  string_t product_uuid = NULL;
end;
type log_transaction_def =
record
  sfixed32_t transaction_event = NULL;
  sfixed32_t transaction_event_type_id = NULL;
end;
type log_impbus_preempt =
record
  fixed64_t date_time;
  fixed64_t auction_id_64;
  sfixed32_t imp_transacted = NULL;
  real64_t buyer_spend = NULL;
  real64_t seller_revenue = NULL;
  real64_t bidder_fees = NULL;
  sfixed32_t instance_id = NULL;
  sfixed32_t fold_position = NULL;
  real64_t seller_deduction = NULL;
  sfixed32_t buyer_member_id = NULL;
  sfixed32_t creative_id = NULL;
  sfixed32_t cleared_direct = NULL;
  string_t buyer_currency = NULL;
  real64_t buyer_exchange_rate = NULL;
  sfixed32_t width = NULL;
  sfixed32_t height = NULL;
  sfixed32_t brand_id = NULL;
  sfixed32_t creative_audit_status = NULL;
  sfixed32_t is_creative_hosted = NULL;
  sfixed32_t vp_expose_domains = NULL;
  sfixed32_t vp_expose_categories = NULL;
  sfixed32_t vp_expose_pubs = NULL;
  sfixed32_t vp_expose_tag = NULL;
  sfixed32_t bidder_id = NULL;
  sfixed32_t deal_id = NULL;
  enum_t imp_type = NULL;
  sfixed32_t is_dw = NULL;
  sfixed64_t vp_bitmap = NULL;
  sfixed32_t ttl = NULL;
  sfixed32_t view_detection_enabled = NULL;
  sfixed32_t media_type = NULL;
  fixed64_t auction_timestamp;
  sfixed32_t spend_protection = NULL;
  sfixed32_t viewdef_definition_id_buyer_member = NULL;
  sfixed32_t deal_type = NULL;
  sfixed32_t ym_floor_id = NULL;
  sfixed32_t ym_bias_id = NULL;
  sfixed32_t bid_price_type = NULL;
  sfixed32_t spend_protection_pixel_id = NULL;
  string_t ip_address = NULL;
  log_transaction_def buyer_transaction_def = NULL;
  log_transaction_def seller_transaction_def = NULL;
  real64_t buyer_bid = NULL;
  sfixed32_t expected_events = NULL;
  fixed64_t accept_timestamp = NULL;
  string_t external_creative_id = NULL;
  sfixed32_t seat_id = NULL;
  bool_t is_prebid_server = NULL;
  sfixed32_t curated_deal_id = NULL;
  string_t external_campaign_id = NULL;
  string_t trust_id = NULL;
  log_product_ads log_product_ads = NULL;
  fixed64_t external_bidrequest_id = NULL;
  fixed64_t external_bidrequest_imp_id = NULL;
  sfixed32_t creative_media_subtype_id = NULL;
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
end[int] log_impbus_preempt_message_types =
[vector
  [record
    name "log_transaction_def"
    field_infos [vector
                  [record name "transaction_event" number 1 type_code 4 message_type -1 optional 1],
                  [record name "transaction_event_type_id" number 2 type_code 4 message_type -1 optional 1]]],
  [record
    name "log_product_ads"
    field_infos [vector
                  [record name "product_feed_id" number 1 type_code 4 message_type -1 optional 1],
                  [record name "item_selection_strategy_id" number 2 type_code 4 message_type -1 optional 1],
                  [record name "product_uuid" number 3 type_code 8 message_type -1 optional 1]]],
  [record
    name "log_impbus_preempt"
    field_infos [vector
                  [record name "date_time" number 1 type_code 1 message_type -1 optional 0],
                  [record name "auction_id_64" number 2 type_code 1 message_type -1 optional 0],
                  [record name "imp_transacted" number 3 type_code 4 message_type -1 optional 1],
                  [record name "buyer_spend" number 4 type_code 1 message_type -1 optional 1],
                  [record name "seller_revenue" number 5 type_code 1 message_type -1 optional 1],
                  [record name "bidder_fees" number 6 type_code 1 message_type -1 optional 1],
                  [record name "instance_id" number 12 type_code 4 message_type -1 optional 1],
                  [record name "fold_position" number 13 type_code 4 message_type -1 optional 1],
                  [record name "seller_deduction" number 14 type_code 1 message_type -1 optional 1],
                  [record name "buyer_member_id" number 15 type_code 4 message_type -1 optional 1],
                  [record name "creative_id" number 16 type_code 4 message_type -1 optional 1],
                  [record name "cleared_direct" number 17 type_code 4 message_type -1 optional 1],
                  [record name "buyer_currency" number 19 type_code 8 message_type -1 optional 1],
                  [record name "buyer_exchange_rate" number 20 type_code 1 message_type -1 optional 1],
                  [record name "width" number 21 type_code 4 message_type -1 optional 1],
                  [record name "height" number 22 type_code 4 message_type -1 optional 1],
                  [record name "brand_id" number 23 type_code 4 message_type -1 optional 1],
                  [record name "creative_audit_status" number 24 type_code 4 message_type -1 optional 1],
                  [record name "is_creative_hosted" number 25 type_code 4 message_type -1 optional 1],
                  [record name "vp_expose_domains" number 26 type_code 4 message_type -1 optional 1],
                  [record name "vp_expose_categories" number 27 type_code 4 message_type -1 optional 1],
                  [record name "vp_expose_pubs" number 28 type_code 4 message_type -1 optional 1],
                  [record name "vp_expose_tag" number 29 type_code 4 message_type -1 optional 1],
                  [record name "bidder_id" number 30 type_code 4 message_type -1 optional 1],
                  [record name "deal_id" number 31 type_code 4 message_type -1 optional 1],
                  [record name "imp_type" number 32 type_code 4 message_type -1 optional 1],
                  [record name "is_dw" number 33 type_code 4 message_type -1 optional 1],
                  [record name "vp_bitmap" number 34 type_code 3 message_type -1 optional 1],
                  [record name "ttl" number 35 type_code 4 message_type -1 optional 1],
                  [record name "view_detection_enabled" number 36 type_code 4 message_type -1 optional 1],
                  [record name "media_type" number 37 type_code 4 message_type -1 optional 1],
                  [record name "auction_timestamp" number 38 type_code 1 message_type -1 optional 0],
                  [record name "spend_protection" number 39 type_code 4 message_type -1 optional 1],
                  [record name "viewdef_definition_id_buyer_member" number 40 type_code 4 message_type -1 optional 1],
                  [record name "deal_type" number 41 type_code 4 message_type -1 optional 1],
                  [record name "ym_floor_id" number 42 type_code 4 message_type -1 optional 1],
                  [record name "ym_bias_id" number 43 type_code 4 message_type -1 optional 1],
                  [record name "bid_price_type" number 44 type_code 4 message_type -1 optional 1],
                  [record name "spend_protection_pixel_id" number 45 type_code 4 message_type -1 optional 1],
                  [record name "ip_address" number 46 type_code 8 message_type -1 optional 1],
                  [record name "buyer_transaction_def" number 47 type_code 9 message_type 0 optional 1],
                  [record name "seller_transaction_def" number 48 type_code 9 message_type 0 optional 1],
                  [record name "buyer_bid" number 49 type_code 1 message_type -1 optional 1],
                  [record name "expected_events" number 50 type_code 4 message_type -1 optional 1],
                  [record name "accept_timestamp" number 51 type_code 1 message_type -1 optional 1],
                  [record name "external_creative_id" number 52 type_code 8 message_type -1 optional 1],
                  [record name "seat_id" number 53 type_code 4 message_type -1 optional 1],
                  [record name "is_prebid_server" number 54 type_code 7 message_type -1 optional 1],
                  [record name "curated_deal_id" number 55 type_code 4 message_type -1 optional 1],
                  [record name "external_campaign_id" number 56 type_code 8 message_type -1 optional 1],
                  [record name "trust_id" number 57 type_code 8 message_type -1 optional 1],
                  [record name "log_product_ads" number 58 type_code 9 message_type 1 optional 1],
                  [record name "external_bidrequest_id" number 59 type_code 3 message_type -1 optional 1],
                  [record name "external_bidrequest_imp_id" number 60 type_code 3 message_type -1 optional 1],
                  [record name "creative_media_subtype_id" number 61 type_code 4 message_type -1 optional 1]]]];
metadata type = log_impbus_preempt;
metadata type = "log_impbus_preempt" ;""")
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
