package graph

import io.prophecy.libs._
import config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Read_Proto_Range_log_impbus_auction_event_pb_log_impbus_auction_event {

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
type pricing_term =
record
  sfixed32_t term_id = NULL;
  real64_t amount = NULL;
  real64_t rate = NULL;
  bool_t is_deduction = NULL;
  bool_t is_media_cost_dependent = NULL;
  sfixed32_t data_member_id = NULL;
end;
type member_pricing_term =
record
  sfixed32_t rate_card_id = NULL;
  sfixed32_t member_id = NULL;
  bool_t is_dw = NULL;
  pricing_term[length_t] pricing_terms = NULL;
  sfixed32_t fx_margin_rate_id = NULL;
  sfixed32_t marketplace_owner_id = NULL;
  sfixed32_t virtual_marketplace_id = NULL;
  bool_t amino_enabled = NULL;
end;
type transaction_event_pricing =
record
  fixed64_t gross_payment_value_microcents = NULL;
  fixed64_t net_payment_value_microcents = NULL;
  fixed64_t seller_revenue_microcents = NULL;
  member_pricing_term buyer_charges = NULL;
  member_pricing_term seller_charges = NULL;
  bool_t buyer_transacted = NULL;
  bool_t seller_transacted = NULL;
end;
type log_impbus_auction_event =
record
  fixed64_t date_time;
  fixed64_t auction_id_64;
  fixed64_t payment_value_microcents = NULL;
  sfixed32_t transaction_event = NULL;
  sfixed32_t transaction_event_type_id = NULL;
  bool_t is_deferred = NULL;
  transaction_event_pricing auction_event_pricing = NULL;
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
end[int] log_impbus_auction_event_message_types =
[vector
  [record
    name "pricing_term"
    field_infos [vector
                  [record name "term_id" number 1 type_code 4 message_type -1 optional 1],
                  [record name "amount" number 2 type_code 1 message_type -1 optional 1],
                  [record name "rate" number 3 type_code 1 message_type -1 optional 1],
                  [record name "is_deduction" number 4 type_code 7 message_type -1 optional 1],
                  [record name "is_media_cost_dependent" number 5 type_code 7 message_type -1 optional 1],
                  [record name "data_member_id" number 6 type_code 4 message_type -1 optional 1]]],
  [record
    name "member_pricing_term"
    field_infos [vector
                  [record name "rate_card_id" number 1 type_code 4 message_type -1 optional 1],
                  [record name "member_id" number 2 type_code 4 message_type -1 optional 1],
                  [record name "is_dw" number 3 type_code 7 message_type -1 optional 1],
                  [record name "pricing_terms" number 4 type_code 25 message_type 0 optional 1],
                  [record name "fx_margin_rate_id" number 5 type_code 4 message_type -1 optional 1],
                  [record name "marketplace_owner_id" number 6 type_code 4 message_type -1 optional 1],
                  [record name "virtual_marketplace_id" number 7 type_code 4 message_type -1 optional 1],
                  [record name "amino_enabled" number 8 type_code 7 message_type -1 optional 1]]],
  [record
    name "transaction_event_pricing"
    field_infos [vector
                  [record name "gross_payment_value_microcents" number 1 type_code 3 message_type -1 optional 1],
                  [record name "net_payment_value_microcents" number 2 type_code 3 message_type -1 optional 1],
                  [record name "seller_revenue_microcents" number 3 type_code 3 message_type -1 optional 1],
                  [record name "buyer_charges" number 4 type_code 9 message_type 1 optional 1],
                  [record name "seller_charges" number 5 type_code 9 message_type 1 optional 1],
                  [record name "buyer_transacted" number 6 type_code 7 message_type -1 optional 1],
                  [record name "seller_transacted" number 7 type_code 7 message_type -1 optional 1]]],
  [record
    name "log_impbus_auction_event"
    field_infos [vector
                  [record name "date_time" number 1 type_code 1 message_type -1 optional 0],
                  [record name "auction_id_64" number 2 type_code 1 message_type -1 optional 0],
                  [record name "payment_value_microcents" number 3 type_code 3 message_type -1 optional 1],
                  [record name "transaction_event" number 4 type_code 4 message_type -1 optional 1],
                  [record name "transaction_event_type_id" number 7 type_code 4 message_type -1 optional 1],
                  [record name "is_deferred" number 8 type_code 7 message_type -1 optional 1],
                  [record name "auction_event_pricing" number 14 type_code 9 message_type 2 optional 1]]]];
metadata type = log_impbus_auction_event;
metadata type = "log_impbus_auction_event" ;""")
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
