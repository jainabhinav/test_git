package graph

import io.prophecy.libs._
import config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Read_Proto_Range_log_impbus_imptracker_pb_log_impbus_imptracker {

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
type log_anonymized_user_info =
record
  bytes_t user_id = NULL;
end;
type log_impbus_imptracker =
record
  fixed64_t date_time;
  fixed64_t auction_id_64;
  sfixed32_t member_id = NULL;
  fixed64_t user_id_64 = NULL;
  sfixed32_t tracker_id = NULL;
  string_t ip_address = NULL;
  string_t user_agent = NULL;
  string_t site_domain = NULL;
  string_t geo_country = NULL;
  string_t geo_region = NULL;
  sfixed32_t content_category_id = NULL;
  sfixed32_t datacenter_id = NULL;
  sfixed32_t user_tz_offset = NULL;
  sfixed32_t user_group_id = NULL;
  sfixed32_t operating_system = NULL;
  sfixed32_t browser = NULL;
  sfixed32_t language = NULL;
  string_t seller_currency = NULL;
  real64_t seller_exchange_rate = NULL;
  sfixed32_t site_id = NULL;
  sfixed32_t publisher_id = NULL;
  sfixed32_t tag_id = NULL;
  sfixed32_t pub_rule_id = NULL;
  string_t referral_url = NULL;
  sfixed32_t truncate_ip = NULL;
  sfixed32_t operating_system_family_id = NULL;
  log_anonymized_user_info anonymized_user_info = NULL;
  sfixed32_t region_id = NULL;
  sfixed32_t fx_rate_snapshot_id = NULL;
  string_t hashed_ip = NULL;
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
end[int] log_impbus_imptracker_message_types =
[vector
  [record
    name "log_anonymized_user_info"
    field_infos [vector
                  [record name "user_id" number 1 type_code 8 message_type -1 optional 1]]],
  [record
    name "log_impbus_imptracker"
    field_infos [vector
                  [record name "date_time" number 1 type_code 1 message_type -1 optional 0],
                  [record name "auction_id_64" number 2 type_code 1 message_type -1 optional 0],
                  [record name "member_id" number 3 type_code 4 message_type -1 optional 1],
                  [record name "user_id_64" number 4 type_code 1 message_type -1 optional 1],
                  [record name "tracker_id" number 5 type_code 4 message_type -1 optional 1],
                  [record name "ip_address" number 6 type_code 8 message_type -1 optional 1],
                  [record name "user_agent" number 7 type_code 8 message_type -1 optional 1],
                  [record name "site_domain" number 8 type_code 8 message_type -1 optional 1],
                  [record name "geo_country" number 9 type_code 8 message_type -1 optional 1],
                  [record name "geo_region" number 10 type_code 8 message_type -1 optional 1],
                  [record name "content_category_id" number 11 type_code 4 message_type -1 optional 1],
                  [record name "datacenter_id" number 12 type_code 4 message_type -1 optional 1],
                  [record name "user_tz_offset" number 13 type_code 4 message_type -1 optional 1],
                  [record name "user_group_id" number 14 type_code 4 message_type -1 optional 1],
                  [record name "operating_system" number 15 type_code 4 message_type -1 optional 1],
                  [record name "browser" number 16 type_code 4 message_type -1 optional 1],
                  [record name "language" number 17 type_code 4 message_type -1 optional 1],
                  [record name "seller_currency" number 18 type_code 8 message_type -1 optional 1],
                  [record name "seller_exchange_rate" number 19 type_code 1 message_type -1 optional 1],
                  [record name "site_id" number 20 type_code 4 message_type -1 optional 1],
                  [record name "publisher_id" number 21 type_code 4 message_type -1 optional 1],
                  [record name "tag_id" number 22 type_code 4 message_type -1 optional 1],
                  [record name "pub_rule_id" number 23 type_code 4 message_type -1 optional 1],
                  [record name "referral_url" number 24 type_code 8 message_type -1 optional 1],
                  [record name "truncate_ip" number 25 type_code 4 message_type -1 optional 1],
                  [record name "operating_system_family_id" number 26 type_code 4 message_type -1 optional 1],
                  [record name "anonymized_user_info" number 27 type_code 9 message_type 0 optional 1],
                  [record name "region_id" number 28 type_code 4 message_type -1 optional 1],
                  [record name "fx_rate_snapshot_id" number 29 type_code 4 message_type -1 optional 1],
                  [record name "hashed_ip" number 30 type_code 8 message_type -1 optional 1]]]];
metadata type = log_impbus_imptracker;
metadata type = "log_impbus_imptracker" ;""")
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
