package graph

import io.prophecy.libs._
import config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Write_Proto_HDFS_stage_impbus_impression_sample_stage_impbus_impression_sample_trans_path_pb {

  def apply(context: Context, in: DataFrame): Unit = {
    import _root_.io.prophecy.abinitio.dml.DMLSchema.parse
    import _root_.io.prophecy.libs.FFSchemaRecord
    import _root_.io.prophecy.libs.FixedFormatSchemaImplicits._
    import play.api.libs.json.Json
    try {
      val schema = Some(
        """type fixed32_t = unsigned little endian integer(4);
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
type stage_impbus_impression_sample =
record
  fixed64_t date_time;
  fixed64_t auction_id_64;
  fixed64_t user_id_64;
  sfixed32_t seller_member_id;
  sfixed32_t inventory_source_id;
  sfixed32_t tag_id;
  sfixed32_t venue_id;
  sfixed32_t publisher_id;
  string_t site_domain;
  sfixed32_t content_category_id;
  string_t geo_country;
  sfixed32_t user_group_id;
  sfixed32_t operating_system;
  string_t geo_region;
  sfixed32_t browser;
  sfixed32_t language;
  sfixed32_t inventory_url_id;
  sfixed32_t audit_type;
  sfixed32_t width;
  sfixed32_t height;
  enum_t imp_type;
  sfixed32_t is_transacted;
  sfixed32_t buyer_member_id;
  real64_t buyer_spend;
  string_t ip_address;
  sfixed32_t vp_expose_domains;
  sfixed32_t vp_expose_pubs;
  sfixed32_t visibility_profile_id;
  sfixed32_t is_exclusive;
  sfixed32_t device_id;
  sfixed32_t supply_type;
  sfixed32_t truncate_ip;
  string_t application_id;
  sfixed32_t device_type;
  sfixed32_t datacenter_id;
  sfixed32_t media_type;
  sfixed32_t site_id;
  sfixed32_t[length_t] allowed_media_types = NULL;
  bool_t imp_ignored = NULL;
  sfixed32_t imp_blacklist_or_fraud = NULL;
  log_anonymized_user_info anonymized_user_info = NULL;
  sfixed32_t cookie_age = NULL;
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
end[int] stage_impbus_impression_sample_message_types =
[vector
  [record
    name "log_anonymized_user_info"
    field_infos [vector
                  [record name "user_id" number 1 type_code 8 message_type -1 optional 1]]],
  [record
    name "stage_impbus_impression_sample"
    field_infos [vector
                  [record name "date_time" number 1 type_code 1 message_type -1 optional 0],
                  [record name "auction_id_64" number 2 type_code 1 message_type -1 optional 0],
                  [record name "user_id_64" number 3 type_code 1 message_type -1 optional 0],
                  [record name "seller_member_id" number 4 type_code 4 message_type -1 optional 0],
                  [record name "inventory_source_id" number 5 type_code 4 message_type -1 optional 0],
                  [record name "tag_id" number 6 type_code 4 message_type -1 optional 0],
                  [record name "venue_id" number 7 type_code 4 message_type -1 optional 0],
                  [record name "publisher_id" number 8 type_code 4 message_type -1 optional 0],
                  [record name "site_domain" number 9 type_code 8 message_type -1 optional 0],
                  [record name "content_category_id" number 10 type_code 4 message_type -1 optional 0],
                  [record name "geo_country" number 11 type_code 8 message_type -1 optional 0],
                  [record name "user_group_id" number 12 type_code 4 message_type -1 optional 0],
                  [record name "operating_system" number 13 type_code 4 message_type -1 optional 0],
                  [record name "geo_region" number 14 type_code 8 message_type -1 optional 0],
                  [record name "browser" number 15 type_code 4 message_type -1 optional 0],
                  [record name "language" number 16 type_code 4 message_type -1 optional 0],
                  [record name "inventory_url_id" number 17 type_code 4 message_type -1 optional 0],
                  [record name "audit_type" number 18 type_code 4 message_type -1 optional 0],
                  [record name "width" number 19 type_code 4 message_type -1 optional 0],
                  [record name "height" number 20 type_code 4 message_type -1 optional 0],
                  [record name "imp_type" number 21 type_code 4 message_type -1 optional 0],
                  [record name "is_transacted" number 22 type_code 4 message_type -1 optional 0],
                  [record name "buyer_member_id" number 23 type_code 4 message_type -1 optional 0],
                  [record name "buyer_spend" number 24 type_code 1 message_type -1 optional 0],
                  [record name "ip_address" number 25 type_code 8 message_type -1 optional 0],
                  [record name "vp_expose_domains" number 26 type_code 4 message_type -1 optional 0],
                  [record name "vp_expose_pubs" number 27 type_code 4 message_type -1 optional 0],
                  [record name "visibility_profile_id" number 28 type_code 4 message_type -1 optional 0],
                  [record name "is_exclusive" number 29 type_code 4 message_type -1 optional 0],
                  [record name "device_id" number 30 type_code 4 message_type -1 optional 0],
                  [record name "supply_type" number 31 type_code 4 message_type -1 optional 0],
                  [record name "truncate_ip" number 32 type_code 4 message_type -1 optional 0],
                  [record name "application_id" number 33 type_code 8 message_type -1 optional 0],
                  [record name "device_type" number 34 type_code 4 message_type -1 optional 0],
                  [record name "datacenter_id" number 35 type_code 4 message_type -1 optional 0],
                  [record name "media_type" number 36 type_code 4 message_type -1 optional 0],
                  [record name "site_id" number 37 type_code 4 message_type -1 optional 0],
                  [record name "allowed_media_types" number 38 type_code 20 message_type -1 optional 1],
                  [record name "imp_ignored" number 39 type_code 7 message_type -1 optional 1],
                  [record name "imp_blacklist_or_fraud" number 40 type_code 4 message_type -1 optional 1],
                  [record name "anonymized_user_info" number 41 type_code 9 message_type 0 optional 1],
                  [record name "cookie_age" number 42 type_code 4 message_type -1 optional 1]]]];
metadata type = stage_impbus_impression_sample;
metadata type = "stage_impbus_impression_sample" ;"""
      ).map(s => parse(s).asInstanceOf[FFSchemaRecord])
      var writer = in.write.format("io.prophecy.libs.FixedFileFormat")
      writer = writer.mode("overwrite")
      schema
        .map(s => Json.stringify(Json.toJson(s)))
        .foreach(schema => writer = writer.option("schema", schema))
      writer.save("NA")
    } catch {
      case e: Error =>
        println(s"Error occurred while writing dataframe: $e")
        throw new Exception(e.getMessage)
      case e: Throwable =>
        println(s"Throwable occurred while writing dataframe: $e")
        throw new Exception(e.getMessage)
    }
  }

}
