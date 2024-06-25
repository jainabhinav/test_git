package graph

import io.prophecy.libs._
import config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Write_Proto_HDFS_agg_dw_curator_impressions_pb_agg_dw_curator_impressions {

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
type targeted_segment_details =
record
  fixed32_t segment_id = NULL;
  fixed32_t last_seen_min = NULL;
end;
type excluded_targeted_segment_details =
record
  fixed32_t segment_id = NULL;
end;
type crossdevice_group =
record
  sfixed32_t graph_id = NULL;
  sfixed64_t group_id = NULL;
end;
type personal_data =
record
  fixed64_t user_id_64 = NULL;
  string_t device_unique_id = NULL;
  string_t external_uid = NULL;
  bytes_t ip_address = NULL;
  crossdevice_group crossdevice_group = NULL;
  real64_t latitude = NULL;
  real64_t longitude = NULL;
  bytes_t ipv6_address = NULL;
  bool_t subject_to_gdpr = NULL;
  string_t geo_country = NULL;
  string_t gdpr_consent_string = NULL;
  bytes_t preempt_ip_address = NULL;
  sfixed32_t device_type = NULL;
  sfixed32_t device_make_id = NULL;
  sfixed32_t device_model_id = NULL;
  fixed64_t new_user_id_64 = NULL;
  bool_t is_service_provider_mode = NULL;
  bool_t is_personal_info_sale = NULL;
end;
type log_anonymized_user_info =
record
  bytes_t user_id = NULL;
end;
type data_cost =
record
  sfixed32_t data_member_id = NULL;
  real64_t cost = NULL;
  sfixed32_t[length_t] used_segments = NULL;
  real64_t cost_pct = NULL;
end;
type crossdevice_graph_cost =
record
  sfixed32_t graph_provider_member_id = NULL;
  real64_t cost_cpm_usd = NULL;
end;
type log_personal_identifier =
record
  fixed32_t identity_type;
  string_t identity_value = NULL;
end;
type crossdevice_group_anonymized =
record
  sfixed32_t graph_id = NULL;
  bytes_t group_id = NULL;
end;
type agg_dw_curator_impressions =
record
  fixed64_t date_time;
  fixed64_t auction_id_64;
  sfixed32_t buyer_member_id = NULL;
  sfixed32_t seller_member_id = NULL;
  sfixed32_t curator_member_id;
  sfixed32_t member_id = NULL;
  sfixed32_t advertiser_id = NULL;
  sfixed32_t publisher_id = NULL;
  sfixed32_t bidder_id = NULL;
  sfixed32_t width = NULL;
  sfixed32_t height = NULL;
  sfixed32_t site_id = NULL;
  sfixed32_t tag_id = NULL;
  string_t geo_country = NULL;
  sfixed32_t brand_id = NULL;
  string_t site_domain = NULL;
  string_t application_id = NULL;
  sfixed32_t device_type = NULL;
  sfixed32_t insertion_order_id = NULL;
  sfixed32_t media_type = NULL;
  sfixed32_t curated_deal_id = NULL;
  sfixed32_t curated_deal_type = NULL;
  sfixed32_t seller_deal_id = NULL;
  sfixed32_t seller_deal_type = NULL;
  sfixed32_t campaign_group_id = NULL;
  sfixed32_t campaign_group_type_id = NULL;
  sfixed32_t fx_rate_snapshot_id = NULL;
  bool_t is_curated = NULL;
  enum_t video_context = NULL;
  enum_t view_result = NULL;
  sfixed32_t view_detection_enabled = NULL;
  sfixed32_t viewdef_definition_id = NULL;
  sfixed32_t viewdef_viewable = NULL;
  sfixed32_t view_measurable = NULL;
  sfixed32_t viewable = NULL;
  sfixed64_t total_data_costs_microcents = NULL;
  sfixed64_t total_cost_microcents = NULL;
  sfixed64_t total_partner_fees_microcents = NULL;
  sfixed64_t net_media_cost_microcents = NULL;
  sfixed64_t gross_revenue_microcents = NULL;
  sfixed64_t total_tech_fees_microcents = NULL;
  targeted_segment_details[length_t] targeted_segment_details = NULL;
  excluded_targeted_segment_details[length_t] excluded_targeted_segment_details = NULL;
  sfixed32_t supply_type = NULL;
  sfixed32_t vp_expose_domains = NULL;
  sfixed32_t inventory_url_id = NULL;
  sfixed64_t curator_margin_microcents = NULL;
  bool_t is_curator_margin_media_cost_dependent = NULL;
  enum_t curator_margin_type = NULL;
  sfixed32_t bidder_seat_id = NULL;
  personal_data personal_data = NULL;
  sfixed32_t user_tz_offset = NULL;
  string_t region = NULL;
  sfixed32_t dma = NULL;
  sfixed32_t city = NULL;
  string_t postal_code = NULL;
  sfixed32_t mobile_app_instance_id = NULL;
  sfixed32_t creative_id = NULL;
  sfixed32_t truncate_ip = NULL;
  sfixed64_t vp_bitmap = NULL;
  string_t gdpr_consent_string = NULL;
  log_anonymized_user_info anonymized_user_info = NULL;
  enum_t view_non_measurable_reason = NULL;
  sfixed32_t operating_system = NULL;
  sfixed32_t browser = NULL;
  sfixed32_t language = NULL;
  sfixed32_t device_id = NULL;
  data_cost[length_t] data_costs = NULL;
  crossdevice_graph_cost crossdevice_graph_cost = NULL;
  log_personal_identifier[length_t] personal_identifiers = NULL;
  crossdevice_group_anonymized crossdevice_group_anon = NULL;
  crossdevice_group_anonymized[length_t] crossdevice_graph_membership = NULL;
  bool_t has_crossdevice_reach_extension = NULL;
  sfixed32_t targeted_crossdevice_graph_id = NULL;
  string_t curated_line_item_currency = NULL;
  sfixed32_t split_id = NULL;
  fixed32_t bidding_host_id = NULL;
  sfixed64_t buyer_dpvp_bitmap = NULL;
  sfixed64_t seller_dpvp_bitmap = NULL;
  string_t external_campaign_id = NULL;
  fixed64_t external_bidrequest_imp_id = NULL;
  fixed64_t external_bidrequest_id = NULL;
  sfixed32_t postal_code_ext_id = NULL;
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
end[int] agg_dw_curator_impressions_message_types =
[vector
  [record
    name "targeted_segment_details"
    field_infos [vector
                  [record name "segment_id" number 1 type_code 4 message_type -1 optional 1],
                  [record name "last_seen_min" number 2 type_code 4 message_type -1 optional 1]]],
  [record
    name "excluded_targeted_segment_details"
    field_infos [vector
                  [record name "segment_id" number 1 type_code 4 message_type -1 optional 1]]],
  [record
    name "crossdevice_group"
    field_infos [vector
                  [record name "graph_id" number 1 type_code 4 message_type -1 optional 1],
                  [record name "group_id" number 2 type_code 3 message_type -1 optional 1]]],
  [record
    name "personal_data"
    field_infos [vector
                  [record name "user_id_64" number 3 type_code 1 message_type -1 optional 1],
                  [record name "device_unique_id" number 4 type_code 8 message_type -1 optional 1],
                  [record name "external_uid" number 5 type_code 8 message_type -1 optional 1],
                  [record name "ip_address" number 7 type_code 8 message_type -1 optional 1],
                  [record name "crossdevice_group" number 8 type_code 9 message_type 2 optional 1],
                  [record name "latitude" number 9 type_code 1 message_type -1 optional 1],
                  [record name "longitude" number 10 type_code 1 message_type -1 optional 1],
                  [record name "ipv6_address" number 11 type_code 8 message_type -1 optional 1],
                  [record name "subject_to_gdpr" number 12 type_code 7 message_type -1 optional 1],
                  [record name "geo_country" number 13 type_code 8 message_type -1 optional 1],
                  [record name "gdpr_consent_string" number 14 type_code 8 message_type -1 optional 1],
                  [record name "preempt_ip_address" number 15 type_code 8 message_type -1 optional 1],
                  [record name "device_type" number 16 type_code 4 message_type -1 optional 1],
                  [record name "device_make_id" number 17 type_code 4 message_type -1 optional 1],
                  [record name "device_model_id" number 18 type_code 4 message_type -1 optional 1],
                  [record name "new_user_id_64" number 19 type_code 1 message_type -1 optional 1],
                  [record name "is_service_provider_mode" number 20 type_code 7 message_type -1 optional 1],
                  [record name "is_personal_info_sale" number 21 type_code 7 message_type -1 optional 1]]],
  [record
    name "log_anonymized_user_info"
    field_infos [vector
                  [record name "user_id" number 1 type_code 8 message_type -1 optional 1]]],
  [record
    name "data_cost"
    field_infos [vector
                  [record name "data_member_id" number 1 type_code 4 message_type -1 optional 1],
                  [record name "cost" number 2 type_code 1 message_type -1 optional 1],
                  [record name "used_segments" number 3 type_code 20 message_type -1 optional 1],
                  [record name "cost_pct" number 4 type_code 1 message_type -1 optional 1]]],
  [record
    name "crossdevice_graph_cost"
    field_infos [vector
                  [record name "graph_provider_member_id" number 1 type_code 4 message_type -1 optional 1],
                  [record name "cost_cpm_usd" number 2 type_code 1 message_type -1 optional 1]]],
  [record
    name "log_personal_identifier"
    field_infos [vector
                  [record name "identity_type" number 1 type_code 4 message_type -1 optional 0],
                  [record name "identity_value" number 2 type_code 8 message_type -1 optional 1]]],
  [record
    name "crossdevice_group_anonymized"
    field_infos [vector
                  [record name "graph_id" number 1 type_code 4 message_type -1 optional 1],
                  [record name "group_id" number 2 type_code 8 message_type -1 optional 1]]],
  [record
    name "agg_dw_curator_impressions"
    field_infos [vector
                  [record name "date_time" number 1 type_code 1 message_type -1 optional 0],
                  [record name "auction_id_64" number 2 type_code 1 message_type -1 optional 0],
                  [record name "buyer_member_id" number 3 type_code 4 message_type -1 optional 1],
                  [record name "seller_member_id" number 4 type_code 4 message_type -1 optional 1],
                  [record name "curator_member_id" number 5 type_code 4 message_type -1 optional 0],
                  [record name "member_id" number 6 type_code 4 message_type -1 optional 1],
                  [record name "advertiser_id" number 7 type_code 4 message_type -1 optional 1],
                  [record name "publisher_id" number 8 type_code 4 message_type -1 optional 1],
                  [record name "bidder_id" number 9 type_code 4 message_type -1 optional 1],
                  [record name "width" number 10 type_code 4 message_type -1 optional 1],
                  [record name "height" number 11 type_code 4 message_type -1 optional 1],
                  [record name "site_id" number 12 type_code 4 message_type -1 optional 1],
                  [record name "tag_id" number 13 type_code 4 message_type -1 optional 1],
                  [record name "geo_country" number 14 type_code 8 message_type -1 optional 1],
                  [record name "brand_id" number 15 type_code 4 message_type -1 optional 1],
                  [record name "site_domain" number 16 type_code 8 message_type -1 optional 1],
                  [record name "application_id" number 17 type_code 8 message_type -1 optional 1],
                  [record name "device_type" number 18 type_code 4 message_type -1 optional 1],
                  [record name "insertion_order_id" number 19 type_code 4 message_type -1 optional 1],
                  [record name "media_type" number 20 type_code 4 message_type -1 optional 1],
                  [record name "curated_deal_id" number 21 type_code 4 message_type -1 optional 1],
                  [record name "curated_deal_type" number 22 type_code 4 message_type -1 optional 1],
                  [record name "seller_deal_id" number 23 type_code 4 message_type -1 optional 1],
                  [record name "seller_deal_type" number 24 type_code 4 message_type -1 optional 1],
                  [record name "campaign_group_id" number 25 type_code 4 message_type -1 optional 1],
                  [record name "campaign_group_type_id" number 26 type_code 4 message_type -1 optional 1],
                  [record name "fx_rate_snapshot_id" number 27 type_code 4 message_type -1 optional 1],
                  [record name "is_curated" number 28 type_code 7 message_type -1 optional 1],
                  [record name "video_context" number 29 type_code 4 message_type -1 optional 1],
                  [record name "view_result" number 30 type_code 4 message_type -1 optional 1],
                  [record name "view_detection_enabled" number 31 type_code 4 message_type -1 optional 1],
                  [record name "viewdef_definition_id" number 32 type_code 4 message_type -1 optional 1],
                  [record name "viewdef_viewable" number 33 type_code 4 message_type -1 optional 1],
                  [record name "view_measurable" number 34 type_code 4 message_type -1 optional 1],
                  [record name "viewable" number 35 type_code 4 message_type -1 optional 1],
                  [record name "total_data_costs_microcents" number 36 type_code 3 message_type -1 optional 1],
                  [record name "total_cost_microcents" number 37 type_code 3 message_type -1 optional 1],
                  [record name "total_partner_fees_microcents" number 38 type_code 3 message_type -1 optional 1],
                  [record name "net_media_cost_microcents" number 39 type_code 3 message_type -1 optional 1],
                  [record name "gross_revenue_microcents" number 40 type_code 3 message_type -1 optional 1],
                  [record name "total_tech_fees_microcents" number 41 type_code 3 message_type -1 optional 1],
                  [record name "targeted_segment_details" number 42 type_code 25 message_type 0 optional 1],
                  [record name "excluded_targeted_segment_details" number 43 type_code 25 message_type 1 optional 1],
                  [record name "supply_type" number 44 type_code 4 message_type -1 optional 1],
                  [record name "vp_expose_domains" number 45 type_code 4 message_type -1 optional 1],
                  [record name "inventory_url_id" number 46 type_code 4 message_type -1 optional 1],
                  [record name "curator_margin_microcents" number 47 type_code 3 message_type -1 optional 1],
                  [record name "is_curator_margin_media_cost_dependent" number 48 type_code 7 message_type -1 optional 1],
                  [record name "curator_margin_type" number 49 type_code 4 message_type -1 optional 1],
                  [record name "bidder_seat_id" number 50 type_code 4 message_type -1 optional 1],
                  [record name "personal_data" number 51 type_code 9 message_type 3 optional 1],
                  [record name "user_tz_offset" number 52 type_code 4 message_type -1 optional 1],
                  [record name "region" number 53 type_code 8 message_type -1 optional 1],
                  [record name "dma" number 54 type_code 4 message_type -1 optional 1],
                  [record name "city" number 55 type_code 4 message_type -1 optional 1],
                  [record name "postal_code" number 56 type_code 8 message_type -1 optional 1],
                  [record name "mobile_app_instance_id" number 57 type_code 4 message_type -1 optional 1],
                  [record name "creative_id" number 58 type_code 4 message_type -1 optional 1],
                  [record name "truncate_ip" number 59 type_code 4 message_type -1 optional 1],
                  [record name "vp_bitmap" number 60 type_code 3 message_type -1 optional 1],
                  [record name "gdpr_consent_string" number 61 type_code 8 message_type -1 optional 1],
                  [record name "anonymized_user_info" number 62 type_code 9 message_type 4 optional 1],
                  [record name "view_non_measurable_reason" number 63 type_code 4 message_type -1 optional 1],
                  [record name "operating_system" number 64 type_code 4 message_type -1 optional 1],
                  [record name "browser" number 65 type_code 4 message_type -1 optional 1],
                  [record name "language" number 66 type_code 4 message_type -1 optional 1],
                  [record name "device_id" number 67 type_code 4 message_type -1 optional 1],
                  [record name "data_costs" number 68 type_code 25 message_type 5 optional 1],
                  [record name "crossdevice_graph_cost" number 69 type_code 9 message_type 6 optional 1],
                  [record name "personal_identifiers" number 70 type_code 25 message_type 7 optional 1],
                  [record name "crossdevice_group_anon" number 71 type_code 9 message_type 8 optional 1],
                  [record name "crossdevice_graph_membership" number 72 type_code 25 message_type 8 optional 1],
                  [record name "has_crossdevice_reach_extension" number 73 type_code 7 message_type -1 optional 1],
                  [record name "targeted_crossdevice_graph_id" number 74 type_code 4 message_type -1 optional 1],
                  [record name "curated_line_item_currency" number 75 type_code 8 message_type -1 optional 1],
                  [record name "split_id" number 76 type_code 4 message_type -1 optional 1],
                  [record name "bidding_host_id" number 77 type_code 4 message_type -1 optional 1],
                  [record name "buyer_dpvp_bitmap" number 78 type_code 3 message_type -1 optional 1],
                  [record name "seller_dpvp_bitmap" number 79 type_code 3 message_type -1 optional 1],
                  [record name "external_campaign_id" number 80 type_code 8 message_type -1 optional 1],
                  [record name "external_bidrequest_imp_id" number 81 type_code 3 message_type -1 optional 1],
                  [record name "external_bidrequest_id" number 82 type_code 3 message_type -1 optional 1],
                  [record name "postal_code_ext_id" number 83 type_code 4 message_type -1 optional 1]]]];
metadata type = agg_dw_curator_impressions ;"""
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
