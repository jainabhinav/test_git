package graph

import io.prophecy.libs._
import config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Write_Proto_HDFS_SSPQ_stage_tl_trx_trans_denormalized_pb_stage_seen_denormalized {

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
type log_transaction_def =
record
  sfixed32_t transaction_event = NULL;
  sfixed32_t transaction_event_type_id = NULL;
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
type log_personal_identifier =
record
  fixed32_t identity_type;
  string_t identity_value = NULL;
end;
type stage_seen_denormalized =
record
  fixed64_t date_time;
  fixed64_t auction_id_64;
  sfixed32_t seller_member_id = NULL;
  sfixed32_t buyer_member_id = NULL;
  sfixed32_t width = NULL;
  sfixed32_t height = NULL;
  sfixed32_t publisher_id = NULL;
  sfixed32_t site_id = NULL;
  sfixed32_t tag_id = NULL;
  string_t gender = NULL;
  string_t geo_country = NULL;
  sfixed32_t inventory_source_id = NULL;
  enum_t imp_type = NULL;
  sfixed32_t is_dw = NULL;
  sfixed32_t bidder_id = NULL;
  real64_t sampling_pct = NULL;
  real64_t seller_revenue = NULL;
  real64_t buyer_spend = NULL;
  sfixed32_t cleared_direct = NULL;
  real64_t creative_overage_fees = NULL;
  real64_t auction_service_fees = NULL;
  real64_t clear_fees = NULL;
  real64_t discrepancy_allowance = NULL;
  real64_t forex_allowance = NULL;
  real64_t auction_service_deduction = NULL;
  sfixed32_t content_category_id = NULL;
  sfixed32_t datacenter_id = NULL;
  sfixed32_t imp_bid_on = NULL;
  real64_t ecp = NULL;
  real64_t eap = NULL;
  string_t buyer_currency = NULL;
  real64_t buyer_spend_buyer_currency = NULL;
  string_t seller_currency = NULL;
  real64_t seller_revenue_seller_currency = NULL;
  sfixed32_t vp_expose_pubs = NULL;
  real64_t seller_deduction = NULL;
  sfixed32_t supply_type = NULL;
  sfixed32_t is_delivered = NULL;
  sfixed32_t buyer_bid_bucket = NULL;
  sfixed32_t device_id = NULL;
  fixed64_t user_id_64 = NULL;
  sfixed32_t cookie_age = NULL;
  string_t ip_address = NULL;
  sfixed32_t imp_blacklist_or_fraud = NULL;
  string_t site_domain = NULL;
  sfixed32_t view_measurable = NULL;
  sfixed32_t viewable = NULL;
  string_t call_type = NULL;
  sfixed32_t deal_id = NULL;
  string_t application_id = NULL;
  fixed64_t imp_date_time = NULL;
  sfixed32_t media_type = NULL;
  sfixed32_t pub_rule_id = NULL;
  sfixed32_t venue_id = NULL;
  member_pricing_term buyer_charges = NULL;
  member_pricing_term seller_charges = NULL;
  real64_t buyer_bid = NULL;
  string_t preempt_ip_address = NULL;
  log_transaction_def seller_transaction_def = NULL;
  log_transaction_def buyer_transaction_def = NULL;
  bool_t is_imp_rejecter_applied = NULL;
  bool_t imp_rejecter_do_auction = NULL;
  sfixed32_t audit_type = NULL;
  sfixed32_t browser = NULL;
  sfixed32_t device_type = NULL;
  string_t geo_region = NULL;
  sfixed32_t language = NULL;
  sfixed32_t operating_system = NULL;
  sfixed32_t operating_system_family_id = NULL;
  sfixed32_t[length_t] allowed_media_types = NULL;
  bool_t imp_biddable = NULL;
  bool_t imp_ignored = NULL;
  sfixed32_t user_group_id = NULL;
  sfixed32_t inventory_url_id = NULL;
  sfixed32_t vp_expose_domains = NULL;
  sfixed32_t visibility_profile_id = NULL;
  sfixed32_t is_exclusive = NULL;
  sfixed32_t truncate_ip = NULL;
  sfixed32_t creative_id = NULL;
  real64_t buyer_exchange_rate = NULL;
  sfixed32_t vp_expose_tag = NULL;
  enum_t view_result = NULL;
  sfixed32_t age = NULL;
  sfixed32_t brand_id = NULL;
  sfixed32_t carrier_id = NULL;
  sfixed32_t city = NULL;
  sfixed32_t dma = NULL;
  string_t device_unique_id = NULL;
  string_t latitude = NULL;
  string_t longitude = NULL;
  string_t postal = NULL;
  string_t sdk_version = NULL;
  sfixed32_t pricing_media_type = NULL;
  string_t traffic_source_code = NULL;
  bool_t is_prebid = NULL;
  bool_t is_unit_of_buyer_trx = NULL;
  bool_t is_unit_of_seller_trx = NULL;
  bool_t two_phase_reduction_applied = NULL;
  sfixed32_t region_id = NULL;
  personal_data personal_data = NULL;
  log_anonymized_user_info anonymized_user_info = NULL;
  string_t gdpr_consent_cookie = NULL;
  string_t external_creative_id = NULL;
  bool_t subject_to_gdpr = NULL;
  sfixed32_t fx_rate_snapshot_id = NULL;
  sfixed32_t view_detection_enabled = NULL;
  real64_t seller_exchange_rate = NULL;
  sfixed32_t browser_code_id = NULL;
  sfixed32_t is_prebid_server_included = NULL;
  sfixed32_t bidder_seat_id = NULL;
  string_t default_referrer_url = NULL;
  sfixed32_t pred_info = NULL;
  sfixed32_t curated_deal_id = NULL;
  sfixed32_t deal_type = NULL;
  sfixed32_t primary_height = NULL;
  sfixed32_t primary_width = NULL;
  sfixed32_t curator_member_id = NULL;
  sfixed32_t instance_id = NULL;
  sfixed32_t hb_source = NULL;
  bool_t from_imps_seen = NULL;
  string_t external_campaign_id = NULL;
  bool_t ss_native_assembly_enabled = NULL;
  enum_t uid_source = NULL;
  enum_t video_context = NULL;
  log_personal_identifier[length_t] personal_identifiers = NULL;
  log_personal_identifier[length_t] personal_identifiers_experimental = NULL;
  sfixed32_t user_tz_offset = NULL;
  fixed64_t external_bidrequest_id = NULL;
  fixed64_t external_bidrequest_imp_id = NULL;
  sfixed32_t ym_floor_id = NULL;
  sfixed32_t ym_bias_id = NULL;
  string_t openrtb_req_subdomain = NULL;
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
end[int] stage_seen_denormalized_message_types =
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
    name "log_transaction_def"
    field_infos [vector
                  [record name "transaction_event" number 1 type_code 4 message_type -1 optional 1],
                  [record name "transaction_event_type_id" number 2 type_code 4 message_type -1 optional 1]]],
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
                  [record name "crossdevice_group" number 8 type_code 9 message_type 3 optional 1],
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
    name "log_personal_identifier"
    field_infos [vector
                  [record name "identity_type" number 1 type_code 4 message_type -1 optional 0],
                  [record name "identity_value" number 2 type_code 8 message_type -1 optional 1]]],
  [record
    name "stage_seen_denormalized"
    field_infos [vector
                  [record name "date_time" number 1 type_code 1 message_type -1 optional 0],
                  [record name "auction_id_64" number 2 type_code 1 message_type -1 optional 0],
                  [record name "seller_member_id" number 3 type_code 4 message_type -1 optional 1],
                  [record name "buyer_member_id" number 4 type_code 4 message_type -1 optional 1],
                  [record name "width" number 5 type_code 4 message_type -1 optional 1],
                  [record name "height" number 6 type_code 4 message_type -1 optional 1],
                  [record name "publisher_id" number 7 type_code 4 message_type -1 optional 1],
                  [record name "site_id" number 8 type_code 4 message_type -1 optional 1],
                  [record name "tag_id" number 9 type_code 4 message_type -1 optional 1],
                  [record name "gender" number 10 type_code 8 message_type -1 optional 1],
                  [record name "geo_country" number 11 type_code 8 message_type -1 optional 1],
                  [record name "inventory_source_id" number 13 type_code 4 message_type -1 optional 1],
                  [record name "imp_type" number 14 type_code 4 message_type -1 optional 1],
                  [record name "is_dw" number 15 type_code 4 message_type -1 optional 1],
                  [record name "bidder_id" number 16 type_code 4 message_type -1 optional 1],
                  [record name "sampling_pct" number 17 type_code 1 message_type -1 optional 1],
                  [record name "seller_revenue" number 18 type_code 1 message_type -1 optional 1],
                  [record name "buyer_spend" number 19 type_code 1 message_type -1 optional 1],
                  [record name "cleared_direct" number 20 type_code 4 message_type -1 optional 1],
                  [record name "creative_overage_fees" number 21 type_code 1 message_type -1 optional 1],
                  [record name "auction_service_fees" number 22 type_code 1 message_type -1 optional 1],
                  [record name "clear_fees" number 23 type_code 1 message_type -1 optional 1],
                  [record name "discrepancy_allowance" number 24 type_code 1 message_type -1 optional 1],
                  [record name "forex_allowance" number 25 type_code 1 message_type -1 optional 1],
                  [record name "auction_service_deduction" number 26 type_code 1 message_type -1 optional 1],
                  [record name "content_category_id" number 27 type_code 4 message_type -1 optional 1],
                  [record name "datacenter_id" number 28 type_code 4 message_type -1 optional 1],
                  [record name "imp_bid_on" number 29 type_code 4 message_type -1 optional 1],
                  [record name "ecp" number 30 type_code 1 message_type -1 optional 1],
                  [record name "eap" number 31 type_code 1 message_type -1 optional 1],
                  [record name "buyer_currency" number 32 type_code 8 message_type -1 optional 1],
                  [record name "buyer_spend_buyer_currency" number 33 type_code 1 message_type -1 optional 1],
                  [record name "seller_currency" number 34 type_code 8 message_type -1 optional 1],
                  [record name "seller_revenue_seller_currency" number 35 type_code 1 message_type -1 optional 1],
                  [record name "vp_expose_pubs" number 36 type_code 4 message_type -1 optional 1],
                  [record name "seller_deduction" number 37 type_code 1 message_type -1 optional 1],
                  [record name "supply_type" number 38 type_code 4 message_type -1 optional 1],
                  [record name "is_delivered" number 39 type_code 4 message_type -1 optional 1],
                  [record name "buyer_bid_bucket" number 40 type_code 4 message_type -1 optional 1],
                  [record name "device_id" number 41 type_code 4 message_type -1 optional 1],
                  [record name "user_id_64" number 42 type_code 1 message_type -1 optional 1],
                  [record name "cookie_age" number 43 type_code 4 message_type -1 optional 1],
                  [record name "ip_address" number 44 type_code 8 message_type -1 optional 1],
                  [record name "imp_blacklist_or_fraud" number 45 type_code 4 message_type -1 optional 1],
                  [record name "site_domain" number 46 type_code 8 message_type -1 optional 1],
                  [record name "view_measurable" number 47 type_code 4 message_type -1 optional 1],
                  [record name "viewable" number 48 type_code 4 message_type -1 optional 1],
                  [record name "call_type" number 49 type_code 8 message_type -1 optional 1],
                  [record name "deal_id" number 50 type_code 4 message_type -1 optional 1],
                  [record name "application_id" number 51 type_code 8 message_type -1 optional 1],
                  [record name "imp_date_time" number 52 type_code 1 message_type -1 optional 1],
                  [record name "media_type" number 53 type_code 4 message_type -1 optional 1],
                  [record name "pub_rule_id" number 54 type_code 4 message_type -1 optional 1],
                  [record name "venue_id" number 55 type_code 4 message_type -1 optional 1],
                  [record name "buyer_charges" number 56 type_code 9 message_type 1 optional 1],
                  [record name "seller_charges" number 57 type_code 9 message_type 1 optional 1],
                  [record name "buyer_bid" number 58 type_code 1 message_type -1 optional 1],
                  [record name "preempt_ip_address" number 59 type_code 8 message_type -1 optional 1],
                  [record name "seller_transaction_def" number 60 type_code 9 message_type 2 optional 1],
                  [record name "buyer_transaction_def" number 61 type_code 9 message_type 2 optional 1],
                  [record name "is_imp_rejecter_applied" number 62 type_code 7 message_type -1 optional 1],
                  [record name "imp_rejecter_do_auction" number 63 type_code 7 message_type -1 optional 1],
                  [record name "audit_type" number 64 type_code 4 message_type -1 optional 1],
                  [record name "browser" number 65 type_code 4 message_type -1 optional 1],
                  [record name "device_type" number 66 type_code 4 message_type -1 optional 1],
                  [record name "geo_region" number 67 type_code 8 message_type -1 optional 1],
                  [record name "language" number 68 type_code 4 message_type -1 optional 1],
                  [record name "operating_system" number 69 type_code 4 message_type -1 optional 1],
                  [record name "operating_system_family_id" number 70 type_code 4 message_type -1 optional 1],
                  [record name "allowed_media_types" number 71 type_code 20 message_type -1 optional 1],
                  [record name "imp_biddable" number 72 type_code 7 message_type -1 optional 1],
                  [record name "imp_ignored" number 73 type_code 7 message_type -1 optional 1],
                  [record name "user_group_id" number 74 type_code 4 message_type -1 optional 1],
                  [record name "inventory_url_id" number 75 type_code 4 message_type -1 optional 1],
                  [record name "vp_expose_domains" number 76 type_code 4 message_type -1 optional 1],
                  [record name "visibility_profile_id" number 77 type_code 4 message_type -1 optional 1],
                  [record name "is_exclusive" number 78 type_code 4 message_type -1 optional 1],
                  [record name "truncate_ip" number 79 type_code 4 message_type -1 optional 1],
                  [record name "creative_id" number 80 type_code 4 message_type -1 optional 1],
                  [record name "buyer_exchange_rate" number 81 type_code 1 message_type -1 optional 1],
                  [record name "vp_expose_tag" number 82 type_code 4 message_type -1 optional 1],
                  [record name "view_result" number 83 type_code 4 message_type -1 optional 1],
                  [record name "age" number 84 type_code 4 message_type -1 optional 1],
                  [record name "brand_id" number 85 type_code 4 message_type -1 optional 1],
                  [record name "carrier_id" number 86 type_code 4 message_type -1 optional 1],
                  [record name "city" number 87 type_code 4 message_type -1 optional 1],
                  [record name "dma" number 88 type_code 4 message_type -1 optional 1],
                  [record name "device_unique_id" number 89 type_code 8 message_type -1 optional 1],
                  [record name "latitude" number 90 type_code 8 message_type -1 optional 1],
                  [record name "longitude" number 91 type_code 8 message_type -1 optional 1],
                  [record name "postal" number 92 type_code 8 message_type -1 optional 1],
                  [record name "sdk_version" number 93 type_code 8 message_type -1 optional 1],
                  [record name "pricing_media_type" number 94 type_code 4 message_type -1 optional 1],
                  [record name "traffic_source_code" number 95 type_code 8 message_type -1 optional 1],
                  [record name "is_prebid" number 96 type_code 7 message_type -1 optional 1],
                  [record name "is_unit_of_buyer_trx" number 97 type_code 7 message_type -1 optional 1],
                  [record name "is_unit_of_seller_trx" number 98 type_code 7 message_type -1 optional 1],
                  [record name "two_phase_reduction_applied" number 100 type_code 7 message_type -1 optional 1],
                  [record name "region_id" number 101 type_code 4 message_type -1 optional 1],
                  [record name "personal_data" number 102 type_code 9 message_type 4 optional 1],
                  [record name "anonymized_user_info" number 103 type_code 9 message_type 5 optional 1],
                  [record name "gdpr_consent_cookie" number 104 type_code 8 message_type -1 optional 1],
                  [record name "external_creative_id" number 105 type_code 8 message_type -1 optional 1],
                  [record name "subject_to_gdpr" number 106 type_code 7 message_type -1 optional 1],
                  [record name "fx_rate_snapshot_id" number 107 type_code 4 message_type -1 optional 1],
                  [record name "view_detection_enabled" number 108 type_code 4 message_type -1 optional 1],
                  [record name "seller_exchange_rate" number 109 type_code 1 message_type -1 optional 1],
                  [record name "browser_code_id" number 110 type_code 4 message_type -1 optional 1],
                  [record name "is_prebid_server_included" number 111 type_code 4 message_type -1 optional 1],
                  [record name "bidder_seat_id" number 112 type_code 4 message_type -1 optional 1],
                  [record name "default_referrer_url" number 113 type_code 8 message_type -1 optional 1],
                  [record name "pred_info" number 114 type_code 4 message_type -1 optional 1],
                  [record name "curated_deal_id" number 115 type_code 4 message_type -1 optional 1],
                  [record name "deal_type" number 116 type_code 4 message_type -1 optional 1],
                  [record name "primary_height" number 117 type_code 4 message_type -1 optional 1],
                  [record name "primary_width" number 118 type_code 4 message_type -1 optional 1],
                  [record name "curator_member_id" number 119 type_code 4 message_type -1 optional 1],
                  [record name "instance_id" number 120 type_code 4 message_type -1 optional 1],
                  [record name "hb_source" number 121 type_code 4 message_type -1 optional 1],
                  [record name "from_imps_seen" number 122 type_code 7 message_type -1 optional 1],
                  [record name "external_campaign_id" number 123 type_code 8 message_type -1 optional 1],
                  [record name "ss_native_assembly_enabled" number 124 type_code 7 message_type -1 optional 1],
                  [record name "uid_source" number 125 type_code 4 message_type -1 optional 1],
                  [record name "video_context" number 126 type_code 4 message_type -1 optional 1],
                  [record name "personal_identifiers" number 127 type_code 25 message_type 6 optional 1],
                  [record name "personal_identifiers_experimental" number 128 type_code 25 message_type 6 optional 1],
                  [record name "user_tz_offset" number 130 type_code 4 message_type -1 optional 1],
                  [record name "external_bidrequest_id" number 131 type_code 3 message_type -1 optional 1],
                  [record name "external_bidrequest_imp_id" number 132 type_code 3 message_type -1 optional 1],
                  [record name "ym_floor_id" number 133 type_code 4 message_type -1 optional 1],
                  [record name "ym_bias_id" number 134 type_code 4 message_type -1 optional 1],
                  [record name "openrtb_req_subdomain" number 135 type_code 8 message_type -1 optional 1]]]];
metadata type = stage_seen_denormalized;
metadata type = "stage_seen_denormalized" ;"""
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
