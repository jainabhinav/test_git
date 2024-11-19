package graph

import io.prophecy.libs._
import config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Read_Proto_Range_log_impbus_impressions_pb_log_impbus_impressions {

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
type tag_size =
record
  sfixed32_t width;
  sfixed32_t height;
end;
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
type log_predicted_video_view_info =
record
  real64_t iab_view_rate_over_measured = NULL;
  real64_t iab_view_rate_over_total = NULL;
  real64_t predicted_100pv50pd_video_view_rate = NULL;
  real64_t predicted_100pv50pd_video_view_rate_over_total = NULL;
  real64_t video_completion_rate = NULL;
  sfixed32_t view_prediction_source = NULL;
end;
type log_auction_url_info =
record
  string_t site_url = NULL;
end;
type log_location_info =
record
  real32_t latitude = NULL;
  real32_t longitude = NULL;
end;
type log_engagement_rate =
record
  sfixed32_t engagement_rate_type = NULL;
  real64_t rate = NULL;
  sfixed32_t engagement_rate_type_id = NULL;
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
type crossdevice_group =
record
  sfixed32_t graph_id = NULL;
  sfixed64_t group_id = NULL;
end;
type log_impbus_impressions =
record
  fixed64_t date_time;
  fixed64_t auction_id_64;
  fixed64_t user_id_64 = NULL;
  sfixed32_t tag_id = NULL;
  string_t ip_address = NULL;
  sfixed32_t venue_id = NULL;
  string_t site_domain = NULL;
  sfixed32_t width = NULL;
  sfixed32_t height = NULL;
  string_t geo_country = NULL;
  string_t geo_region = NULL;
  string_t gender = NULL;
  sfixed32_t age = NULL;
  sfixed32_t bidder_id = NULL;
  sfixed32_t seller_member_id = NULL;
  sfixed32_t buyer_member_id = NULL;
  sfixed32_t creative_id = NULL;
  sfixed32_t imp_blacklist_or_fraud = NULL;
  sfixed32_t imp_bid_on = NULL;
  real64_t buyer_bid = NULL;
  real64_t buyer_spend = NULL;
  real64_t seller_revenue = NULL;
  sfixed32_t num_of_bids = NULL;
  real64_t ecp = NULL;
  real64_t reserve_price = NULL;
  string_t inv_code = NULL;
  string_t call_type = NULL;
  sfixed32_t inventory_source_id = NULL;
  sfixed32_t cookie_age = NULL;
  sfixed32_t brand_id = NULL;
  sfixed32_t cleared_direct = NULL;
  real64_t forex_allowance = NULL;
  sfixed32_t fold_position = NULL;
  sfixed32_t external_inv_id = NULL;
  enum_t imp_type = NULL;
  sfixed32_t is_delivered = NULL;
  sfixed32_t is_dw = NULL;
  sfixed32_t publisher_id = NULL;
  sfixed32_t site_id = NULL;
  sfixed32_t content_category_id = NULL;
  sfixed32_t datacenter_id = NULL;
  real64_t eap = NULL;
  sfixed32_t user_tz_offset = NULL;
  sfixed32_t user_group_id = NULL;
  sfixed32_t pub_rule_id = NULL;
  sfixed32_t media_type = NULL;
  sfixed32_t operating_system = NULL;
  sfixed32_t browser = NULL;
  sfixed32_t language = NULL;
  string_t application_id = NULL;
  string_t user_locale = NULL;
  sfixed32_t inventory_url_id = NULL;
  sfixed32_t audit_type = NULL;
  real64_t shadow_price = NULL;
  sfixed32_t impbus_id = NULL;
  string_t buyer_currency = NULL;
  real64_t buyer_exchange_rate = NULL;
  string_t seller_currency = NULL;
  real64_t seller_exchange_rate = NULL;
  sfixed32_t vp_expose_domains = NULL;
  sfixed32_t vp_expose_categories = NULL;
  sfixed32_t vp_expose_pubs = NULL;
  sfixed32_t vp_expose_tag = NULL;
  sfixed32_t is_exclusive = NULL;
  sfixed32_t bidder_instance_id = NULL;
  sfixed32_t visibility_profile_id = NULL;
  sfixed32_t truncate_ip = NULL;
  sfixed32_t device_id = NULL;
  sfixed32_t carrier_id = NULL;
  sfixed32_t creative_audit_status = NULL;
  sfixed32_t is_creative_hosted = NULL;
  sfixed32_t city = NULL;
  string_t latitude = NULL;
  string_t longitude = NULL;
  string_t device_unique_id = NULL;
  sfixed32_t supply_type = NULL;
  sfixed32_t is_toolbar = NULL;
  sfixed32_t deal_id = NULL;
  sfixed64_t vp_bitmap = NULL;
  sfixed32_t ttl = NULL;
  sfixed32_t view_detection_enabled = NULL;
  sfixed32_t ozone_id = NULL;
  sfixed32_t is_performance = NULL;
  string_t sdk_version = NULL;
  sfixed32_t inventory_session_frequency = NULL;
  sfixed32_t bid_price_type = NULL;
  sfixed32_t device_type = NULL;
  sfixed32_t dma = NULL;
  string_t postal = NULL;
  sfixed32_t package_id = NULL;
  sfixed32_t spend_protection = NULL;
  sfixed32_t is_secure = NULL;
  real64_t estimated_view_rate = NULL;
  string_t external_request_id = NULL;
  sfixed32_t viewdef_definition_id_buyer_member = NULL;
  sfixed32_t spend_protection_pixel_id = NULL;
  string_t external_uid = NULL;
  string_t request_uuid = NULL;
  sfixed32_t mobile_app_instance_id = NULL;
  string_t traffic_source_code = NULL;
  string_t stitch_group_id = NULL;
  sfixed32_t deal_type = NULL;
  sfixed32_t ym_floor_id = NULL;
  sfixed32_t ym_bias_id = NULL;
  real64_t estimated_view_rate_over_total = NULL;
  sfixed32_t device_make_id = NULL;
  sfixed32_t operating_system_family_id = NULL;
  tag_size[length_t] tag_sizes = NULL;
  log_transaction_def seller_transaction_def = NULL;
  log_transaction_def buyer_transaction_def = NULL;
  log_predicted_video_view_info predicted_video_view_info = NULL;
  log_auction_url_info auction_url = NULL;
  sfixed32_t[length_t] allowed_media_types = NULL;
  bool_t is_imp_rejecter_applied = NULL;
  bool_t imp_rejecter_do_auction = NULL;
  log_location_info geo_location = NULL;
  real64_t seller_bid_currency_conversion_rate = NULL;
  string_t seller_bid_currency_code = NULL;
  bool_t is_prebid = NULL;
  string_t default_referrer_url = NULL;
  log_engagement_rate[length_t] engagement_rates = NULL;
  sfixed32_t fx_rate_snapshot_id = NULL;
  enum_t payment_type = NULL;
  sfixed32_t apply_cost_on_default = NULL;
  real64_t media_buy_cost = NULL;
  real64_t media_buy_rev_share_pct = NULL;
  sfixed32_t auction_duration_ms = NULL;
  sfixed32_t expected_events = NULL;
  log_anonymized_user_info anonymized_user_info = NULL;
  sfixed32_t region_id = NULL;
  sfixed32_t media_company_id = NULL;
  string_t gdpr_consent_cookie = NULL;
  bool_t subject_to_gdpr = NULL;
  sfixed32_t browser_code_id = NULL;
  sfixed32_t is_prebid_server_included = NULL;
  sfixed32_t seat_id = NULL;
  enum_t uid_source = NULL;
  bool_t is_whiteops_scanned = NULL;
  sfixed32_t pred_info = NULL;
  crossdevice_group[length_t] crossdevice_groups = NULL;
  bool_t is_amp = NULL;
  sfixed32_t hb_source = NULL;
  string_t external_campaign_id = NULL;
  log_product_ads log_product_ads = NULL;
  bool_t ss_native_assembly_enabled = NULL;
  real64_t emp = NULL;
  log_personal_identifier[length_t] personal_identifiers = NULL;
  log_personal_identifier[length_t] personal_identifiers_experimental = NULL;
  sfixed32_t postal_code_ext_id = NULL;
  string_t hashed_ip = NULL;
  string_t external_deal_code = NULL;
  sfixed32_t creative_duration = NULL;
  string_t openrtb_req_subdomain = NULL;
  sfixed32_t creative_media_subtype_id = NULL;
  bool_t is_private_auction = NULL;
  bool_t private_auction_eligible = NULL;
  string_t client_request_id = NULL;
  enum_t chrome_traffic_label = NULL;
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
end[int] log_impbus_impressions_message_types =
[vector
  [record
    name "tag_size"
    field_infos [vector
                  [record name "width" number 1 type_code 4 message_type -1 optional 0],
                  [record name "height" number 2 type_code 4 message_type -1 optional 0]]],
  [record
    name "log_transaction_def"
    field_infos [vector
                  [record name "transaction_event" number 1 type_code 4 message_type -1 optional 1],
                  [record name "transaction_event_type_id" number 2 type_code 4 message_type -1 optional 1]]],
  [record
    name "log_predicted_video_view_info"
    field_infos [vector
                  [record name "iab_view_rate_over_measured" number 1 type_code 1 message_type -1 optional 1],
                  [record name "iab_view_rate_over_total" number 2 type_code 1 message_type -1 optional 1],
                  [record name "predicted_100pv50pd_video_view_rate" number 3 type_code 1 message_type -1 optional 1],
                  [record name "predicted_100pv50pd_video_view_rate_over_total" number 4 type_code 1 message_type -1 optional 1],
                  [record name "video_completion_rate" number 5 type_code 1 message_type -1 optional 1],
                  [record name "view_prediction_source" number 6 type_code 4 message_type -1 optional 1]]],
  [record
    name "log_auction_url_info"
    field_infos [vector
                  [record name "site_url" number 1 type_code 8 message_type -1 optional 1]]],
  [record
    name "log_location_info"
    field_infos [vector
                  [record name "latitude" number 1 type_code 2 message_type -1 optional 1],
                  [record name "longitude" number 2 type_code 2 message_type -1 optional 1]]],
  [record
    name "log_engagement_rate"
    field_infos [vector
                  [record name "engagement_rate_type" number 1 type_code 4 message_type -1 optional 1],
                  [record name "rate" number 2 type_code 1 message_type -1 optional 1],
                  [record name "engagement_rate_type_id" number 3 type_code 4 message_type -1 optional 1]]],
  [record
    name "log_anonymized_user_info"
    field_infos [vector
                  [record name "user_id" number 1 type_code 8 message_type -1 optional 1]]],
  [record
    name "crossdevice_group"
    field_infos [vector
                  [record name "graph_id" number 1 type_code 4 message_type -1 optional 1],
                  [record name "group_id" number 2 type_code 3 message_type -1 optional 1]]],
  [record
    name "log_product_ads"
    field_infos [vector
                  [record name "product_feed_id" number 1 type_code 4 message_type -1 optional 1],
                  [record name "item_selection_strategy_id" number 2 type_code 4 message_type -1 optional 1],
                  [record name "product_uuid" number 3 type_code 8 message_type -1 optional 1]]],
  [record
    name "log_personal_identifier"
    field_infos [vector
                  [record name "identity_type" number 1 type_code 4 message_type -1 optional 0],
                  [record name "identity_value" number 2 type_code 8 message_type -1 optional 1]]],
  [record
    name "log_impbus_impressions"
    field_infos [vector
                  [record name "date_time" number 1 type_code 1 message_type -1 optional 0],
                  [record name "auction_id_64" number 2 type_code 1 message_type -1 optional 0],
                  [record name "user_id_64" number 3 type_code 1 message_type -1 optional 1],
                  [record name "tag_id" number 4 type_code 4 message_type -1 optional 1],
                  [record name "ip_address" number 5 type_code 8 message_type -1 optional 1],
                  [record name "venue_id" number 6 type_code 4 message_type -1 optional 1],
                  [record name "site_domain" number 7 type_code 8 message_type -1 optional 1],
                  [record name "width" number 8 type_code 4 message_type -1 optional 1],
                  [record name "height" number 9 type_code 4 message_type -1 optional 1],
                  [record name "geo_country" number 10 type_code 8 message_type -1 optional 1],
                  [record name "geo_region" number 11 type_code 8 message_type -1 optional 1],
                  [record name "gender" number 12 type_code 8 message_type -1 optional 1],
                  [record name "age" number 13 type_code 4 message_type -1 optional 1],
                  [record name "bidder_id" number 14 type_code 4 message_type -1 optional 1],
                  [record name "seller_member_id" number 15 type_code 4 message_type -1 optional 1],
                  [record name "buyer_member_id" number 16 type_code 4 message_type -1 optional 1],
                  [record name "creative_id" number 17 type_code 4 message_type -1 optional 1],
                  [record name "imp_blacklist_or_fraud" number 19 type_code 4 message_type -1 optional 1],
                  [record name "imp_bid_on" number 20 type_code 4 message_type -1 optional 1],
                  [record name "buyer_bid" number 21 type_code 1 message_type -1 optional 1],
                  [record name "buyer_spend" number 22 type_code 1 message_type -1 optional 1],
                  [record name "seller_revenue" number 23 type_code 1 message_type -1 optional 1],
                  [record name "num_of_bids" number 24 type_code 4 message_type -1 optional 1],
                  [record name "ecp" number 25 type_code 1 message_type -1 optional 1],
                  [record name "reserve_price" number 26 type_code 1 message_type -1 optional 1],
                  [record name "inv_code" number 27 type_code 8 message_type -1 optional 1],
                  [record name "call_type" number 28 type_code 8 message_type -1 optional 1],
                  [record name "inventory_source_id" number 29 type_code 4 message_type -1 optional 1],
                  [record name "cookie_age" number 30 type_code 4 message_type -1 optional 1],
                  [record name "brand_id" number 31 type_code 4 message_type -1 optional 1],
                  [record name "cleared_direct" number 32 type_code 4 message_type -1 optional 1],
                  [record name "forex_allowance" number 36 type_code 1 message_type -1 optional 1],
                  [record name "fold_position" number 38 type_code 4 message_type -1 optional 1],
                  [record name "external_inv_id" number 39 type_code 4 message_type -1 optional 1],
                  [record name "imp_type" number 40 type_code 4 message_type -1 optional 1],
                  [record name "is_delivered" number 41 type_code 4 message_type -1 optional 1],
                  [record name "is_dw" number 42 type_code 4 message_type -1 optional 1],
                  [record name "publisher_id" number 43 type_code 4 message_type -1 optional 1],
                  [record name "site_id" number 44 type_code 4 message_type -1 optional 1],
                  [record name "content_category_id" number 45 type_code 4 message_type -1 optional 1],
                  [record name "datacenter_id" number 47 type_code 4 message_type -1 optional 1],
                  [record name "eap" number 48 type_code 1 message_type -1 optional 1],
                  [record name "user_tz_offset" number 49 type_code 4 message_type -1 optional 1],
                  [record name "user_group_id" number 50 type_code 4 message_type -1 optional 1],
                  [record name "pub_rule_id" number 52 type_code 4 message_type -1 optional 1],
                  [record name "media_type" number 53 type_code 4 message_type -1 optional 1],
                  [record name "operating_system" number 54 type_code 4 message_type -1 optional 1],
                  [record name "browser" number 55 type_code 4 message_type -1 optional 1],
                  [record name "language" number 56 type_code 4 message_type -1 optional 1],
                  [record name "application_id" number 57 type_code 8 message_type -1 optional 1],
                  [record name "user_locale" number 58 type_code 8 message_type -1 optional 1],
                  [record name "inventory_url_id" number 59 type_code 4 message_type -1 optional 1],
                  [record name "audit_type" number 60 type_code 4 message_type -1 optional 1],
                  [record name "shadow_price" number 61 type_code 1 message_type -1 optional 1],
                  [record name "impbus_id" number 62 type_code 4 message_type -1 optional 1],
                  [record name "buyer_currency" number 63 type_code 8 message_type -1 optional 1],
                  [record name "buyer_exchange_rate" number 64 type_code 1 message_type -1 optional 1],
                  [record name "seller_currency" number 65 type_code 8 message_type -1 optional 1],
                  [record name "seller_exchange_rate" number 66 type_code 1 message_type -1 optional 1],
                  [record name "vp_expose_domains" number 67 type_code 4 message_type -1 optional 1],
                  [record name "vp_expose_categories" number 68 type_code 4 message_type -1 optional 1],
                  [record name "vp_expose_pubs" number 69 type_code 4 message_type -1 optional 1],
                  [record name "vp_expose_tag" number 70 type_code 4 message_type -1 optional 1],
                  [record name "is_exclusive" number 71 type_code 4 message_type -1 optional 1],
                  [record name "bidder_instance_id" number 72 type_code 4 message_type -1 optional 1],
                  [record name "visibility_profile_id" number 73 type_code 4 message_type -1 optional 1],
                  [record name "truncate_ip" number 74 type_code 4 message_type -1 optional 1],
                  [record name "device_id" number 75 type_code 4 message_type -1 optional 1],
                  [record name "carrier_id" number 76 type_code 4 message_type -1 optional 1],
                  [record name "creative_audit_status" number 77 type_code 4 message_type -1 optional 1],
                  [record name "is_creative_hosted" number 78 type_code 4 message_type -1 optional 1],
                  [record name "city" number 80 type_code 4 message_type -1 optional 1],
                  [record name "latitude" number 81 type_code 8 message_type -1 optional 1],
                  [record name "longitude" number 82 type_code 8 message_type -1 optional 1],
                  [record name "device_unique_id" number 83 type_code 8 message_type -1 optional 1],
                  [record name "supply_type" number 84 type_code 4 message_type -1 optional 1],
                  [record name "is_toolbar" number 85 type_code 4 message_type -1 optional 1],
                  [record name "deal_id" number 86 type_code 4 message_type -1 optional 1],
                  [record name "vp_bitmap" number 87 type_code 3 message_type -1 optional 1],
                  [record name "ttl" number 88 type_code 4 message_type -1 optional 1],
                  [record name "view_detection_enabled" number 89 type_code 4 message_type -1 optional 1],
                  [record name "ozone_id" number 90 type_code 4 message_type -1 optional 1],
                  [record name "is_performance" number 91 type_code 4 message_type -1 optional 1],
                  [record name "sdk_version" number 92 type_code 8 message_type -1 optional 1],
                  [record name "inventory_session_frequency" number 93 type_code 4 message_type -1 optional 1],
                  [record name "bid_price_type" number 94 type_code 4 message_type -1 optional 1],
                  [record name "device_type" number 95 type_code 4 message_type -1 optional 1],
                  [record name "dma" number 96 type_code 4 message_type -1 optional 1],
                  [record name "postal" number 97 type_code 8 message_type -1 optional 1],
                  [record name "package_id" number 98 type_code 4 message_type -1 optional 1],
                  [record name "spend_protection" number 99 type_code 4 message_type -1 optional 1],
                  [record name "is_secure" number 100 type_code 4 message_type -1 optional 1],
                  [record name "estimated_view_rate" number 101 type_code 1 message_type -1 optional 1],
                  [record name "external_request_id" number 102 type_code 8 message_type -1 optional 1],
                  [record name "viewdef_definition_id_buyer_member" number 103 type_code 4 message_type -1 optional 1],
                  [record name "spend_protection_pixel_id" number 104 type_code 4 message_type -1 optional 1],
                  [record name "external_uid" number 105 type_code 8 message_type -1 optional 1],
                  [record name "request_uuid" number 106 type_code 8 message_type -1 optional 1],
                  [record name "mobile_app_instance_id" number 107 type_code 4 message_type -1 optional 1],
                  [record name "traffic_source_code" number 108 type_code 8 message_type -1 optional 1],
                  [record name "stitch_group_id" number 109 type_code 8 message_type -1 optional 1],
                  [record name "deal_type" number 110 type_code 4 message_type -1 optional 1],
                  [record name "ym_floor_id" number 111 type_code 4 message_type -1 optional 1],
                  [record name "ym_bias_id" number 112 type_code 4 message_type -1 optional 1],
                  [record name "estimated_view_rate_over_total" number 113 type_code 1 message_type -1 optional 1],
                  [record name "device_make_id" number 114 type_code 4 message_type -1 optional 1],
                  [record name "operating_system_family_id" number 115 type_code 4 message_type -1 optional 1],
                  [record name "tag_sizes" number 116 type_code 25 message_type 0 optional 1],
                  [record name "seller_transaction_def" number 117 type_code 9 message_type 1 optional 1],
                  [record name "buyer_transaction_def" number 118 type_code 9 message_type 1 optional 1],
                  [record name "predicted_video_view_info" number 119 type_code 9 message_type 2 optional 1],
                  [record name "auction_url" number 120 type_code 9 message_type 3 optional 1],
                  [record name "allowed_media_types" number 121 type_code 20 message_type -1 optional 1],
                  [record name "is_imp_rejecter_applied" number 122 type_code 7 message_type -1 optional 1],
                  [record name "imp_rejecter_do_auction" number 123 type_code 7 message_type -1 optional 1],
                  [record name "geo_location" number 124 type_code 9 message_type 4 optional 1],
                  [record name "seller_bid_currency_conversion_rate" number 125 type_code 1 message_type -1 optional 1],
                  [record name "seller_bid_currency_code" number 126 type_code 8 message_type -1 optional 1],
                  [record name "is_prebid" number 127 type_code 7 message_type -1 optional 1],
                  [record name "default_referrer_url" number 128 type_code 8 message_type -1 optional 1],
                  [record name "engagement_rates" number 129 type_code 25 message_type 5 optional 1],
                  [record name "fx_rate_snapshot_id" number 130 type_code 4 message_type -1 optional 1],
                  [record name "payment_type" number 131 type_code 4 message_type -1 optional 1],
                  [record name "apply_cost_on_default" number 132 type_code 4 message_type -1 optional 1],
                  [record name "media_buy_cost" number 133 type_code 1 message_type -1 optional 1],
                  [record name "media_buy_rev_share_pct" number 134 type_code 1 message_type -1 optional 1],
                  [record name "auction_duration_ms" number 135 type_code 4 message_type -1 optional 1],
                  [record name "expected_events" number 136 type_code 4 message_type -1 optional 1],
                  [record name "anonymized_user_info" number 137 type_code 9 message_type 6 optional 1],
                  [record name "region_id" number 138 type_code 4 message_type -1 optional 1],
                  [record name "media_company_id" number 139 type_code 4 message_type -1 optional 1],
                  [record name "gdpr_consent_cookie" number 140 type_code 8 message_type -1 optional 1],
                  [record name "subject_to_gdpr" number 141 type_code 7 message_type -1 optional 1],
                  [record name "browser_code_id" number 142 type_code 4 message_type -1 optional 1],
                  [record name "is_prebid_server_included" number 143 type_code 4 message_type -1 optional 1],
                  [record name "seat_id" number 144 type_code 4 message_type -1 optional 1],
                  [record name "uid_source" number 145 type_code 4 message_type -1 optional 1],
                  [record name "is_whiteops_scanned" number 147 type_code 7 message_type -1 optional 1],
                  [record name "pred_info" number 148 type_code 4 message_type -1 optional 1],
                  [record name "crossdevice_groups" number 149 type_code 25 message_type 7 optional 1],
                  [record name "is_amp" number 150 type_code 7 message_type -1 optional 1],
                  [record name "hb_source" number 151 type_code 4 message_type -1 optional 1],
                  [record name "external_campaign_id" number 152 type_code 8 message_type -1 optional 1],
                  [record name "log_product_ads" number 153 type_code 9 message_type 8 optional 1],
                  [record name "ss_native_assembly_enabled" number 154 type_code 7 message_type -1 optional 1],
                  [record name "emp" number 155 type_code 1 message_type -1 optional 1],
                  [record name "personal_identifiers" number 157 type_code 25 message_type 9 optional 1],
                  [record name "personal_identifiers_experimental" number 158 type_code 25 message_type 9 optional 1],
                  [record name "postal_code_ext_id" number 159 type_code 4 message_type -1 optional 1],
                  [record name "hashed_ip" number 160 type_code 8 message_type -1 optional 1],
                  [record name "external_deal_code" number 161 type_code 8 message_type -1 optional 1],
                  [record name "creative_duration" number 162 type_code 4 message_type -1 optional 1],
                  [record name "openrtb_req_subdomain" number 165 type_code 8 message_type -1 optional 1],
                  [record name "creative_media_subtype_id" number 166 type_code 4 message_type -1 optional 1],
                  [record name "is_private_auction" number 167 type_code 7 message_type -1 optional 1],
                  [record name "private_auction_eligible" number 168 type_code 7 message_type -1 optional 1],
                  [record name "client_request_id" number 169 type_code 8 message_type -1 optional 1],
                  [record name "chrome_traffic_label" number 170 type_code 4 message_type -1 optional 1]]]];
metadata type = log_impbus_impressions;
metadata type = "log_impbus_impressions" ;""")
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
