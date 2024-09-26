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

object Partition_by_Key_log_impbus_impressions_auction_id_UnionAll_normalize_schema_2 {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(
      col("date_time").cast(LongType).as("date_time"),
      col("auction_id_64").cast(LongType).as("auction_id_64"),
      col("user_id_64").cast(LongType).as("user_id_64"),
      col("tag_id").cast(IntegerType).as("tag_id"),
      col("ip_address").cast(StringType).as("ip_address"),
      col("venue_id").cast(IntegerType).as("venue_id"),
      col("site_domain").cast(StringType).as("site_domain"),
      col("width").cast(IntegerType).as("width"),
      col("height").cast(IntegerType).as("height"),
      col("geo_country").cast(StringType).as("geo_country"),
      col("geo_region").cast(StringType).as("geo_region"),
      col("gender").cast(StringType).as("gender"),
      col("age").cast(IntegerType).as("age"),
      col("bidder_id").cast(IntegerType).as("bidder_id"),
      col("seller_member_id").cast(IntegerType).as("seller_member_id"),
      col("buyer_member_id").cast(IntegerType).as("buyer_member_id"),
      col("creative_id").cast(IntegerType).as("creative_id"),
      col("imp_blacklist_or_fraud")
        .cast(IntegerType)
        .as("imp_blacklist_or_fraud"),
      col("imp_bid_on").cast(IntegerType).as("imp_bid_on"),
      col("buyer_bid").cast(DoubleType).as("buyer_bid"),
      col("buyer_spend").cast(DoubleType).as("buyer_spend"),
      col("seller_revenue").cast(DoubleType).as("seller_revenue"),
      col("num_of_bids").cast(IntegerType).as("num_of_bids"),
      col("ecp").cast(DoubleType).as("ecp"),
      col("reserve_price").cast(DoubleType).as("reserve_price"),
      col("inv_code").cast(StringType).as("inv_code"),
      col("call_type").cast(StringType).as("call_type"),
      col("inventory_source_id").cast(IntegerType).as("inventory_source_id"),
      col("cookie_age").cast(IntegerType).as("cookie_age"),
      col("brand_id").cast(IntegerType).as("brand_id"),
      col("cleared_direct").cast(IntegerType).as("cleared_direct"),
      col("forex_allowance").cast(DoubleType).as("forex_allowance"),
      col("fold_position").cast(IntegerType).as("fold_position"),
      col("external_inv_id").cast(IntegerType).as("external_inv_id"),
      col("imp_type").cast(IntegerType).as("imp_type"),
      col("is_delivered").cast(IntegerType).as("is_delivered"),
      col("is_dw").cast(IntegerType).as("is_dw"),
      col("publisher_id").cast(IntegerType).as("publisher_id"),
      col("site_id").cast(IntegerType).as("site_id"),
      col("content_category_id").cast(IntegerType).as("content_category_id"),
      col("datacenter_id").cast(IntegerType).as("datacenter_id"),
      col("eap").cast(DoubleType).as("eap"),
      col("user_tz_offset").cast(IntegerType).as("user_tz_offset"),
      col("user_group_id").cast(IntegerType).as("user_group_id"),
      col("pub_rule_id").cast(IntegerType).as("pub_rule_id"),
      col("media_type").cast(IntegerType).as("media_type"),
      col("operating_system").cast(IntegerType).as("operating_system"),
      col("browser").cast(IntegerType).as("browser"),
      col("language").cast(IntegerType).as("language"),
      col("application_id").cast(StringType).as("application_id"),
      col("user_locale").cast(StringType).as("user_locale"),
      col("inventory_url_id").cast(IntegerType).as("inventory_url_id"),
      col("audit_type").cast(IntegerType).as("audit_type"),
      col("shadow_price").cast(DoubleType).as("shadow_price"),
      col("impbus_id").cast(IntegerType).as("impbus_id"),
      col("buyer_currency").cast(StringType).as("buyer_currency"),
      col("buyer_exchange_rate").cast(DoubleType).as("buyer_exchange_rate"),
      col("seller_currency").cast(StringType).as("seller_currency"),
      col("seller_exchange_rate").cast(DoubleType).as("seller_exchange_rate"),
      col("vp_expose_domains").cast(IntegerType).as("vp_expose_domains"),
      col("vp_expose_categories").cast(IntegerType).as("vp_expose_categories"),
      col("vp_expose_pubs").cast(IntegerType).as("vp_expose_pubs"),
      col("vp_expose_tag").cast(IntegerType).as("vp_expose_tag"),
      col("is_exclusive").cast(IntegerType).as("is_exclusive"),
      col("bidder_instance_id").cast(IntegerType).as("bidder_instance_id"),
      col("visibility_profile_id")
        .cast(IntegerType)
        .as("visibility_profile_id"),
      col("truncate_ip").cast(IntegerType).as("truncate_ip"),
      col("device_id").cast(IntegerType).as("device_id"),
      col("carrier_id").cast(IntegerType).as("carrier_id"),
      col("creative_audit_status")
        .cast(IntegerType)
        .as("creative_audit_status"),
      col("is_creative_hosted").cast(IntegerType).as("is_creative_hosted"),
      col("city").cast(IntegerType).as("city"),
      col("latitude").cast(StringType).as("latitude"),
      col("longitude").cast(StringType).as("longitude"),
      col("device_unique_id").cast(StringType).as("device_unique_id"),
      col("supply_type").cast(IntegerType).as("supply_type"),
      col("is_toolbar").cast(IntegerType).as("is_toolbar"),
      col("deal_id").cast(IntegerType).as("deal_id"),
      col("vp_bitmap").cast(LongType).as("vp_bitmap"),
      col("ttl").cast(IntegerType).as("ttl"),
      col("view_detection_enabled")
        .cast(IntegerType)
        .as("view_detection_enabled"),
      col("ozone_id").cast(IntegerType).as("ozone_id"),
      col("is_performance").cast(IntegerType).as("is_performance"),
      col("sdk_version").cast(StringType).as("sdk_version"),
      col("inventory_session_frequency")
        .cast(IntegerType)
        .as("inventory_session_frequency"),
      col("bid_price_type").cast(IntegerType).as("bid_price_type"),
      col("device_type").cast(IntegerType).as("device_type"),
      col("dma").cast(IntegerType).as("dma"),
      col("postal").cast(StringType).as("postal"),
      col("package_id").cast(IntegerType).as("package_id"),
      col("spend_protection").cast(IntegerType).as("spend_protection"),
      col("is_secure").cast(IntegerType).as("is_secure"),
      col("estimated_view_rate").cast(DoubleType).as("estimated_view_rate"),
      col("external_request_id").cast(StringType).as("external_request_id"),
      col("viewdef_definition_id_buyer_member")
        .cast(IntegerType)
        .as("viewdef_definition_id_buyer_member"),
      col("spend_protection_pixel_id")
        .cast(IntegerType)
        .as("spend_protection_pixel_id"),
      col("external_uid").cast(StringType).as("external_uid"),
      col("request_uuid").cast(StringType).as("request_uuid"),
      col("mobile_app_instance_id")
        .cast(IntegerType)
        .as("mobile_app_instance_id"),
      col("traffic_source_code").cast(StringType).as("traffic_source_code"),
      col("stitch_group_id").cast(StringType).as("stitch_group_id"),
      col("deal_type").cast(IntegerType).as("deal_type"),
      col("ym_floor_id").cast(IntegerType).as("ym_floor_id"),
      col("ym_bias_id").cast(IntegerType).as("ym_bias_id"),
      col("estimated_view_rate_over_total")
        .cast(DoubleType)
        .as("estimated_view_rate_over_total"),
      col("device_make_id").cast(IntegerType).as("device_make_id"),
      col("operating_system_family_id")
        .cast(IntegerType)
        .as("operating_system_family_id"),
      col("tag_sizes")
        .cast(
          ArrayType(StructType(
                      Array(StructField("width",  IntegerType, true),
                            StructField("height", IntegerType, true)
                      )
                    ),
                    true
          )
        )
        .as("tag_sizes"),
      when(
        is_not_null(col("seller_transaction_def")),
        struct(
          col("seller_transaction_def.transaction_event").as(
            "transaction_event"
          ),
          col("seller_transaction_def.transaction_event_type_id").as(
            "transaction_event_type_id"
          )
        )
      ).cast(
          StructType(
            Array(StructField("transaction_event",         IntegerType, true),
                  StructField("transaction_event_type_id", IntegerType, true)
            )
          )
        )
        .as("seller_transaction_def"),
      when(
        is_not_null(col("buyer_transaction_def")),
        struct(
          col("buyer_transaction_def.transaction_event").as(
            "transaction_event"
          ),
          col("buyer_transaction_def.transaction_event_type_id").as(
            "transaction_event_type_id"
          )
        )
      ).cast(
          StructType(
            Array(StructField("transaction_event",         IntegerType, true),
                  StructField("transaction_event_type_id", IntegerType, true)
            )
          )
        )
        .as("buyer_transaction_def"),
      when(
        is_not_null(col("predicted_video_view_info")),
        struct(
          col("predicted_video_view_info.iab_view_rate_over_measured").as(
            "iab_view_rate_over_measured"
          ),
          col("predicted_video_view_info.iab_view_rate_over_total").as(
            "iab_view_rate_over_total"
          ),
          col("predicted_video_view_info.predicted_100pv50pd_video_view_rate")
            .as("predicted_100pv50pd_video_view_rate"),
          col(
            "predicted_video_view_info.predicted_100pv50pd_video_view_rate_over_total"
          ).as("predicted_100pv50pd_video_view_rate_over_total"),
          col("predicted_video_view_info.video_completion_rate").as(
            "video_completion_rate"
          ),
          col("predicted_video_view_info.view_prediction_source").as(
            "view_prediction_source"
          )
        )
      ).cast(
          StructType(
            Array(
              StructField("iab_view_rate_over_measured", DoubleType, true),
              StructField("iab_view_rate_over_total",    DoubleType, true),
              StructField("predicted_100pv50pd_video_view_rate",
                          DoubleType,
                          true
              ),
              StructField("predicted_100pv50pd_video_view_rate_over_total",
                          DoubleType,
                          true
              ),
              StructField("video_completion_rate",  DoubleType,  true),
              StructField("view_prediction_source", IntegerType, true)
            )
          )
        )
        .as("predicted_video_view_info"),
      when(is_not_null(col("auction_url")),
           struct(col("auction_url.site_url").as("site_url"))
      ).cast(StructType(Array(StructField("site_url", StringType, true))))
        .as("auction_url"),
      col("allowed_media_types")
        .cast(ArrayType(IntegerType, true))
        .as("allowed_media_types"),
      col("is_imp_rejecter_applied")
        .cast(BooleanType)
        .as("is_imp_rejecter_applied"),
      col("imp_rejecter_do_auction")
        .cast(BooleanType)
        .as("imp_rejecter_do_auction"),
      when(is_not_null(col("geo_location")),
           struct(col("geo_location.latitude").as("latitude"),
                  col("geo_location.longitude").as("longitude")
           )
      ).cast(
          StructType(
            Array(StructField("latitude",  FloatType, true),
                  StructField("longitude", FloatType, true)
            )
          )
        )
        .as("geo_location"),
      col("seller_bid_currency_conversion_rate")
        .cast(DoubleType)
        .as("seller_bid_currency_conversion_rate"),
      col("seller_bid_currency_code")
        .cast(StringType)
        .as("seller_bid_currency_code"),
      col("is_prebid").cast(BooleanType).as("is_prebid"),
      col("default_referrer_url").cast(StringType).as("default_referrer_url"),
      col("engagement_rates")
        .cast(
          ArrayType(
            StructType(
              Array(StructField("engagement_rate_type",    IntegerType, true),
                    StructField("rate",                    DoubleType,  true),
                    StructField("engagement_rate_type_id", IntegerType, true)
              )
            ),
            true
          )
        )
        .as("engagement_rates"),
      col("fx_rate_snapshot_id").cast(IntegerType).as("fx_rate_snapshot_id"),
      col("payment_type").cast(IntegerType).as("payment_type"),
      col("apply_cost_on_default")
        .cast(IntegerType)
        .as("apply_cost_on_default"),
      col("media_buy_cost").cast(DoubleType).as("media_buy_cost"),
      col("media_buy_rev_share_pct")
        .cast(DoubleType)
        .as("media_buy_rev_share_pct"),
      col("auction_duration_ms").cast(IntegerType).as("auction_duration_ms"),
      col("expected_events").cast(IntegerType).as("expected_events"),
      when(is_not_null(col("anonymized_user_info")),
           struct(col("anonymized_user_info.user_id").as("user_id"))
      ).cast(StructType(Array(StructField("user_id", BinaryType, true))))
        .as("anonymized_user_info"),
      col("region_id").cast(IntegerType).as("region_id"),
      col("media_company_id").cast(IntegerType).as("media_company_id"),
      col("gdpr_consent_cookie").cast(StringType).as("gdpr_consent_cookie"),
      col("subject_to_gdpr").cast(BooleanType).as("subject_to_gdpr"),
      col("browser_code_id").cast(IntegerType).as("browser_code_id"),
      col("is_prebid_server_included")
        .cast(IntegerType)
        .as("is_prebid_server_included"),
      col("seat_id").cast(IntegerType).as("seat_id"),
      col("uid_source").cast(IntegerType).as("uid_source"),
      col("is_whiteops_scanned").cast(BooleanType).as("is_whiteops_scanned"),
      col("pred_info").cast(IntegerType).as("pred_info"),
      col("crossdevice_groups")
        .cast(
          ArrayType(StructType(
                      Array(StructField("graph_id", IntegerType, true),
                            StructField("group_id", LongType,    true)
                      )
                    ),
                    true
          )
        )
        .as("crossdevice_groups"),
      col("is_amp").cast(BooleanType).as("is_amp"),
      col("hb_source").cast(IntegerType).as("hb_source"),
      col("external_campaign_id").cast(StringType).as("external_campaign_id"),
      when(
        is_not_null(col("log_product_ads")),
        struct(
          col("log_product_ads.product_feed_id").as("product_feed_id"),
          col("log_product_ads.item_selection_strategy_id").as(
            "item_selection_strategy_id"
          ),
          col("log_product_ads.product_uuid").as("product_uuid")
        )
      ).cast(
          StructType(
            Array(
              StructField("product_feed_id",            IntegerType, true),
              StructField("item_selection_strategy_id", IntegerType, true),
              StructField("product_uuid",               StringType,  true)
            )
          )
        )
        .as("log_product_ads"),
      col("ss_native_assembly_enabled")
        .cast(BooleanType)
        .as("ss_native_assembly_enabled"),
      col("emp").cast(DoubleType).as("emp"),
      col("personal_identifiers")
        .cast(
          ArrayType(StructType(
                      Array(StructField("identity_type",  IntegerType, true),
                            StructField("identity_value", StringType,  true)
                      )
                    ),
                    true
          )
        )
        .as("personal_identifiers"),
      col("personal_identifiers_experimental")
        .cast(
          ArrayType(StructType(
                      Array(StructField("identity_type",  IntegerType, true),
                            StructField("identity_value", StringType,  true)
                      )
                    ),
                    true
          )
        )
        .as("personal_identifiers_experimental"),
      col("postal_code_ext_id").cast(IntegerType).as("postal_code_ext_id"),
      col("hashed_ip").cast(StringType).as("hashed_ip"),
      col("external_deal_code").cast(StringType).as("external_deal_code"),
      col("creative_duration").cast(IntegerType).as("creative_duration"),
      col("openrtb_req_subdomain").cast(StringType).as("openrtb_req_subdomain"),
      col("creative_media_subtype_id")
        .cast(IntegerType)
        .as("creative_media_subtype_id"),
      col("is_private_auction").cast(BooleanType).as("is_private_auction"),
      col("private_auction_eligible")
        .cast(BooleanType)
        .as("private_auction_eligible"),
      col("client_request_id").cast(StringType).as("client_request_id"),
      col("chrome_traffic_label").cast(IntegerType).as("chrome_traffic_label")
    )

}