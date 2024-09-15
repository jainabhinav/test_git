package io.prophecy.pipelines.first_agg_platform_video_analytics.graph

import io.prophecy.libs._
import io.prophecy.pipelines.first_agg_platform_video_analytics.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object temp_output1 {

  def apply(context: Context): DataFrame =
    context.spark.read
      .format("csv")
      .option("header", true)
      .option("sep",    ",")
      .schema(
        StructType(
          Array(
            StructField("auction_id_64", LongType, true),
            StructField("date_time",     LongType, true),
            StructField(
              "agg_platform_video_requests",
              StructType(
                Array(
                  StructField("date_time",                  LongType,    true),
                  StructField("auction_id_64",              LongType,    true),
                  StructField("tag_id",                     IntegerType, true),
                  StructField("width",                      IntegerType, true),
                  StructField("height",                     IntegerType, true),
                  StructField("geo_country",                StringType,  true),
                  StructField("geo_region",                 StringType,  true),
                  StructField("seller_member_id",           IntegerType, true),
                  StructField("imp_blacklist_or_fraud",     IntegerType, true),
                  StructField("call_type",                  StringType,  true),
                  StructField("brand_id",                   IntegerType, true),
                  StructField("imp_type",                   IntegerType, true),
                  StructField("publisher_id",               IntegerType, true),
                  StructField("site_id",                    IntegerType, true),
                  StructField("content_category_id",        IntegerType, true),
                  StructField("media_type",                 IntegerType, true),
                  StructField("browser",                    IntegerType, true),
                  StructField("language",                   IntegerType, true),
                  StructField("application_id",             StringType,  true),
                  StructField("city",                       IntegerType, true),
                  StructField("supply_type",                IntegerType, true),
                  StructField("deal_id",                    IntegerType, true),
                  StructField("vp_bitmap",                  LongType,    true),
                  StructField("device_type",                IntegerType, true),
                  StructField("view_detection_enabled",     IntegerType, true),
                  StructField("viewdef_definition_id",      IntegerType, true),
                  StructField("request_uuid",               StringType,  true),
                  StructField("operating_system_id",        IntegerType, true),
                  StructField("operating_system_family_id", IntegerType, true),
                  StructField("allowed_media_types",
                              ArrayType(IntegerType, true),
                              true
                  ),
                  StructField("is_imp_rejecter_applied", BooleanType, true),
                  StructField("imp_rejecter_do_auction", BooleanType, true),
                  StructField("imp_bid_on",              IntegerType, true),
                  StructField("is_prebid",               BooleanType, true),
                  StructField("placement_set_id",        IntegerType, true),
                  StructField("max_duration",            IntegerType, true),
                  StructField("max_number_ads",          IntegerType, true),
                  StructField("supported_playback_methods",
                              ArrayType(IntegerType, true),
                              true
                  ),
                  StructField("video_context",            IntegerType,                  true),
                  StructField("player_width",             IntegerType,                  true),
                  StructField("player_height",            IntegerType,                  true),
                  StructField("start_delay",              IntegerType,                  true),
                  StructField("frameworks",               ArrayType(IntegerType, true), true),
                  StructField("protocols",                ArrayType(IntegerType, true), true),
                  StructField("supports_skippable",       BooleanType,                  true),
                  StructField("min_ad_duration",          IntegerType,                  true),
                  StructField("max_ad_duration",          IntegerType,                  true),
                  StructField("slot_type",                IntegerType,                  true),
                  StructField("ad_slot_position",         IntegerType,                  true),
                  StructField("inventory_url_id",         IntegerType,                  true),
                  StructField("is_dw_seller",             IntegerType,                  true),
                  StructField("is_mediated",              BooleanType,                  true),
                  StructField("num_of_bids",              IntegerType,                  true),
                  StructField("buyer_bid",                DoubleType,                   true),
                  StructField("creative_id",              IntegerType,                  true),
                  StructField("filled_number_ads",        IntegerType,                  true),
                  StructField("max_slot_duration",        IntegerType,                  true),
                  StructField("filled_slot_duration",     IntegerType,                  true),
                  StructField("mobile_app_instance_id",   IntegerType,                  true),
                  StructField("user_id_64",               LongType,                     true),
                  StructField("instance_id",              IntegerType,                  true),
                  StructField("hb_source",                IntegerType,                  true),
                  StructField("content_delivery_type_id", IntegerType,                  true),
                  StructField("content_duration_secs",    IntegerType,                  true),
                  StructField("content_genre_id",         IntegerType,                  true),
                  StructField("content_program_type_id",  IntegerType,                  true),
                  StructField("content_rating_id",        IntegerType,                  true),
                  StructField("content_network_id",       IntegerType,                  true),
                  StructField("content_language_id",      IntegerType,                  true),
                  StructField("external_deal_code",       StringType,                   true),
                  StructField("creative_duration",        IntegerType,                  true),
                  StructField("pod_id_64",                LongType,                     true),
                  StructField("imp_requests",             LongType,                     true),
                  StructField("region_id",                IntegerType,                  true)
                )
              ),
              true
            ),
            StructField(
              "agg_dw_video_events",
              StructType(
                Array(
                  StructField("date_time",                  LongType,    true),
                  StructField("auction_id_64",              LongType,    true),
                  StructField("buyer_member_id",            IntegerType, true),
                  StructField("seller_member_id",           IntegerType, true),
                  StructField("advertiser_id",              IntegerType, true),
                  StructField("publisher_id",               IntegerType, true),
                  StructField("site_id",                    IntegerType, true),
                  StructField("tag_id",                     IntegerType, true),
                  StructField("insertion_order_id",         IntegerType, true),
                  StructField("campaign_group_id",          IntegerType, true),
                  StructField("campaign_id",                IntegerType, true),
                  StructField("creative_id",                IntegerType, true),
                  StructField("creative_freq",              IntegerType, true),
                  StructField("creative_rec",               IntegerType, true),
                  StructField("brand_id",                   IntegerType, true),
                  StructField("geo_country",                StringType,  true),
                  StructField("width",                      IntegerType, true),
                  StructField("height",                     IntegerType, true),
                  StructField("deal_id",                    IntegerType, true),
                  StructField("video_was_served",           IntegerType, true),
                  StructField("video_started",              IntegerType, true),
                  StructField("video_was_skipped",          IntegerType, true),
                  StructField("video_had_error",            IntegerType, true),
                  StructField("video_hit_25_pct",           IntegerType, true),
                  StructField("video_hit_50_pct",           IntegerType, true),
                  StructField("video_hit_75_pct",           IntegerType, true),
                  StructField("video_completed",            IntegerType, true),
                  StructField("imp_type",                   IntegerType, true),
                  StructField("advertiser_currency",        StringType,  true),
                  StructField("publisher_currency",         StringType,  true),
                  StructField("site_domain",                StringType,  true),
                  StructField("application_id",             StringType,  true),
                  StructField("media_cost_dollars_cpm",     DoubleType,  true),
                  StructField("booked_revenue_dollars",     DoubleType,  true),
                  StructField("seller_revenue_cpm",         DoubleType,  true),
                  StructField("vp_bitmap",                  LongType,    true),
                  StructField("buyer_currency",             StringType,  true),
                  StructField("is_learn",                   IntegerType, true),
                  StructField("external_inv_id",            IntegerType, true),
                  StructField("pub_rule_id",                IntegerType, true),
                  StructField("predict_type_rev",           IntegerType, true),
                  StructField("predict_type_goal",          IntegerType, true),
                  StructField("media_type",                 IntegerType, true),
                  StructField("venue_id",                   IntegerType, true),
                  StructField("payment_type",               IntegerType, true),
                  StructField("revenue_type",               IntegerType, true),
                  StructField("viewdef_definition_id",      IntegerType, true),
                  StructField("advertiser_exchange_rate",   DoubleType,  true),
                  StructField("publisher_exchange_rate",    DoubleType,  true),
                  StructField("vp_expose_pubs",             IntegerType, true),
                  StructField("vp_expose_tag",              IntegerType, true),
                  StructField("vp_expose_categories",       IntegerType, true),
                  StructField("playback_method",            IntegerType, true),
                  StructField("video_context",              IntegerType, true),
                  StructField("player_size_id",             IntegerType, true),
                  StructField("supply_type",                IntegerType, true),
                  StructField("vp_expose_domains",          IntegerType, true),
                  StructField("view_result",                IntegerType, true),
                  StructField("view_non_measurable_reason", IntegerType, true),
                  StructField("error_code",                 IntegerType, true),
                  StructField("request_imp_type",           IntegerType, true),
                  StructField("call_type",                  StringType,  true),
                  StructField("companion_creative_id",      IntegerType, true),
                  StructField("companion_imps",             LongType,    true),
                  StructField("companion_clicks",           LongType,    true),
                  StructField("user_id_64",                 LongType,    true),
                  StructField("geo_region",                 StringType,  true),
                  StructField("gender",                     StringType,  true),
                  StructField("age",                        IntegerType, true),
                  StructField("operating_system",           IntegerType, true),
                  StructField("browser",                    IntegerType, true),
                  StructField("language",                   IntegerType, true),
                  StructField("latitude",                   StringType,  true),
                  StructField("longitude",                  StringType,  true),
                  StructField("device_id",                  IntegerType, true),
                  StructField("carrier_id",                 IntegerType, true),
                  StructField("device_type",                IntegerType, true),
                  StructField("dma",                        IntegerType, true),
                  StructField("postal",                     StringType,  true),
                  StructField("city",                       IntegerType, true),
                  StructField("request_uuid",               StringType,  true),
                  StructField(
                    "tag_sizes",
                    ArrayType(StructType(
                                Array(StructField("width",  IntegerType, true),
                                      StructField("height", IntegerType, true)
                                )
                              ),
                              true
                    ),
                    true
                  ),
                  StructField("user_tz_offset", IntegerType, true),
                  StructField("fold_position",  IntegerType, true),
                  StructField(
                    "anonymized_user_info",
                    StructType(Array(StructField("user_id", BinaryType, true))),
                    true
                  ),
                  StructField(
                    "personal_data",
                    StructType(
                      Array(
                        StructField("user_id_64",       LongType,   true),
                        StructField("device_unique_id", StringType, true),
                        StructField("external_uid",     StringType, true),
                        StructField("ip_address",       BinaryType, true),
                        StructField(
                          "crossdevice_group",
                          StructType(
                            Array(StructField("graph_id", IntegerType, true),
                                  StructField("group_id", LongType,    true)
                            )
                          ),
                          true
                        ),
                        StructField("latitude",            DoubleType,  true),
                        StructField("longitude",           DoubleType,  true),
                        StructField("ipv6_address",        BinaryType,  true),
                        StructField("subject_to_gdpr",     BooleanType, true),
                        StructField("geo_country",         StringType,  true),
                        StructField("gdpr_consent_string", StringType,  true),
                        StructField("preempt_ip_address",  BinaryType,  true),
                        StructField("device_type",         IntegerType, true),
                        StructField("device_make_id",      IntegerType, true),
                        StructField("device_model_id",     IntegerType, true),
                        StructField("new_user_id_64",      LongType,    true),
                        StructField("is_service_provider_mode",
                                    BooleanType,
                                    true
                        ),
                        StructField("is_personal_info_sale", BooleanType, true),
                        StructField("ip_derived_latitude",   FloatType,   true),
                        StructField("ip_derived_longitude",  FloatType,   true),
                        StructField("allow_user_data",       BooleanType, true),
                        StructField("us_privacy_is_in_effect",
                                    BooleanType,
                                    true
                        )
                      )
                    ),
                    true
                  ),
                  StructField("inventory_url_id", IntegerType, true),
                  StructField("predicted_100pd_video_completion_rate",
                              DoubleType,
                              true
                  ),
                  StructField("inventory_source_id",  IntegerType, true),
                  StructField("session_frequency",    IntegerType, true),
                  StructField("is_control",           IntegerType, true),
                  StructField("device_unique_id",     StringType,  true),
                  StructField("targeted_segments",    StringType,  true),
                  StructField("seller_currency",      StringType,  true),
                  StructField("is_toolbar",           IntegerType, true),
                  StructField("control_pct",          DoubleType,  true),
                  StructField("advertiser_frequency", IntegerType, true),
                  StructField("advertiser_recency",   IntegerType, true),
                  StructField("ozone_id",             IntegerType, true),
                  StructField("is_performance",       IntegerType, true),
                  StructField("sdk_version",          StringType,  true),
                  StructField("media_buy_id",         IntegerType, true),
                  StructField("camp_dp_id",           IntegerType, true),
                  StructField("user_group_id",        IntegerType, true),
                  StructField("package_id",           IntegerType, true),
                  StructField("campaign_group_freq",  IntegerType, true),
                  StructField("campaign_group_rec",   IntegerType, true),
                  StructField("insertion_order_freq", IntegerType, true),
                  StructField("insertion_order_rec",  IntegerType, true),
                  StructField("buyer_gender",         StringType,  true),
                  StructField("buyer_age",            IntegerType, true),
                  StructField("targeted_segment_list",
                              ArrayType(IntegerType, true),
                              true
                  ),
                  StructField("custom_model_id",            IntegerType, true),
                  StructField("custom_model_last_modified", LongType,    true),
                  StructField("custom_model_output_code",   StringType,  true),
                  StructField("external_uid",               StringType,  true),
                  StructField("is_remarketing",             IntegerType, true),
                  StructField("mobile_app_instance_id",     IntegerType, true),
                  StructField("traffic_source_code",        StringType,  true),
                  StructField("external_request_id",        StringType,  true),
                  StructField("stitch_group_id",            StringType,  true),
                  StructField("deal_type",                  IntegerType, true),
                  StructField("ym_floor_id",                IntegerType, true),
                  StructField("ym_bias_id",                 IntegerType, true),
                  StructField("bid_priority",               IntegerType, true),
                  StructField("pricing_type",               StringType,  true),
                  StructField(
                    "buyer_charges",
                    StructType(
                      Array(
                        StructField("rate_card_id", IntegerType, true),
                        StructField("member_id",    IntegerType, true),
                        StructField("is_dw",        BooleanType, true),
                        StructField(
                          "pricing_terms",
                          ArrayType(
                            StructType(
                              Array(
                                StructField("term_id",      IntegerType, true),
                                StructField("amount",       DoubleType,  true),
                                StructField("rate",         DoubleType,  true),
                                StructField("is_deduction", BooleanType, true),
                                StructField("is_media_cost_dependent",
                                            BooleanType,
                                            true
                                ),
                                StructField("data_member_id", IntegerType, true)
                              )
                            ),
                            true
                          ),
                          true
                        ),
                        StructField("fx_margin_rate_id",    IntegerType, true),
                        StructField("marketplace_owner_id", IntegerType, true),
                        StructField("virtual_marketplace_id",
                                    IntegerType,
                                    true
                        ),
                        StructField("amino_enabled", BooleanType, true)
                      )
                    ),
                    true
                  ),
                  StructField(
                    "seller_charges",
                    StructType(
                      Array(
                        StructField("rate_card_id", IntegerType, true),
                        StructField("member_id",    IntegerType, true),
                        StructField("is_dw",        BooleanType, true),
                        StructField(
                          "pricing_terms",
                          ArrayType(
                            StructType(
                              Array(
                                StructField("term_id",      IntegerType, true),
                                StructField("amount",       DoubleType,  true),
                                StructField("rate",         DoubleType,  true),
                                StructField("is_deduction", BooleanType, true),
                                StructField("is_media_cost_dependent",
                                            BooleanType,
                                            true
                                ),
                                StructField("data_member_id", IntegerType, true)
                              )
                            ),
                            true
                          ),
                          true
                        ),
                        StructField("fx_margin_rate_id",    IntegerType, true),
                        StructField("marketplace_owner_id", IntegerType, true),
                        StructField("virtual_marketplace_id",
                                    IntegerType,
                                    true
                        ),
                        StructField("amino_enabled", BooleanType, true)
                      )
                    ),
                    true
                  ),
                  StructField("revenue_value",           DoubleType,  true),
                  StructField("media_buy_rev_share_pct", DoubleType,  true),
                  StructField("view_detection_enabled",  IntegerType, true),
                  StructField(
                    "data_costs",
                    ArrayType(
                      StructType(
                        Array(
                          StructField("data_member_id", IntegerType, true),
                          StructField("cost",           DoubleType,  true),
                          StructField("used_segments",
                                      ArrayType(IntegerType, true),
                                      true
                          ),
                          StructField("cost_pct", DoubleType, true)
                        )
                      ),
                      true
                    ),
                    true
                  ),
                  StructField("device_make_id",              IntegerType, true),
                  StructField("operating_system_family_id",  IntegerType, true),
                  StructField("pricing_media_type",          IntegerType, true),
                  StructField("buyer_trx_event_id",          IntegerType, true),
                  StructField("seller_trx_event_id",         IntegerType, true),
                  StructField("is_unit_of_trx",              BooleanType, true),
                  StructField("revenue_auction_event_type",  IntegerType, true),
                  StructField("is_prebid",                   BooleanType, true),
                  StructField("auction_timestamp",           LongType,    true),
                  StructField("clear_fees",                  DoubleType,  true),
                  StructField("is_appnexus_cleared",         IntegerType, true),
                  StructField("two_phase_reduction_applied", BooleanType, true),
                  StructField("region_id",                   IntegerType, true),
                  StructField("media_company_id",            IntegerType, true),
                  StructField("trade_agreement_id",          IntegerType, true),
                  StructField("cadence_modifier",            DoubleType,  true),
                  StructField("content_category_id",         IntegerType, true),
                  StructField("fx_rate_snapshot_id",         IntegerType, true),
                  StructField(
                    "crossdevice_group_anon",
                    StructType(
                      Array(StructField("graph_id", IntegerType, true),
                            StructField("group_id", BinaryType,  true)
                      )
                    ),
                    true
                  ),
                  StructField("revenue_event_type_id",     IntegerType, true),
                  StructField("inv_code",                  StringType,  true),
                  StructField("bidder_id",                 IntegerType, true),
                  StructField("serving_fees_revshare",     DoubleType,  true),
                  StructField("commission_revshare",       DoubleType,  true),
                  StructField("booked_revenue_adv_curr",   DoubleType,  true),
                  StructField("predict_type_cost",         IntegerType, true),
                  StructField("ip_address",                StringType,  true),
                  StructField("predict_goal",              DoubleType,  true),
                  StructField("seller_deduction",          DoubleType,  true),
                  StructField("auction_service_fees",      DoubleType,  true),
                  StructField("auction_service_deduction", DoubleType,  true),
                  StructField("is_creative_hosted",        IntegerType, true),
                  StructField("creative_audit_status",     IntegerType, true),
                  StructField("datacenter_id",             IntegerType, true),
                  StructField("truncate_ip",               IntegerType, true),
                  StructField("is_exclusive",              IntegerType, true),
                  StructField("vp_expose_gender",          IntegerType, true),
                  StructField("vp_expose_age",             IntegerType, true),
                  StructField(
                    "crossdevice_graph_cost",
                    StructType(
                      Array(StructField("graph_provider_member_id",
                                        IntegerType,
                                        true
                            ),
                            StructField("cost_cpm_usd", DoubleType, true)
                      )
                    ),
                    true
                  ),
                  StructField("player_width",                  IntegerType, true),
                  StructField("player_height",                 IntegerType, true),
                  StructField("external_creative_id",          StringType,  true),
                  StructField("age_bucket",                    IntegerType, true),
                  StructField("bidder_seat_id",                IntegerType, true),
                  StructField("billing_period_id",             IntegerType, true),
                  StructField("flight_id",                     IntegerType, true),
                  StructField("split_id",                      IntegerType, true),
                  StructField("total_partner_fees_microcents", LongType,    true),
                  StructField("net_media_cost_dollars_cpm",    DoubleType,  true),
                  StructField("total_data_costs_microcents",   LongType,    true),
                  StructField("total_profit_microcents",       LongType,    true),
                  StructField("curator_member_id",             IntegerType, true),
                  StructField("eap",                           DoubleType,  true),
                  StructField("ecp",                           DoubleType,  true),
                  StructField("buyer_bid",                     DoubleType,  true),
                  StructField("reserve_price",                 DoubleType,  true),
                  StructField("serving_fees_cpm",              DoubleType,  true),
                  StructField("can_convert",                   IntegerType, true),
                  StructField("control_creative_id",           IntegerType, true),
                  StructField("commission_cpm",                DoubleType,  true),
                  StructField("creative_overage_fees",         DoubleType,  true),
                  StructField("imps_for_budget_caps_pacing",   IntegerType, true),
                  StructField("buyer_spend",                   DoubleType,  true),
                  StructField("actual_bid",                    DoubleType,  true),
                  StructField("campaign_group_type_id",        IntegerType, true),
                  StructField("predicted_kpi_event_rate",      DoubleType,  true),
                  StructField("total_segment_data_costs_microcents",
                              LongType,
                              true
                  ),
                  StructField("total_feature_costs_microcents", LongType,    true),
                  StructField("counterparty_ruleset_type",      IntegerType, true),
                  StructField(
                    "log_product_ads",
                    StructType(
                      Array(
                        StructField("product_feed_id", IntegerType, true),
                        StructField("item_selection_strategy_id",
                                    IntegerType,
                                    true
                        ),
                        StructField("product_uuid", StringType, true)
                      )
                    ),
                    true
                  ),
                  StructField("hb_source",                IntegerType, true),
                  StructField("buyer_line_item_currency", StringType,  true),
                  StructField("deal_line_item_currency",  StringType,  true),
                  StructField("is_curated",               BooleanType, true),
                  StructField(
                    "personal_identifiers_experimental",
                    ArrayType(
                      StructType(
                        Array(StructField("identity_type",  IntegerType, true),
                              StructField("identity_value", StringType,  true)
                        )
                      ),
                      true
                    ),
                    true
                  ),
                  StructField("postal_code_ext_id",   IntegerType, true),
                  StructField("ecpm_conversion_rate", DoubleType,  true),
                  StructField(
                    "targeted_segment_details_by_id_type",
                    ArrayType(
                      StructType(
                        Array(
                          StructField("identity_type", IntegerType, true),
                          StructField(
                            "targeted_segment_details",
                            ArrayType(
                              StructType(
                                Array(
                                  StructField("segment_id", IntegerType, true),
                                  StructField("last_seen_min",
                                              IntegerType,
                                              true
                                  )
                                )
                              ),
                              true
                            ),
                            true
                          )
                        )
                      ),
                      true
                    ),
                    true
                  ),
                  StructField(
                    "targeted_segment_details",
                    ArrayType(
                      StructType(
                        Array(StructField("segment_id",    IntegerType, true),
                              StructField("last_seen_min", IntegerType, true)
                        )
                      ),
                      true
                    ),
                    true
                  ),
                  StructField(
                    "personal_identifiers",
                    ArrayType(
                      StructType(
                        Array(StructField("identity_type",  IntegerType, true),
                              StructField("identity_value", StringType,  true)
                        )
                      ),
                      true
                    ),
                    true
                  ),
                  StructField("district_postal_code_lists",
                              ArrayType(IntegerType, true),
                              true
                  ),
                  StructField("video_served_timestamp",     LongType,    true),
                  StructField("video_started_timestamp",    LongType,    true),
                  StructField("video_skipped_timestamp",    LongType,    true),
                  StructField("video_errored_timestamp",    LongType,    true),
                  StructField("video_hit_25_pct_timestamp", LongType,    true),
                  StructField("video_hit_50_pct_timestamp", LongType,    true),
                  StructField("video_hit_75_pct_timestamp", LongType,    true),
                  StructField("video_completed_timestamp",  LongType,    true),
                  StructField("fallback_ad_index",          IntegerType, true),
                  StructField("is_transacted",              BooleanType, true),
                  StructField("is_winning_events",          BooleanType, true),
                  StructField("is_service_events",          BooleanType, true),
                  StructField("buyer_dpvp_bitmap",          LongType,    true),
                  StructField("seller_dpvp_bitmap",         LongType,    true),
                  StructField("imp_media_cost_dollars_cpm", DoubleType,  true),
                  StructField("imp_booked_revenue_dollars", DoubleType,  true),
                  StructField("imp_seller_revenue_cpm",     DoubleType,  true)
                )
              ),
              true
            ),
            StructField(
              "agg_platform_video_impressions",
              StructType(
                Array(
                  StructField("date_time",                   LongType,    true),
                  StructField("auction_id_64",               LongType,    true),
                  StructField("seller_member_id",            IntegerType, true),
                  StructField("buyer_member_id",             IntegerType, true),
                  StructField("is_delivered",                IntegerType, true),
                  StructField("cleared_direct",              IntegerType, true),
                  StructField("transaction_type",            IntegerType, true),
                  StructField("advertiser_id",               IntegerType, true),
                  StructField("insertion_order_id",          IntegerType, true),
                  StructField("campaign_group_id",           IntegerType, true),
                  StructField("campaign_id",                 IntegerType, true),
                  StructField("publisher_id",                IntegerType, true),
                  StructField("bidder_id",                   IntegerType, true),
                  StructField("creative_id",                 IntegerType, true),
                  StructField("brand_id",                    IntegerType, true),
                  StructField("is_dw_buyer",                 IntegerType, true),
                  StructField("is_dw_seller",                IntegerType, true),
                  StructField("has_dw_buy",                  IntegerType, true),
                  StructField("has_dw_sell",                 IntegerType, true),
                  StructField("booked_revenue_dollars",      DoubleType,  true),
                  StructField("buyer_media_cost_cpm",        DoubleType,  true),
                  StructField("auction_service_fees",        DoubleType,  true),
                  StructField("auction_service_deduction",   DoubleType,  true),
                  StructField("clear_fees",                  DoubleType,  true),
                  StructField("creative_overage_fees",       DoubleType,  true),
                  StructField("seller_media_cost_cpm",       DoubleType,  true),
                  StructField("seller_revenue",              DoubleType,  true),
                  StructField("seller_deduction",            DoubleType,  true),
                  StructField("discrepancy_allowance",       DoubleType,  true),
                  StructField("commission_cpm",              DoubleType,  true),
                  StructField("commission_revshare",         DoubleType,  true),
                  StructField("serving_fees_cpm",            DoubleType,  true),
                  StructField("serving_fees_revshare",       DoubleType,  true),
                  StructField("advertiser_currency",         StringType,  true),
                  StructField("advertiser_exchange_rate",    DoubleType,  true),
                  StructField("publisher_currency",          StringType,  true),
                  StructField("publisher_exchange_rate",     DoubleType,  true),
                  StructField("deal_id",                     IntegerType, true),
                  StructField("geo_country",                 StringType,  true),
                  StructField("imp_blacklist_or_fraud",      IntegerType, true),
                  StructField("pricing_media_type",          IntegerType, true),
                  StructField("imp_ignored",                 BooleanType, true),
                  StructField("is_prebid",                   BooleanType, true),
                  StructField("is_unit_of_buyer_trx",        BooleanType, true),
                  StructField("is_unit_of_seller_trx",       BooleanType, true),
                  StructField("custom_model_id",             IntegerType, true),
                  StructField("media_type",                  IntegerType, true),
                  StructField("device_type",                 IntegerType, true),
                  StructField("two_phase_reduction_applied", BooleanType, true),
                  StructField(
                    "buyer_charges",
                    StructType(
                      Array(
                        StructField("rate_card_id", IntegerType, true),
                        StructField("member_id",    IntegerType, true),
                        StructField("is_dw",        BooleanType, true),
                        StructField(
                          "pricing_terms",
                          ArrayType(
                            StructType(
                              Array(
                                StructField("term_id",      IntegerType, true),
                                StructField("amount",       DoubleType,  true),
                                StructField("rate",         DoubleType,  true),
                                StructField("is_deduction", BooleanType, true),
                                StructField("is_media_cost_dependent",
                                            BooleanType,
                                            true
                                ),
                                StructField("data_member_id", IntegerType, true)
                              )
                            ),
                            true
                          ),
                          true
                        ),
                        StructField("fx_margin_rate_id",    IntegerType, true),
                        StructField("marketplace_owner_id", IntegerType, true),
                        StructField("virtual_marketplace_id",
                                    IntegerType,
                                    true
                        ),
                        StructField("amino_enabled", BooleanType, true)
                      )
                    ),
                    true
                  ),
                  StructField(
                    "seller_charges",
                    StructType(
                      Array(
                        StructField("rate_card_id", IntegerType, true),
                        StructField("member_id",    IntegerType, true),
                        StructField("is_dw",        BooleanType, true),
                        StructField(
                          "pricing_terms",
                          ArrayType(
                            StructType(
                              Array(
                                StructField("term_id",      IntegerType, true),
                                StructField("amount",       DoubleType,  true),
                                StructField("rate",         DoubleType,  true),
                                StructField("is_deduction", BooleanType, true),
                                StructField("is_media_cost_dependent",
                                            BooleanType,
                                            true
                                ),
                                StructField("data_member_id", IntegerType, true)
                              )
                            ),
                            true
                          ),
                          true
                        ),
                        StructField("fx_margin_rate_id",    IntegerType, true),
                        StructField("marketplace_owner_id", IntegerType, true),
                        StructField("virtual_marketplace_id",
                                    IntegerType,
                                    true
                        ),
                        StructField("amino_enabled", BooleanType, true)
                      )
                    ),
                    true
                  ),
                  StructField(
                    "seller_transaction_def",
                    StructType(
                      Array(StructField("transaction_event", IntegerType, true),
                            StructField("transaction_event_type_id",
                                        IntegerType,
                                        true
                            )
                      )
                    ),
                    true
                  ),
                  StructField(
                    "buyer_transaction_def",
                    StructType(
                      Array(StructField("transaction_event", IntegerType, true),
                            StructField("transaction_event_type_id",
                                        IntegerType,
                                        true
                            )
                      )
                    ),
                    true
                  ),
                  StructField("view_measurable",             IntegerType, true),
                  StructField("viewable",                    IntegerType, true),
                  StructField("imp_type",                    IntegerType, true),
                  StructField("viewdef_definition_id",       IntegerType, true),
                  StructField("viewdef_viewed_imps",         LongType,    true),
                  StructField("imp_rejecter_do_auction",     BooleanType, true),
                  StructField("buyer_trx_event_id",          IntegerType, true),
                  StructField("seller_trx_event_id",         IntegerType, true),
                  StructField("inventory_url_id",            IntegerType, true),
                  StructField("mobile_app_instance_id",      IntegerType, true),
                  StructField("user_id_64",                  LongType,    true),
                  StructField("split_id",                    IntegerType, true),
                  StructField("campaign_group_type_id",      IntegerType, true),
                  StructField("advertiser_default_currency", StringType,  true),
                  StructField("advertiser_default_exchange_rate",
                              DoubleType,
                              true
                  ),
                  StructField("member_currency",       StringType,  true),
                  StructField("member_exchange_rate",  DoubleType,  true),
                  StructField("billing_currency",      StringType,  true),
                  StructField("billing_exchange_rate", DoubleType,  true),
                  StructField("fx_rate_snapshot_id",   IntegerType, true),
                  StructField("bidder_seat_id",        IntegerType, true),
                  StructField("region_id",             IntegerType, true)
                )
              ),
              true
            ),
            StructField(
              "agg_dw_clicks",
              StructType(
                Array(
                  StructField("date_time",                 LongType,    true),
                  StructField("auction_id_64",             LongType,    true),
                  StructField("user_id_64",                LongType,    true),
                  StructField("tag_id",                    IntegerType, true),
                  StructField("venue_id",                  IntegerType, true),
                  StructField("inventory_source_id",       IntegerType, true),
                  StructField("session_frequency",         IntegerType, true),
                  StructField("width",                     IntegerType, true),
                  StructField("height",                    IntegerType, true),
                  StructField("geo_country",               StringType,  true),
                  StructField("geo_region",                StringType,  true),
                  StructField("gender",                    StringType,  true),
                  StructField("age",                       IntegerType, true),
                  StructField("seller_member_id",          IntegerType, true),
                  StructField("buyer_member_id",           IntegerType, true),
                  StructField("creative_id",               IntegerType, true),
                  StructField("seller_currency",           StringType,  true),
                  StructField("buyer_currency",            StringType,  true),
                  StructField("advertiser_id",             IntegerType, true),
                  StructField("campaign_group_id",         IntegerType, true),
                  StructField("campaign_id",               IntegerType, true),
                  StructField("creative_freq",             IntegerType, true),
                  StructField("creative_rec",              IntegerType, true),
                  StructField("is_learn",                  IntegerType, true),
                  StructField("is_remarketing",            IntegerType, true),
                  StructField("advertiser_frequency",      IntegerType, true),
                  StructField("advertiser_recency",        IntegerType, true),
                  StructField("user_group_id",             IntegerType, true),
                  StructField("camp_dp_id",                IntegerType, true),
                  StructField("media_buy_id",              IntegerType, true),
                  StructField("brand_id",                  IntegerType, true),
                  StructField("is_appnexus_cleared",       IntegerType, true),
                  StructField("clear_fees",                DoubleType,  true),
                  StructField("media_buy_rev_share_pct",   DoubleType,  true),
                  StructField("revenue_value",             DoubleType,  true),
                  StructField("pricing_type",              StringType,  true),
                  StructField("site_id",                   IntegerType, true),
                  StructField("content_category_id",       IntegerType, true),
                  StructField("fold_position",             IntegerType, true),
                  StructField("external_inv_id",           IntegerType, true),
                  StructField("cadence_modifier",          DoubleType,  true),
                  StructField("predict_type",              StringType,  true),
                  StructField("predict_goal",              DoubleType,  true),
                  StructField("imp_type",                  IntegerType, true),
                  StructField("advertiser_currency",       StringType,  true),
                  StructField("advertiser_exchange_rate",  DoubleType,  true),
                  StructField("ip_address",                StringType,  true),
                  StructField("pub_rule_id",               IntegerType, true),
                  StructField("publisher_id",              IntegerType, true),
                  StructField("insertion_order_id",        IntegerType, true),
                  StructField("predict_type_rev",          IntegerType, true),
                  StructField("predict_type_goal",         IntegerType, true),
                  StructField("predict_type_cost",         IntegerType, true),
                  StructField("booked_revenue_dollars",    DoubleType,  true),
                  StructField("booked_revenue_adv_curr",   DoubleType,  true),
                  StructField("commission_revshare",       DoubleType,  true),
                  StructField("serving_fees_revshare",     DoubleType,  true),
                  StructField("user_tz_offset",            IntegerType, true),
                  StructField("media_type",                IntegerType, true),
                  StructField("operating_system",          IntegerType, true),
                  StructField("browser",                   IntegerType, true),
                  StructField("language",                  IntegerType, true),
                  StructField("publisher_currency",        StringType,  true),
                  StructField("publisher_exchange_rate",   DoubleType,  true),
                  StructField("media_cost_dollars_cpm",    DoubleType,  true),
                  StructField("site_domain",               StringType,  true),
                  StructField("payment_type",              IntegerType, true),
                  StructField("revenue_type",              IntegerType, true),
                  StructField("bidder_id",                 IntegerType, true),
                  StructField("inv_code",                  StringType,  true),
                  StructField("application_id",            StringType,  true),
                  StructField("is_control",                IntegerType, true),
                  StructField("vp_expose_domains",         IntegerType, true),
                  StructField("vp_expose_categories",      IntegerType, true),
                  StructField("vp_expose_pubs",            IntegerType, true),
                  StructField("vp_expose_tag",             IntegerType, true),
                  StructField("vp_expose_age",             IntegerType, true),
                  StructField("vp_expose_gender",          IntegerType, true),
                  StructField("inventory_url_id",          IntegerType, true),
                  StructField("imp_time",                  StringType,  true),
                  StructField("is_exclusive",              IntegerType, true),
                  StructField("truncate_ip",               IntegerType, true),
                  StructField("datacenter_id",             IntegerType, true),
                  StructField("device_id",                 IntegerType, true),
                  StructField("carrier_id",                IntegerType, true),
                  StructField("creative_audit_status",     IntegerType, true),
                  StructField("is_creative_hosted",        IntegerType, true),
                  StructField("auction_service_deduction", DoubleType,  true),
                  StructField("auction_service_fees",      DoubleType,  true),
                  StructField("seller_deduction",          DoubleType,  true),
                  StructField("city",                      IntegerType, true),
                  StructField("latitude",                  StringType,  true),
                  StructField("longitude",                 StringType,  true),
                  StructField("device_unique_id",          StringType,  true),
                  StructField("targeted_segments",         StringType,  true),
                  StructField("supply_type",               IntegerType, true),
                  StructField("is_toolbar",                IntegerType, true),
                  StructField("control_pct",               DoubleType,  true),
                  StructField("deal_id",                   IntegerType, true),
                  StructField("vp_bitmap",                 LongType,    true),
                  StructField("ozone_id",                  IntegerType, true),
                  StructField("is_performance",            IntegerType, true),
                  StructField("sdk_version",               StringType,  true),
                  StructField("device_type",               IntegerType, true),
                  StructField("dma",                       IntegerType, true),
                  StructField("postal",                    StringType,  true),
                  StructField("package_id",                IntegerType, true),
                  StructField("campaign_group_freq",       IntegerType, true),
                  StructField("campaign_group_rec",        IntegerType, true),
                  StructField("insertion_order_freq",      IntegerType, true),
                  StructField("insertion_order_rec",       IntegerType, true),
                  StructField("buyer_gender",              StringType,  true),
                  StructField("buyer_age",                 IntegerType, true),
                  StructField("targeted_segment_list",
                              ArrayType(IntegerType, true),
                              true
                  ),
                  StructField("custom_model_id",            IntegerType, true),
                  StructField("custom_model_last_modified", LongType,    true),
                  StructField("custom_model_output_code",   StringType,  true),
                  StructField("external_uid",               StringType,  true),
                  StructField("request_uuid",               StringType,  true),
                  StructField("mobile_app_instance_id",     IntegerType, true),
                  StructField("traffic_source_code",        StringType,  true),
                  StructField("external_request_id",        StringType,  true),
                  StructField("stitch_group_id",            StringType,  true),
                  StructField("deal_type",                  IntegerType, true),
                  StructField("ym_floor_id",                IntegerType, true),
                  StructField("ym_bias_id",                 IntegerType, true),
                  StructField("bid_priority",               IntegerType, true),
                  StructField("viewdef_definition_id",      IntegerType, true),
                  StructField(
                    "buyer_charges",
                    StructType(
                      Array(
                        StructField("rate_card_id", IntegerType, true),
                        StructField("member_id",    IntegerType, true),
                        StructField("is_dw",        BooleanType, true),
                        StructField(
                          "pricing_terms",
                          ArrayType(
                            StructType(
                              Array(
                                StructField("term_id",      IntegerType, true),
                                StructField("amount",       DoubleType,  true),
                                StructField("rate",         DoubleType,  true),
                                StructField("is_deduction", BooleanType, true),
                                StructField("is_media_cost_dependent",
                                            BooleanType,
                                            true
                                ),
                                StructField("data_member_id", IntegerType, true)
                              )
                            ),
                            true
                          ),
                          true
                        ),
                        StructField("fx_margin_rate_id",    IntegerType, true),
                        StructField("marketplace_owner_id", IntegerType, true),
                        StructField("virtual_marketplace_id",
                                    IntegerType,
                                    true
                        ),
                        StructField("amino_enabled", BooleanType, true)
                      )
                    ),
                    true
                  ),
                  StructField(
                    "seller_charges",
                    StructType(
                      Array(
                        StructField("rate_card_id", IntegerType, true),
                        StructField("member_id",    IntegerType, true),
                        StructField("is_dw",        BooleanType, true),
                        StructField(
                          "pricing_terms",
                          ArrayType(
                            StructType(
                              Array(
                                StructField("term_id",      IntegerType, true),
                                StructField("amount",       DoubleType,  true),
                                StructField("rate",         DoubleType,  true),
                                StructField("is_deduction", BooleanType, true),
                                StructField("is_media_cost_dependent",
                                            BooleanType,
                                            true
                                ),
                                StructField("data_member_id", IntegerType, true)
                              )
                            ),
                            true
                          ),
                          true
                        ),
                        StructField("fx_margin_rate_id",    IntegerType, true),
                        StructField("marketplace_owner_id", IntegerType, true),
                        StructField("virtual_marketplace_id",
                                    IntegerType,
                                    true
                        ),
                        StructField("amino_enabled", BooleanType, true)
                      )
                    ),
                    true
                  ),
                  StructField("view_result",                IntegerType, true),
                  StructField("view_non_measurable_reason", IntegerType, true),
                  StructField("view_detection_enabled",     IntegerType, true),
                  StructField(
                    "data_costs",
                    ArrayType(
                      StructType(
                        Array(
                          StructField("data_member_id", IntegerType, true),
                          StructField("cost",           DoubleType,  true),
                          StructField("used_segments",
                                      ArrayType(IntegerType, true),
                                      true
                          ),
                          StructField("cost_pct", DoubleType, true)
                        )
                      ),
                      true
                    ),
                    true
                  ),
                  StructField("device_make_id",              IntegerType, true),
                  StructField("operating_system_family_id",  IntegerType, true),
                  StructField("pricing_media_type",          IntegerType, true),
                  StructField("buyer_trx_event_id",          IntegerType, true),
                  StructField("seller_trx_event_id",         IntegerType, true),
                  StructField("is_unit_of_trx",              BooleanType, true),
                  StructField("revenue_auction_event_type",  IntegerType, true),
                  StructField("is_prebid",                   BooleanType, true),
                  StructField("auction_timestamp",           LongType,    true),
                  StructField("two_phase_reduction_applied", BooleanType, true),
                  StructField("region_id",                   IntegerType, true),
                  StructField("media_company_id",            IntegerType, true),
                  StructField("trade_agreement_id",          IntegerType, true),
                  StructField(
                    "personal_data",
                    StructType(
                      Array(
                        StructField("user_id_64",       LongType,   true),
                        StructField("device_unique_id", StringType, true),
                        StructField("external_uid",     StringType, true),
                        StructField("ip_address",       BinaryType, true),
                        StructField(
                          "crossdevice_group",
                          StructType(
                            Array(StructField("graph_id", IntegerType, true),
                                  StructField("group_id", LongType,    true)
                            )
                          ),
                          false
                        ),
                        StructField("latitude",            DoubleType,  true),
                        StructField("longitude",           DoubleType,  true),
                        StructField("ipv6_address",        BinaryType,  true),
                        StructField("subject_to_gdpr",     BooleanType, true),
                        StructField("geo_country",         StringType,  true),
                        StructField("gdpr_consent_string", StringType,  true),
                        StructField("preempt_ip_address",  BinaryType,  true),
                        StructField("device_type",         IntegerType, true),
                        StructField("device_make_id",      IntegerType, true),
                        StructField("device_model_id",     IntegerType, true),
                        StructField("new_user_id_64",      LongType,    true),
                        StructField("is_service_provider_mode",
                                    BooleanType,
                                    true
                        ),
                        StructField("is_personal_info_sale", BooleanType, true)
                      )
                    ),
                    true
                  ),
                  StructField(
                    "anonymized_user_info",
                    StructType(Array(StructField("user_id", BinaryType, true))),
                    true
                  ),
                  StructField("fx_rate_snapshot_id", IntegerType, true),
                  StructField(
                    "crossdevice_group_anon",
                    StructType(
                      Array(StructField("graph_id", IntegerType, true),
                            StructField("group_id", BinaryType,  true)
                      )
                    ),
                    true
                  ),
                  StructField("revenue_event_type_id", IntegerType, true),
                  StructField("external_creative_id",  StringType,  true),
                  StructField(
                    "targeted_segment_details",
                    ArrayType(
                      StructType(
                        Array(StructField("segment_id",    IntegerType, true),
                              StructField("last_seen_min", IntegerType, true)
                        )
                      ),
                      true
                    ),
                    true
                  ),
                  StructField("bidder_seat_id",                IntegerType, true),
                  StructField("is_curated",                    BooleanType, true),
                  StructField("curator_member_id",             IntegerType, true),
                  StructField("cold_start_price_type",         IntegerType, true),
                  StructField("discovery_state",               IntegerType, true),
                  StructField("billing_period_id",             IntegerType, true),
                  StructField("flight_id",                     IntegerType, true),
                  StructField("split_id",                      IntegerType, true),
                  StructField("total_partner_fees_microcents", LongType,    true),
                  StructField("net_buyer_spend",               DoubleType,  true),
                  StructField("net_media_cost_dollars_cpm",    DoubleType,  true),
                  StructField("total_data_costs_microcents",   LongType,    true),
                  StructField("total_profit_microcents",       LongType,    true),
                  StructField("discovery_prediction",          DoubleType,  true),
                  StructField("campaign_group_type_id",        IntegerType, true),
                  StructField(
                    "excluded_targeted_segment_details",
                    ArrayType(
                      StructType(
                        Array(StructField("segment_id", IntegerType, true))
                      ),
                      true
                    ),
                    true
                  ),
                  StructField("trust_id",                 StringType, true),
                  StructField("predicted_kpi_event_rate", DoubleType, true),
                  StructField("has_crossdevice_reach_extension",
                              BooleanType,
                              true
                  ),
                  StructField("total_segment_data_costs_microcents",
                              LongType,
                              true
                  ),
                  StructField("total_feature_costs_microcents", LongType,    true),
                  StructField("counterparty_ruleset_type",      IntegerType, true),
                  StructField(
                    "log_product_ads",
                    StructType(
                      Array(
                        StructField("product_feed_id", IntegerType, true),
                        StructField("item_selection_strategy_id",
                                    IntegerType,
                                    true
                        ),
                        StructField("product_uuid", StringType, true)
                      )
                    ),
                    true
                  ),
                  StructField("hb_source",                IntegerType, true),
                  StructField("buyer_line_item_currency", StringType,  true),
                  StructField("deal_line_item_currency",  StringType,  true),
                  StructField(
                    "personal_identifiers_experimental",
                    ArrayType(
                      StructType(
                        Array(StructField("identity_type",  IntegerType, true),
                              StructField("identity_value", StringType,  true)
                        )
                      ),
                      true
                    ),
                    true
                  ),
                  StructField("postal_code_ext_id", IntegerType, true),
                  StructField(
                    "targeted_segment_details_by_id_type",
                    ArrayType(
                      StructType(
                        Array(
                          StructField("identity_type", IntegerType, true),
                          StructField(
                            "targeted_segment_details",
                            ArrayType(
                              StructType(
                                Array(
                                  StructField("segment_id", IntegerType, true),
                                  StructField("last_seen_min",
                                              IntegerType,
                                              true
                                  )
                                )
                              ),
                              true
                            ),
                            true
                          )
                        )
                      ),
                      true
                    ),
                    true
                  ),
                  StructField(
                    "personal_identifiers",
                    ArrayType(
                      StructType(
                        Array(StructField("identity_type",  IntegerType, true),
                              StructField("identity_value", StringType,  true)
                        )
                      ),
                      true
                    ),
                    true
                  ),
                  StructField("is_residential_ip", BooleanType, true),
                  StructField("hashed_ip",         StringType,  true),
                  StructField("district_postal_code_lists",
                              ArrayType(IntegerType, true),
                              true
                  ),
                  StructField("buyer_dpvp_bitmap",        LongType,    true),
                  StructField("seller_dpvp_bitmap",       LongType,    true),
                  StructField("private_auction_eligible", BooleanType, true),
                  StructField("chrome_traffic_label",     IntegerType, true),
                  StructField("is_private_auction",       BooleanType, true)
                )
              ),
              true
            ),
            StructField(
              "agg_dw_pixels",
              StructType(
                Array(
                  StructField("date_time",                 LongType,    true),
                  StructField("auction_id_64",             LongType,    true),
                  StructField("user_id_64",                LongType,    true),
                  StructField("tag_id",                    IntegerType, true),
                  StructField("venue_id",                  IntegerType, true),
                  StructField("inventory_source_id",       IntegerType, true),
                  StructField("session_frequency",         IntegerType, true),
                  StructField("width",                     IntegerType, true),
                  StructField("height",                    IntegerType, true),
                  StructField("geo_country",               StringType,  true),
                  StructField("geo_region",                StringType,  true),
                  StructField("gender",                    StringType,  true),
                  StructField("age",                       IntegerType, true),
                  StructField("seller_member_id",          IntegerType, true),
                  StructField("buyer_member_id",           IntegerType, true),
                  StructField("creative_id",               IntegerType, true),
                  StructField("seller_currency",           StringType,  true),
                  StructField("buyer_currency",            StringType,  true),
                  StructField("advertiser_id",             IntegerType, true),
                  StructField("campaign_group_id",         IntegerType, true),
                  StructField("campaign_id",               IntegerType, true),
                  StructField("creative_freq",             IntegerType, true),
                  StructField("creative_rec",              IntegerType, true),
                  StructField("is_learn",                  IntegerType, true),
                  StructField("is_remarketing",            IntegerType, true),
                  StructField("advertiser_frequency",      IntegerType, true),
                  StructField("advertiser_recency",        IntegerType, true),
                  StructField("user_group_id",             IntegerType, true),
                  StructField("camp_dp_id",                IntegerType, true),
                  StructField("media_buy_id",              IntegerType, true),
                  StructField("post_click_conv",           IntegerType, true),
                  StructField("post_view_conv",            IntegerType, true),
                  StructField("post_click_revenue",        DoubleType,  true),
                  StructField("post_view_revenue",         DoubleType,  true),
                  StructField("brand_id",                  IntegerType, true),
                  StructField("is_appnexus_cleared",       IntegerType, true),
                  StructField("clear_fees",                DoubleType,  true),
                  StructField("media_buy_rev_share_pct",   DoubleType,  true),
                  StructField("revenue_value",             DoubleType,  true),
                  StructField("pricing_type",              StringType,  true),
                  StructField("imp_time",                  StringType,  true),
                  StructField("pixel_id",                  IntegerType, true),
                  StructField("booked_revenue",            DoubleType,  true),
                  StructField("site_id",                   IntegerType, true),
                  StructField("content_category_id",       IntegerType, true),
                  StructField("fold_position",             IntegerType, true),
                  StructField("external_inv_id",           IntegerType, true),
                  StructField("cadence_modifier",          DoubleType,  true),
                  StructField("predict_goal",              DoubleType,  true),
                  StructField("imp_type",                  IntegerType, true),
                  StructField("advertiser_currency",       StringType,  true),
                  StructField("advertiser_exchange_rate",  DoubleType,  true),
                  StructField("ip_address",                StringType,  true),
                  StructField("order_id",                  StringType,  true),
                  StructField("external_data",             StringType,  true),
                  StructField("pub_rule_id",               IntegerType, true),
                  StructField("publisher_id",              IntegerType, true),
                  StructField("insertion_order_id",        IntegerType, true),
                  StructField("predict_type_rev",          IntegerType, true),
                  StructField("predict_type_goal",         IntegerType, true),
                  StructField("predict_type_cost",         IntegerType, true),
                  StructField("booked_revenue_dollars",    DoubleType,  true),
                  StructField("booked_revenue_adv_curr",   DoubleType,  true),
                  StructField("commission_revshare",       DoubleType,  true),
                  StructField("serving_fees_revshare",     DoubleType,  true),
                  StructField("is_control",                IntegerType, true),
                  StructField("user_tz_offset",            IntegerType, true),
                  StructField("media_type",                IntegerType, true),
                  StructField("operating_system",          IntegerType, true),
                  StructField("browser",                   IntegerType, true),
                  StructField("language",                  IntegerType, true),
                  StructField("publisher_currency",        StringType,  true),
                  StructField("publisher_exchange_rate",   DoubleType,  true),
                  StructField("media_cost_dollars_cpm",    DoubleType,  true),
                  StructField("site_domain",               StringType,  true),
                  StructField("payment_type",              IntegerType, true),
                  StructField("revenue_type",              IntegerType, true),
                  StructField("datacenter_id",             IntegerType, true),
                  StructField("vp_expose_domains",         IntegerType, true),
                  StructField("vp_expose_categories",      IntegerType, true),
                  StructField("vp_expose_pubs",            IntegerType, true),
                  StructField("vp_expose_tag",             IntegerType, true),
                  StructField("vp_expose_age",             IntegerType, true),
                  StructField("vp_expose_gender",          IntegerType, true),
                  StructField("inventory_url_id",          IntegerType, true),
                  StructField("is_exclusive",              IntegerType, true),
                  StructField("truncate_ip",               IntegerType, true),
                  StructField("device_id",                 IntegerType, true),
                  StructField("carrier_id",                IntegerType, true),
                  StructField("creative_audit_status",     IntegerType, true),
                  StructField("is_creative_hosted",        IntegerType, true),
                  StructField("auction_service_deduction", DoubleType,  true),
                  StructField("auction_service_fees",      DoubleType,  true),
                  StructField("seller_deduction",          DoubleType,  true),
                  StructField("city",                      IntegerType, true),
                  StructField("latitude",                  StringType,  true),
                  StructField("longitude",                 StringType,  true),
                  StructField("device_unique_id",          StringType,  true),
                  StructField("targeted_segments",         StringType,  true),
                  StructField("supply_type",               IntegerType, true),
                  StructField("is_toolbar",                IntegerType, true),
                  StructField("deal_id",                   IntegerType, true),
                  StructField("vp_bitmap",                 LongType,    true),
                  StructField("application_id",            StringType,  true),
                  StructField("ozone_id",                  IntegerType, true),
                  StructField("is_performance",            IntegerType, true),
                  StructField("sdk_version",               StringType,  true),
                  StructField("device_type",               IntegerType, true),
                  StructField("dma",                       IntegerType, true),
                  StructField("postal",                    StringType,  true),
                  StructField("package_id",                IntegerType, true),
                  StructField("campaign_group_freq",       IntegerType, true),
                  StructField("campaign_group_rec",        IntegerType, true),
                  StructField("insertion_order_freq",      IntegerType, true),
                  StructField("insertion_order_rec",       IntegerType, true),
                  StructField("buyer_gender",              StringType,  true),
                  StructField("buyer_age",                 IntegerType, true),
                  StructField("targeted_segment_list",
                              ArrayType(IntegerType, true),
                              true
                  ),
                  StructField("custom_model_id",            IntegerType, true),
                  StructField("custom_model_last_modified", LongType,    true),
                  StructField("custom_model_output_code",   StringType,  true),
                  StructField("external_uid",               StringType,  true),
                  StructField("request_uuid",               StringType,  true),
                  StructField("mobile_app_instance_id",     IntegerType, true),
                  StructField("traffic_source_code",        StringType,  true),
                  StructField("external_request_id",        StringType,  true),
                  StructField("stitch_group_id",            StringType,  true),
                  StructField("deal_type",                  IntegerType, true),
                  StructField("ym_floor_id",                IntegerType, true),
                  StructField("ym_bias_id",                 IntegerType, true),
                  StructField("bid_priority",               IntegerType, true),
                  StructField("viewdef_definition_id",      IntegerType, true),
                  StructField(
                    "buyer_charges",
                    StructType(
                      Array(
                        StructField("rate_card_id", IntegerType, true),
                        StructField("member_id",    IntegerType, true),
                        StructField("is_dw",        BooleanType, true),
                        StructField(
                          "pricing_terms",
                          ArrayType(
                            StructType(
                              Array(
                                StructField("term_id",      IntegerType, true),
                                StructField("amount",       DoubleType,  true),
                                StructField("rate",         DoubleType,  true),
                                StructField("is_deduction", BooleanType, true),
                                StructField("is_media_cost_dependent",
                                            BooleanType,
                                            true
                                ),
                                StructField("data_member_id", IntegerType, true)
                              )
                            ),
                            true
                          ),
                          true
                        ),
                        StructField("fx_margin_rate_id",    IntegerType, true),
                        StructField("marketplace_owner_id", IntegerType, true),
                        StructField("virtual_marketplace_id",
                                    IntegerType,
                                    true
                        ),
                        StructField("amino_enabled", BooleanType, true)
                      )
                    ),
                    true
                  ),
                  StructField(
                    "seller_charges",
                    StructType(
                      Array(
                        StructField("rate_card_id", IntegerType, true),
                        StructField("member_id",    IntegerType, true),
                        StructField("is_dw",        BooleanType, true),
                        StructField(
                          "pricing_terms",
                          ArrayType(
                            StructType(
                              Array(
                                StructField("term_id",      IntegerType, true),
                                StructField("amount",       DoubleType,  true),
                                StructField("rate",         DoubleType,  true),
                                StructField("is_deduction", BooleanType, true),
                                StructField("is_media_cost_dependent",
                                            BooleanType,
                                            true
                                ),
                                StructField("data_member_id", IntegerType, true)
                              )
                            ),
                            true
                          ),
                          true
                        ),
                        StructField("fx_margin_rate_id",    IntegerType, true),
                        StructField("marketplace_owner_id", IntegerType, true),
                        StructField("virtual_marketplace_id",
                                    IntegerType,
                                    true
                        ),
                        StructField("amino_enabled", BooleanType, true)
                      )
                    ),
                    true
                  ),
                  StructField("view_result",                IntegerType, true),
                  StructField("view_non_measurable_reason", IntegerType, true),
                  StructField("view_detection_enabled",     IntegerType, true),
                  StructField(
                    "data_costs",
                    ArrayType(
                      StructType(
                        Array(
                          StructField("data_member_id", IntegerType, true),
                          StructField("cost",           DoubleType,  true),
                          StructField("used_segments",
                                      ArrayType(IntegerType, true),
                                      true
                          ),
                          StructField("cost_pct", DoubleType, true)
                        )
                      ),
                      true
                    ),
                    true
                  ),
                  StructField("device_make_id",              IntegerType, true),
                  StructField("operating_system_family_id",  IntegerType, true),
                  StructField("bidder_id",                   IntegerType, true),
                  StructField("pricing_media_type",          IntegerType, true),
                  StructField("buyer_trx_event_id",          IntegerType, true),
                  StructField("seller_trx_event_id",         IntegerType, true),
                  StructField("is_unit_of_trx",              BooleanType, true),
                  StructField("revenue_auction_event_type",  IntegerType, true),
                  StructField("is_prebid",                   BooleanType, true),
                  StructField("attribution_context",         IntegerType, true),
                  StructField("two_phase_reduction_applied", BooleanType, true),
                  StructField("region_id",                   IntegerType, true),
                  StructField("media_company_id",            IntegerType, true),
                  StructField("trade_agreement_id",          IntegerType, true),
                  StructField(
                    "personal_data",
                    StructType(
                      Array(
                        StructField("user_id_64",       LongType,   true),
                        StructField("device_unique_id", StringType, true),
                        StructField("external_uid",     StringType, true),
                        StructField("ip_address",       BinaryType, true),
                        StructField(
                          "crossdevice_group",
                          StructType(
                            Array(StructField("graph_id", IntegerType, true),
                                  StructField("group_id", LongType,    true)
                            )
                          ),
                          false
                        ),
                        StructField("latitude",            DoubleType,  true),
                        StructField("longitude",           DoubleType,  true),
                        StructField("ipv6_address",        BinaryType,  true),
                        StructField("subject_to_gdpr",     BooleanType, true),
                        StructField("geo_country",         StringType,  true),
                        StructField("gdpr_consent_string", StringType,  true),
                        StructField("preempt_ip_address",  BinaryType,  true),
                        StructField("device_type",         IntegerType, true),
                        StructField("device_make_id",      IntegerType, true),
                        StructField("device_model_id",     IntegerType, true),
                        StructField("new_user_id_64",      LongType,    true),
                        StructField("is_service_provider_mode",
                                    BooleanType,
                                    true
                        ),
                        StructField("is_personal_info_sale", BooleanType, true)
                      )
                    ),
                    true
                  ),
                  StructField(
                    "anonymized_user_info",
                    StructType(Array(StructField("user_id", BinaryType, true))),
                    true
                  ),
                  StructField("auction_timestamp",   LongType,    true),
                  StructField("fx_rate_snapshot_id", IntegerType, true),
                  StructField(
                    "crossdevice_group_anon",
                    StructType(
                      Array(StructField("graph_id", IntegerType, true),
                            StructField("group_id", BinaryType,  true)
                      )
                    ),
                    true
                  ),
                  StructField("post_click_crossdevice_conv", IntegerType, true),
                  StructField("post_view_crossdevice_conv",  IntegerType, true),
                  StructField("revenue_event_type_id",       IntegerType, true),
                  StructField("post_click_crossdevice_revenue",
                              DoubleType,
                              true
                  ),
                  StructField("post_view_crossdevice_revenue",
                              DoubleType,
                              true
                  ),
                  StructField("external_creative_id", StringType, true),
                  StructField(
                    "targeted_segment_details",
                    ArrayType(
                      StructType(
                        Array(StructField("segment_id",    IntegerType, true),
                              StructField("last_seen_min", IntegerType, true)
                        )
                      ),
                      true
                    ),
                    true
                  ),
                  StructField("bidder_seat_id", IntegerType, true),
                  StructField("universal_pixel_rule_version_id",
                              IntegerType,
                              true
                  ),
                  StructField("is_curated",                BooleanType, true),
                  StructField("curator_member_id",         IntegerType, true),
                  StructField("cold_start_price_type",     IntegerType, true),
                  StructField("discovery_state",           IntegerType, true),
                  StructField("billing_period_id",         IntegerType, true),
                  StructField("flight_id",                 IntegerType, true),
                  StructField("split_id",                  IntegerType, true),
                  StructField("conversion_device_type",    IntegerType, true),
                  StructField("conversion_device_make_id", IntegerType, true),
                  StructField("universal_pixel_fire_id",   StringType,  true),
                  StructField("discovery_prediction",      DoubleType,  true),
                  StructField("campaign_group_type_id",    IntegerType, true),
                  StructField(
                    "excluded_targeted_segment_details",
                    ArrayType(
                      StructType(
                        Array(StructField("segment_id", IntegerType, true))
                      ),
                      true
                    ),
                    true
                  ),
                  StructField("trust_id",                 StringType, true),
                  StructField("predicted_kpi_event_rate", DoubleType, true),
                  StructField("has_crossdevice_reach_extension",
                              BooleanType,
                              true
                  ),
                  StructField("counterparty_ruleset_type", IntegerType, true),
                  StructField(
                    "log_product_ads",
                    StructType(
                      Array(
                        StructField("product_feed_id", IntegerType, true),
                        StructField("item_selection_strategy_id",
                                    IntegerType,
                                    true
                        ),
                        StructField("product_uuid", StringType, true)
                      )
                    ),
                    true
                  ),
                  StructField("hb_source", IntegerType, true),
                  StructField(
                    "personal_identifiers_experimental",
                    ArrayType(
                      StructType(
                        Array(StructField("identity_type",  IntegerType, true),
                              StructField("identity_value", StringType,  true)
                        )
                      ),
                      true
                    ),
                    true
                  ),
                  StructField("postal_code_ext_id", IntegerType, true),
                  StructField(
                    "targeted_segment_details_by_id_type",
                    ArrayType(
                      StructType(
                        Array(
                          StructField("identity_type", IntegerType, true),
                          StructField(
                            "targeted_segment_details",
                            ArrayType(
                              StructType(
                                Array(
                                  StructField("segment_id", IntegerType, true),
                                  StructField("last_seen_min",
                                              IntegerType,
                                              true
                                  )
                                )
                              ),
                              true
                            ),
                            true
                          )
                        )
                      ),
                      true
                    ),
                    true
                  ),
                  StructField(
                    "personal_identifiers",
                    ArrayType(
                      StructType(
                        Array(StructField("identity_type",  IntegerType, true),
                              StructField("identity_value", StringType,  true)
                        )
                      ),
                      true
                    ),
                    true
                  ),
                  StructField("post_click_ip_conv", IntegerType, true),
                  StructField("post_view_ip_conv",  IntegerType, true),
                  StructField("hashed_ip",          StringType,  true),
                  StructField("district_postal_code_lists",
                              ArrayType(IntegerType, true),
                              true
                  ),
                  StructField("buyer_dpvp_bitmap",        LongType,    true),
                  StructField("seller_dpvp_bitmap",       LongType,    true),
                  StructField("private_auction_eligible", BooleanType, true),
                  StructField("chrome_traffic_label",     IntegerType, true),
                  StructField("is_private_auction",       BooleanType, true)
                )
              ),
              true
            ),
            StructField(
              "agg_impbus_clicks",
              StructType(
                Array(
                  StructField("date_time",                LongType,    true),
                  StructField("auction_id_64",            LongType,    true),
                  StructField("user_id_64",               LongType,    true),
                  StructField("tag_id",                   IntegerType, true),
                  StructField("venue_id",                 IntegerType, true),
                  StructField("inventory_source_id",      IntegerType, true),
                  StructField("session_frequency",        IntegerType, true),
                  StructField("width",                    IntegerType, true),
                  StructField("height",                   IntegerType, true),
                  StructField("geo_country",              StringType,  true),
                  StructField("geo_region",               StringType,  true),
                  StructField("gender",                   StringType,  true),
                  StructField("age",                      IntegerType, true),
                  StructField("seller_member_id",         IntegerType, true),
                  StructField("buyer_member_id",          IntegerType, true),
                  StructField("creative_id",              IntegerType, true),
                  StructField("seller_currency",          StringType,  true),
                  StructField("buyer_currency",           StringType,  true),
                  StructField("advertiser_id",            IntegerType, true),
                  StructField("campaign_group_id",        IntegerType, true),
                  StructField("campaign_id",              IntegerType, true),
                  StructField("creative_freq",            IntegerType, true),
                  StructField("creative_rec",             IntegerType, true),
                  StructField("is_learn",                 IntegerType, true),
                  StructField("is_remarketing",           IntegerType, true),
                  StructField("advertiser_frequency",     IntegerType, true),
                  StructField("advertiser_recency",       IntegerType, true),
                  StructField("user_group_id",            IntegerType, true),
                  StructField("camp_dp_id",               IntegerType, true),
                  StructField("media_buy_id",             IntegerType, true),
                  StructField("brand_id",                 IntegerType, true),
                  StructField("is_appnexus_cleared",      IntegerType, true),
                  StructField("clear_fees",               DoubleType,  true),
                  StructField("media_buy_rev_share_pct",  DoubleType,  true),
                  StructField("revenue_value",            DoubleType,  true),
                  StructField("pricing_type",             StringType,  true),
                  StructField("site_id",                  IntegerType, true),
                  StructField("content_category_id",      IntegerType, true),
                  StructField("fold_position",            IntegerType, true),
                  StructField("external_inv_id",          IntegerType, true),
                  StructField("cadence_modifier",         DoubleType,  true),
                  StructField("imp_type",                 IntegerType, true),
                  StructField("advertiser_currency",      StringType,  true),
                  StructField("advertiser_exchange_rate", DoubleType,  true),
                  StructField("ip_address",               StringType,  true),
                  StructField("pub_rule_id",              IntegerType, true),
                  StructField("publisher_id",             IntegerType, true),
                  StructField("insertion_order_id",       IntegerType, true),
                  StructField("predict_type_rev",         IntegerType, true),
                  StructField("predict_type_goal",        IntegerType, true),
                  StructField("predict_type_cost",        IntegerType, true),
                  StructField("commission_revshare",      DoubleType,  true),
                  StructField("serving_fees_revshare",    DoubleType,  true),
                  StructField("user_tz_offset",           IntegerType, true),
                  StructField("media_type",               IntegerType, true),
                  StructField("operating_system",         IntegerType, true),
                  StructField("browser",                  IntegerType, true),
                  StructField("language",                 IntegerType, true),
                  StructField("publisher_currency",       StringType,  true),
                  StructField("publisher_exchange_rate",  DoubleType,  true),
                  StructField("site_domain",              StringType,  true),
                  StructField("payment_type",             IntegerType, true),
                  StructField("revenue_type",             IntegerType, true),
                  StructField("bidder_id",                IntegerType, true),
                  StructField("inv_code",                 StringType,  true),
                  StructField("application_id",           StringType,  true),
                  StructField("is_control",               IntegerType, true),
                  StructField("vp_expose_domains",        IntegerType, true),
                  StructField("vp_expose_categories",     IntegerType, true),
                  StructField("vp_expose_pubs",           IntegerType, true),
                  StructField("vp_expose_tag",            IntegerType, true),
                  StructField("vp_expose_age",            IntegerType, true),
                  StructField("vp_expose_gender",         IntegerType, true),
                  StructField("inventory_url_id",         IntegerType, true),
                  StructField("imp_time",                 StringType,  true),
                  StructField("is_exclusive",             IntegerType, true),
                  StructField("truncate_ip",              IntegerType, true),
                  StructField("datacenter_id",            IntegerType, true),
                  StructField("device_id",                IntegerType, true),
                  StructField("carrier_id",               IntegerType, true),
                  StructField("creative_audit_status",    IntegerType, true),
                  StructField("is_creative_hosted",       IntegerType, true),
                  StructField("city",                     IntegerType, true),
                  StructField("latitude",                 StringType,  true),
                  StructField("longitude",                StringType,  true),
                  StructField("device_unique_id",         StringType,  true),
                  StructField("targeted_segments",        StringType,  true),
                  StructField("supply_type",              IntegerType, true),
                  StructField("is_toolbar",               IntegerType, true),
                  StructField("control_pct",              DoubleType,  true),
                  StructField("deal_id",                  IntegerType, true),
                  StructField("vp_bitmap",                LongType,    true),
                  StructField("ozone_id",                 IntegerType, true),
                  StructField("is_performance",           IntegerType, true),
                  StructField("sdk_version",              StringType,  true),
                  StructField("device_type",              IntegerType, true),
                  StructField("dma",                      IntegerType, true),
                  StructField("postal",                   StringType,  true),
                  StructField("package_id",               IntegerType, true),
                  StructField("campaign_group_freq",      IntegerType, true),
                  StructField("campaign_group_rec",       IntegerType, true),
                  StructField("insertion_order_freq",     IntegerType, true),
                  StructField("insertion_order_rec",      IntegerType, true),
                  StructField("buyer_gender",             StringType,  true),
                  StructField("buyer_age",                IntegerType, true),
                  StructField("targeted_segment_list",
                              ArrayType(IntegerType, true),
                              true
                  ),
                  StructField("custom_model_id",             IntegerType, true),
                  StructField("custom_model_last_modified",  LongType,    true),
                  StructField("custom_model_output_code",    StringType,  true),
                  StructField("external_uid",                StringType,  true),
                  StructField("request_uuid",                StringType,  true),
                  StructField("mobile_app_instance_id",      IntegerType, true),
                  StructField("traffic_source_code",         StringType,  true),
                  StructField("external_request_id",         StringType,  true),
                  StructField("stitch_group_id",             StringType,  true),
                  StructField("deal_type",                   IntegerType, true),
                  StructField("ym_floor_id",                 IntegerType, true),
                  StructField("ym_bias_id",                  IntegerType, true),
                  StructField("bid_priority",                IntegerType, true),
                  StructField("viewdef_definition_id",       IntegerType, true),
                  StructField("view_result",                 IntegerType, true),
                  StructField("view_non_measurable_reason",  IntegerType, true),
                  StructField("view_detection_enabled",      IntegerType, true),
                  StructField("device_make_id",              IntegerType, true),
                  StructField("operating_system_family_id",  IntegerType, true),
                  StructField("pricing_media_type",          IntegerType, true),
                  StructField("buyer_trx_event_id",          IntegerType, true),
                  StructField("seller_trx_event_id",         IntegerType, true),
                  StructField("is_unit_of_trx",              BooleanType, true),
                  StructField("revenue_auction_event_type",  IntegerType, true),
                  StructField("is_prebid",                   BooleanType, true),
                  StructField("auction_timestamp",           LongType,    true),
                  StructField("two_phase_reduction_applied", BooleanType, true),
                  StructField("region_id",                   IntegerType, true),
                  StructField("media_company_id",            IntegerType, true),
                  StructField("trade_agreement_id",          IntegerType, true),
                  StructField(
                    "personal_data",
                    StructType(
                      Array(
                        StructField("user_id_64",       LongType,   true),
                        StructField("device_unique_id", StringType, true),
                        StructField("external_uid",     StringType, true),
                        StructField("ip_address",       BinaryType, true),
                        StructField(
                          "crossdevice_group",
                          StructType(
                            Array(StructField("graph_id", IntegerType, true),
                                  StructField("group_id", LongType,    true)
                            )
                          ),
                          false
                        ),
                        StructField("latitude",            DoubleType,  true),
                        StructField("longitude",           DoubleType,  true),
                        StructField("ipv6_address",        BinaryType,  true),
                        StructField("subject_to_gdpr",     BooleanType, true),
                        StructField("geo_country",         StringType,  true),
                        StructField("gdpr_consent_string", StringType,  true),
                        StructField("preempt_ip_address",  BinaryType,  true),
                        StructField("device_type",         IntegerType, true),
                        StructField("device_make_id",      IntegerType, true),
                        StructField("device_model_id",     IntegerType, true),
                        StructField("new_user_id_64",      LongType,    true),
                        StructField("is_service_provider_mode",
                                    BooleanType,
                                    true
                        ),
                        StructField("is_personal_info_sale", BooleanType, true)
                      )
                    ),
                    true
                  ),
                  StructField(
                    "anonymized_user_info",
                    StructType(Array(StructField("user_id", BinaryType, true))),
                    true
                  ),
                  StructField("fx_rate_snapshot_id", IntegerType, true),
                  StructField(
                    "crossdevice_group_anon",
                    StructType(
                      Array(StructField("graph_id", IntegerType, true),
                            StructField("group_id", BinaryType,  true)
                      )
                    ),
                    true
                  ),
                  StructField("revenue_event_type_id", IntegerType, true),
                  StructField("external_creative_id",  StringType,  true),
                  StructField(
                    "targeted_segment_details",
                    ArrayType(
                      StructType(
                        Array(StructField("segment_id",    IntegerType, true),
                              StructField("last_seen_min", IntegerType, true)
                        )
                      ),
                      true
                    ),
                    true
                  ),
                  StructField("bidder_seat_id",         IntegerType, true),
                  StructField("is_curated",             BooleanType, true),
                  StructField("curator_member_id",      IntegerType, true),
                  StructField("cold_start_price_type",  IntegerType, true),
                  StructField("discovery_state",        IntegerType, true),
                  StructField("billing_period_id",      IntegerType, true),
                  StructField("flight_id",              IntegerType, true),
                  StructField("split_id",               IntegerType, true),
                  StructField("discovery_prediction",   DoubleType,  true),
                  StructField("campaign_group_type_id", IntegerType, true),
                  StructField(
                    "excluded_targeted_segment_details",
                    ArrayType(
                      StructType(
                        Array(StructField("segment_id", IntegerType, true))
                      ),
                      true
                    ),
                    true
                  ),
                  StructField("predicted_kpi_event_rate", DoubleType, true),
                  StructField("has_crossdevice_reach_extension",
                              BooleanType,
                              true
                  ),
                  StructField("counterparty_ruleset_type", IntegerType, true),
                  StructField(
                    "log_product_ads",
                    StructType(
                      Array(
                        StructField("product_feed_id", IntegerType, true),
                        StructField("item_selection_strategy_id",
                                    IntegerType,
                                    true
                        ),
                        StructField("product_uuid", StringType, true)
                      )
                    ),
                    true
                  ),
                  StructField("hb_source",                IntegerType, true),
                  StructField("buyer_line_item_currency", StringType,  true),
                  StructField("deal_line_item_currency",  StringType,  true),
                  StructField(
                    "personal_identifiers_experimental",
                    ArrayType(
                      StructType(
                        Array(StructField("identity_type",  IntegerType, true),
                              StructField("identity_value", StringType,  true)
                        )
                      ),
                      true
                    ),
                    true
                  ),
                  StructField("postal_code_ext_id", IntegerType, true),
                  StructField(
                    "targeted_segment_details_by_id_type",
                    ArrayType(
                      StructType(
                        Array(
                          StructField("identity_type", IntegerType, true),
                          StructField(
                            "targeted_segment_details",
                            ArrayType(
                              StructType(
                                Array(
                                  StructField("segment_id", IntegerType, true),
                                  StructField("last_seen_min",
                                              IntegerType,
                                              true
                                  )
                                )
                              ),
                              true
                            ),
                            true
                          )
                        )
                      ),
                      true
                    ),
                    true
                  ),
                  StructField(
                    "personal_identifiers",
                    ArrayType(
                      StructType(
                        Array(StructField("identity_type",  IntegerType, true),
                              StructField("identity_value", StringType,  true)
                        )
                      ),
                      true
                    ),
                    true
                  ),
                  StructField("buyer_dpvp_bitmap",  LongType, true),
                  StructField("seller_dpvp_bitmap", LongType, true)
                )
              ),
              true
            )
          )
        )
      )
      .load("asd")

}
