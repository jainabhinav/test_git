package io.prophecy.pipelines.test_pipeline.functions

import io.prophecy.pipelines.test_pipeline.functions.UDFs._
import _root_.io.prophecy.libs._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.sql.expressions._
import java.time._

object ColumnFunctions extends Serializable {

  def xcdf_member_owner_object_check(
    imp_type:                     Column,
    buyer_member_id:              Column,
    seller_member_id:             Column,
    advertiser_id:                Column,
    campaign_group_id:            Column,
    publisher_id:                 Column,
    advertiser_buyer_member_id:   Column,
    campaign_group_advertiser_id: Column,
    publisher_seller_member_id:   Column
  ): Column = {
    var good_data: org.apache.spark.sql.Column = lit(1)
    var owner_member_id: org.apache.spark.sql.Column =
      lit(null).cast(IntegerType)
    owner_member_id =
      when(is_not_null(imp_type).cast(BooleanType),
           when(imp_type.isin(lit(11), lit(10), lit(9), lit(5), lit(7)),
                buyer_member_id
           ).otherwise(seller_member_id)
      ).otherwise(owner_member_id)
    good_data = when(isnull(publisher_seller_member_id).and(
                       is_not_null(publisher_id).and(publisher_id =!= lit(0))
                     ),
                     lit(0)
    ).when(
        is_not_null(campaign_group_id)
          .and(campaign_group_id =!= lit(0))
          .and(is_not_null(advertiser_id))
          .and(advertiser_id =!= lit(0))
          .and(
            is_not_null(imp_type)
              .and(imp_type.isin(lit(6), lit(2)))
              .and(is_not_null(seller_member_id))
              .and(is_not_null(publisher_seller_member_id))
              .and(seller_member_id =!= publisher_seller_member_id)
              .or(
                is_not_null(imp_type)
                  .and(imp_type =!= lit(6))
                  .and(imp_type =!= lit(2))
                  .and(is_not_null(buyer_member_id))
                  .and(is_not_null(advertiser_buyer_member_id))
                  .and(buyer_member_id =!= advertiser_buyer_member_id)
              )
              .or(
                is_not_null(owner_member_id)
                  .and(is_not_null(advertiser_buyer_member_id))
                  .and(owner_member_id =!= advertiser_buyer_member_id)
              )
              .or(
                is_not_null(owner_member_id)
                  .and(is_not_null(advertiser_buyer_member_id))
                  .and(is_not_null(campaign_group_advertiser_id))
                  .and(owner_member_id === advertiser_buyer_member_id)
                  .and(advertiser_id =!= campaign_group_advertiser_id)
              )
          ),
        lit(0)
      )
      .when(
        is_not_null(imp_type)
          .and(imp_type.isin(lit(6), lit(2)))
          .and(is_not_null(seller_member_id))
          .and(is_not_null(publisher_seller_member_id))
          .and(seller_member_id =!= publisher_seller_member_id),
        lit(0)
      )
      .when(
        is_not_null(publisher_id)
          .and(is_not_null(publisher_seller_member_id))
          .and(is_not_null(seller_member_id))
          .and(publisher_id =!= lit(0))
          .and(publisher_seller_member_id =!= seller_member_id),
        lit(0)
      )
      .when(
        is_not_null(imp_type)
          .and(imp_type === lit(5))
          .and(is_not_null(seller_member_id))
          .and(is_not_null(buyer_member_id))
          .and(buyer_member_id =!= lit(0))
          .and(seller_member_id =!= buyer_member_id),
        lit(0)
      )
      .otherwise(good_data)
    good_data === lit(1)
  }

  def xcdf_member_owner_object_no_imp_type_check(
    buyer_member_id:              Column,
    seller_member_id:             Column,
    advertiser_id:                Column,
    campaign_group_id:            Column,
    publisher_id:                 Column,
    advertiser_buyer_member_id:   Column,
    campaign_group_advertiser_id: Column,
    publisher_seller_member_id:   Column
  ): Column = {
    var good_data: org.apache.spark.sql.Column = lit(1)
    good_data = when(isnull(publisher_seller_member_id).and(
                       is_not_null(publisher_id).and(publisher_id =!= lit(0))
                     ),
                     lit(0)
    ).when(
        is_not_null(campaign_group_id)
          .and(campaign_group_id =!= lit(0))
          .and(is_not_null(advertiser_id))
          .and(advertiser_id =!= lit(0))
          .and(
            is_not_null(seller_member_id)
              .and(is_not_null(publisher_seller_member_id))
              .and(seller_member_id =!= publisher_seller_member_id)
              .or(
                is_not_null(buyer_member_id)
                  .and(is_not_null(advertiser_buyer_member_id))
                  .and(buyer_member_id =!= advertiser_buyer_member_id)
              )
              .or(
                is_not_null(buyer_member_id)
                  .and(is_not_null(advertiser_buyer_member_id))
                  .and(is_not_null(campaign_group_advertiser_id))
                  .and(buyer_member_id === advertiser_buyer_member_id)
                  .and(advertiser_id =!= campaign_group_advertiser_id)
              )
          ),
        lit(0)
      )
      .when(is_not_null(seller_member_id)
              .and(is_not_null(publisher_seller_member_id))
              .and(seller_member_id =!= publisher_seller_member_id),
            lit(0)
      )
      .when(
        is_not_null(publisher_id)
          .and(is_not_null(publisher_seller_member_id))
          .and(is_not_null(seller_member_id))
          .and(publisher_id =!= lit(0))
          .and(publisher_seller_member_id =!= seller_member_id),
        lit(0)
      )
      .otherwise(good_data)
    good_data === lit(1)
  }

  def f_get_buyer_bid_bucket(bid: Column): Column = {
    var MIN_BID:   org.apache.spark.sql.Column = lit(0.01d)
    var MAX_BID:   org.apache.spark.sql.Column = lit(1000.0d)
    var BID_SCALE: org.apache.spark.sql.Column = lit(1000000.0d)
    var MIN_BUCKET: org.apache.spark.sql.Column =
      (MIN_BID * BID_SCALE).cast(IntegerType)
    var MAX_BUCKET: org.apache.spark.sql.Column =
      (MAX_BID * BID_SCALE).cast(IntegerType)
    var BUCKET_SIZE: org.apache.spark.sql.Column =
      ((log10(MAX_BID) - log10(MIN_BID)) / lit(1500)).cast(DoubleType)
    var l_bid:        org.apache.spark.sql.Column = lit(0)
    var l_bid_bucket: org.apache.spark.sql.Column = lit(0)
    l_bid = when(is_not_null(bid).cast(BooleanType), bid)
      .otherwise(l_bid)
      .cast(DoubleType)
    l_bid_bucket = when(
      l_bid > lit(0),
      when(l_bid <= MIN_BID,    MIN_BUCKET)
        .when(l_bid >= MAX_BID, MAX_BUCKET)
        .otherwise(
          floor(
            pow(lit(10),
                (decimal_round(log10(l_bid) / BUCKET_SIZE, 0) * BUCKET_SIZE)
                  .cast(DoubleType)
            ) * BID_SCALE
          )
        )
    ).otherwise(l_bid_bucket).cast(IntegerType)
    l_bid_bucket
  }

  def f_preempt_over_impression_non_zero_waterfall(
    impression_value: Column,
    preempt_value:    Column
  ): Column =
    coalesce(
      when(is_not_null(preempt_value).and(preempt_value =!= lit(0)),
           preempt_value
      ),
      when(is_not_null(impression_value).and(impression_value =!= lit(0)),
           impression_value
      ),
      lit(null)
    )

  def f_get_pricing_term(term_id: Column, pricing_terms: Column): Column =
    when(
      is_not_null(
        when(is_not_null(pricing_terms).cast(BooleanType), pricing_terms)
          .otherwise(
            lit(null).cast(
              ArrayType(
                StructType(
                  List(
                    StructField("term_id",                 IntegerType, true),
                    StructField("amount",                  DoubleType,  true),
                    StructField("rate",                    DoubleType,  true),
                    StructField("is_deduction",            BooleanType, true),
                    StructField("is_media_cost_dependent", BooleanType, true),
                    StructField("data_member_id",          IntegerType, true)
                  )
                ),
                true
              )
            )
          )
      ).cast(BooleanType),
      temp1248596_UDF(
        lit(0),
        term_id,
        when(is_not_null(pricing_terms).cast(BooleanType), pricing_terms)
          .otherwise(
            lit(null).cast(
              ArrayType(
                StructType(
                  List(
                    StructField("term_id",                 IntegerType, true),
                    StructField("amount",                  DoubleType,  true),
                    StructField("rate",                    DoubleType,  true),
                    StructField("is_deduction",            BooleanType, true),
                    StructField("is_media_cost_dependent", BooleanType, true),
                    StructField("data_member_id",          IntegerType, true)
                  )
                ),
                true
              )
            )
          ),
        lit(0)
      ).getField("l_pricing_term")
    ).otherwise(
      lit(null).cast(
        StructType(
          List(
            StructField("term_id",                 IntegerType, true),
            StructField("amount",                  DoubleType,  true),
            StructField("rate",                    DoubleType,  true),
            StructField("is_deduction",            BooleanType, true),
            StructField("is_media_cost_dependent", BooleanType, true),
            StructField("data_member_id",          IntegerType, true)
          )
        )
      )
    )

  def f_viewdef_definition_id(
    impressions_viewdef_definition_id_buyer_member: Column,
    preempt_viewdef_definition_id_buyer_member:     Column,
    impbus_view_viewdef_definition_id:              Column
  ): Column = {
    var l_viewdef_definition_id: org.apache.spark.sql.Column = lit(0)
    l_viewdef_definition_id =
      when(is_not_null(impbus_view_viewdef_definition_id).and(
             impbus_view_viewdef_definition_id > lit(0)
           ),
           impbus_view_viewdef_definition_id
      ).when(is_not_null(preempt_viewdef_definition_id_buyer_member).cast(
                BooleanType
              ),
              preempt_viewdef_definition_id_buyer_member
        )
        .when(
          is_not_null(impressions_viewdef_definition_id_buyer_member)
            .and(impressions_viewdef_definition_id_buyer_member =!= lit(0)),
          impressions_viewdef_definition_id_buyer_member
        )
        .otherwise(l_viewdef_definition_id)
        .cast(IntegerType)
    l_viewdef_definition_id
  }

  def f_convert_pricing_term_amount(
    in_pricing_terms: Column,
    magnitude:        Column
  ): Column =
    when(
      is_not_null(in_pricing_terms).cast(BooleanType),
      temp2704511_UDF(
        in_pricing_terms,
        lit(null).cast(
          ArrayType(
            StructType(
              List(
                StructField("term_id",                 IntegerType, true),
                StructField("amount",                  DoubleType,  true),
                StructField("rate",                    DoubleType,  true),
                StructField("is_deduction",            BooleanType, true),
                StructField("is_media_cost_dependent", BooleanType, true),
                StructField("data_member_id",          IntegerType, true)
              )
            ),
            true
          )
        ),
        lit(0),
        magnitude
      ).getField("l_pricing_terms")
    ).otherwise(
      lit(null).cast(
        ArrayType(
          StructType(
            List(
              StructField("term_id",                 IntegerType, true),
              StructField("amount",                  DoubleType,  true),
              StructField("rate",                    DoubleType,  true),
              StructField("is_deduction",            BooleanType, true),
              StructField("is_media_cost_dependent", BooleanType, true),
              StructField("data_member_id",          IntegerType, true)
            )
          ),
          true
        )
      )
    )

  def f_drop_is_deduction_pricing_terms(unfiltered_terms: Column): Column =
    when(is_not_null(unfiltered_terms).cast(BooleanType),
         temp3446850_UDF(unfiltered_terms, unfiltered_terms, lit(0))
           .getField("l_filtered_terms")
    )

  def f_get_transaction_event_pricing(
    impression_transaction_event_pricing: Column,
    auction_transaction_event_pricing:    Column,
    buyer_charges:                        Column,
    seller_charges:                       Column,
    should_process_views:                 Column
  ): Column = {
    var l_transaction_event_pricing: org.apache.spark.sql.Column = struct(
      lit(null).as("gross_payment_value_microcents"),
      lit(null).as("net_payment_value_microcents"),
      lit(null).as("seller_revenue_microcents"),
      lit(null).as("buyer_charges"),
      lit(null).as("seller_charges"),
      lit(null).as("buyer_transacted"),
      lit(null).as("seller_transacted")
    )
    l_transaction_event_pricing = impression_transaction_event_pricing
    l_transaction_event_pricing = struct(
      when(
        (should_process_views === lit(1))
          .and(
            is_not_null(
              auction_transaction_event_pricing.getField("buyer_transacted")
            )
          )
          .and(
            auction_transaction_event_pricing
              .getField("buyer_transacted") === lit(1)
          ),
        auction_transaction_event_pricing.getField(
          "gross_payment_value_microcents"
        )
      ).otherwise(
          impression_transaction_event_pricing
            .getField("gross_payment_value_microcents")
        )
        .as("gross_payment_value_microcents"),
      when(
        (should_process_views === lit(1))
          .and(
            is_not_null(
              auction_transaction_event_pricing.getField("buyer_transacted")
            )
          )
          .and(
            auction_transaction_event_pricing
              .getField("buyer_transacted") === lit(1)
          ),
        auction_transaction_event_pricing.getField(
          "net_payment_value_microcents"
        )
      ).otherwise(
          impression_transaction_event_pricing
            .getField("net_payment_value_microcents")
        )
        .as("net_payment_value_microcents"),
      when(
        (should_process_views === lit(1))
          .and(
            is_not_null(
              auction_transaction_event_pricing.getField("seller_transacted")
            )
          )
          .and(
            auction_transaction_event_pricing
              .getField("seller_transacted") === lit(1)
          ),
        auction_transaction_event_pricing.getField("seller_revenue_microcents")
      ).otherwise(
          impression_transaction_event_pricing
            .getField("seller_revenue_microcents")
        )
        .as("seller_revenue_microcents"),
      when(
        is_not_null(
          impression_transaction_event_pricing.getField("buyer_transacted")
        ).and(
            impression_transaction_event_pricing
              .getField("buyer_transacted") =!= lit(1)
          )
          .and(
            isnull(
              impression_transaction_event_pricing
                .getField("buyer_charges")
                .getField("pricing_terms")
            )
          ),
        buyer_charges
      ).when(
          (should_process_views === lit(1))
            .and(
              is_not_null(
                auction_transaction_event_pricing.getField("buyer_transacted")
              )
            )
            .and(
              auction_transaction_event_pricing
                .getField("buyer_transacted") === lit(1)
            ),
          auction_transaction_event_pricing.getField("buyer_charges")
        )
        .otherwise(
          impression_transaction_event_pricing.getField("buyer_charges")
        )
        .as("buyer_charges"),
      when(
        is_not_null(
          impression_transaction_event_pricing.getField("seller_transacted")
        ).and(
            impression_transaction_event_pricing
              .getField("seller_transacted") =!= lit(1)
          )
          .and(
            isnull(
              impression_transaction_event_pricing
                .getField("seller_charges")
                .getField("pricing_terms")
            )
          ),
        seller_charges
      ).when(
          (should_process_views === lit(1))
            .and(
              is_not_null(
                auction_transaction_event_pricing.getField("seller_transacted")
              )
            )
            .and(
              auction_transaction_event_pricing
                .getField("seller_transacted") === lit(1)
            ),
          auction_transaction_event_pricing.getField("seller_charges")
        )
        .otherwise(
          impression_transaction_event_pricing.getField("seller_charges")
        )
        .as("seller_charges"),
      when(
        (should_process_views === lit(1))
          .and(
            is_not_null(
              auction_transaction_event_pricing.getField("buyer_transacted")
            )
          )
          .and(
            auction_transaction_event_pricing
              .getField("buyer_transacted") === lit(1)
          ),
        auction_transaction_event_pricing.getField("buyer_transacted")
      ).otherwise(
          impression_transaction_event_pricing.getField("buyer_transacted")
        )
        .as("buyer_transacted"),
      when(
        (should_process_views === lit(1))
          .and(
            is_not_null(
              auction_transaction_event_pricing.getField("seller_transacted")
            )
          )
          .and(
            auction_transaction_event_pricing
              .getField("seller_transacted") === lit(1)
          ),
        auction_transaction_event_pricing.getField("seller_transacted")
      ).otherwise(
          impression_transaction_event_pricing.getField("seller_transacted")
        )
        .as("seller_transacted")
    )
    l_transaction_event_pricing
  }

  def f_view_non_measurable_reason(
    view_detection_enabled: Column,
    impbus_view_result:     Column
  ): Column = {
    var l_view_non_measurable_reason: org.apache.spark.sql.Column = lit(0)
    l_view_non_measurable_reason = when(view_detection_enabled === lit(0),
                                        lit(1)
    ).when(isnull(impbus_view_result).cast(BooleanType), lit(2))
      .when(is_not_null(impbus_view_result).and(impbus_view_result === lit(3)),
            lit(3)
      )
      .otherwise(l_view_non_measurable_reason)
      .cast(IntegerType)
    l_view_non_measurable_reason
  }

  def f_viewdef_viewable(
    viewdef_definition_id:           Column,
    view_measurable:                 Column,
    impbus_view_viewdef_view_result: Column
  ): Column = {
    var l_viewdef_viewable: org.apache.spark.sql.Column = lit(0)
    l_viewdef_viewable = when(
      (viewdef_definition_id > lit(0))
        .and(view_measurable === lit(1))
        .and(is_not_null(impbus_view_viewdef_view_result))
        .and(impbus_view_viewdef_view_result === lit(1)),
      lit(1)
    ).otherwise(l_viewdef_viewable).cast(IntegerType)
    l_viewdef_viewable
  }

  def f_view_result(
    view_detection_enabled: Column,
    impbus_view_result:     Column
  ): Column = {
    var l_view_result: org.apache.spark.sql.Column = lit(0)
    l_view_result = when(
      is_not_null(impbus_view_result).and(view_detection_enabled === lit(1)),
      impbus_view_result
    ).when(view_detection_enabled === lit(1), lit(3))
      .otherwise(lit(0))
      .cast(IntegerType)
    l_view_result
  }

  def f_transaction_event(
    impression_transaction_def: Column,
    preempt_transaction_def:    Column
  ): Column = {
    var l_transaction_event: org.apache.spark.sql.Column = lit(1)
    l_transaction_event = when(
      is_not_null(preempt_transaction_def).cast(BooleanType),
      coalesce(preempt_transaction_def.getField("transaction_event"), lit(0))
    ).when(is_not_null(impression_transaction_def).cast(BooleanType),
            coalesce(impression_transaction_def.getField("transaction_event"),
                     lit(0)
            )
      )
      .otherwise(l_transaction_event)
      .cast(IntegerType)
    l_transaction_event
  }

  def f_preempt_over_impression(
    impression_value: Column,
    preempt_value:    Column
  ): Column =
    coalesce(preempt_value, impression_value)

  def f_should_process_views(
    in_log_dw_view:      Column,
    seller_trx_event_id: Column,
    buyer_trx_event_id:  Column
  ): Column = {
    var l_should_process_views: org.apache.spark.sql.Column = lit(0)
    l_should_process_views = when(
      is_not_null(in_log_dw_view)
        .and(
          (coalesce(seller_trx_event_id, lit(0)) === lit(0))
            .or(coalesce(seller_trx_event_id, lit(0)) === lit(1))
        )
        .and(
          (coalesce(buyer_trx_event_id, lit(0)) === lit(2))
            .or(coalesce(buyer_trx_event_id, lit(0)) === lit(5))
            .or(coalesce(buyer_trx_event_id, lit(0)) === lit(6))
            .or(coalesce(buyer_trx_event_id, lit(0)) === lit(7))
            .or(coalesce(buyer_trx_event_id, lit(0)) === lit(8))
            .or(coalesce(buyer_trx_event_id, lit(0)) === lit(9))
            .or(coalesce(buyer_trx_event_id, lit(0)) === lit(13))
            .or(coalesce(buyer_trx_event_id, lit(0)) === lit(14))
            .or(coalesce(buyer_trx_event_id, lit(0)) === lit(15))
            .or(coalesce(buyer_trx_event_id, lit(0)) === lit(16))
        ),
      lit(1)
    ).otherwise(l_should_process_views).cast(IntegerType)
    l_should_process_views
  }

  def f_get_impression_transaction_event_pricing(
    impression_transaction_event_pricing: Column,
    buyer_charges:                        Column,
    seller_charges:                       Column
  ): Column = {
    var l_transaction_event_pricing: org.apache.spark.sql.Column = struct(
      lit(null).as("gross_payment_value_microcents"),
      lit(null).as("net_payment_value_microcents"),
      lit(null).as("seller_revenue_microcents"),
      lit(null).as("buyer_charges"),
      lit(null).as("seller_charges"),
      lit(null).as("buyer_transacted"),
      lit(null).as("seller_transacted")
    )
    l_transaction_event_pricing = impression_transaction_event_pricing
    l_transaction_event_pricing = struct(
      impression_transaction_event_pricing
        .getField("gross_payment_value_microcents")
        .as("gross_payment_value_microcents"),
      impression_transaction_event_pricing
        .getField("net_payment_value_microcents")
        .as("net_payment_value_microcents"),
      impression_transaction_event_pricing
        .getField("seller_revenue_microcents")
        .as("seller_revenue_microcents"),
      when(
        is_not_null(
          impression_transaction_event_pricing.getField("buyer_transacted")
        ).and(
            impression_transaction_event_pricing
              .getField("buyer_transacted") =!= lit(1)
          )
          .and(
            isnull(
              impression_transaction_event_pricing
                .getField("buyer_charges")
                .getField("pricing_terms")
            )
          ),
        buyer_charges
      ).otherwise(
          impression_transaction_event_pricing.getField("buyer_charges")
        )
        .as("buyer_charges"),
      when(
        is_not_null(
          impression_transaction_event_pricing.getField("seller_transacted")
        ).and(
            impression_transaction_event_pricing
              .getField("seller_transacted") =!= lit(1)
          )
          .and(
            isnull(
              impression_transaction_event_pricing
                .getField("seller_charges")
                .getField("pricing_terms")
            )
          ),
        seller_charges
      ).otherwise(
          impression_transaction_event_pricing.getField("seller_charges")
        )
        .as("seller_charges"),
      impression_transaction_event_pricing
        .getField("buyer_transacted")
        .as("buyer_transacted"),
      impression_transaction_event_pricing
        .getField("seller_transacted")
        .as("seller_transacted")
    )
    l_transaction_event_pricing
  }

  def f_preempt_over_impression_non_zero_explicit(
    preempt_exists:   Column,
    impression_value: Column,
    preempt_value:    Column
  ): Column =
    coalesce(
      when(preempt_exists
             .cast(BooleanType)
             .and(is_not_null(preempt_value))
             .and(preempt_value =!= lit(0)),
           preempt_value
      ),
      when(not(preempt_exists.cast(BooleanType))
             .and(is_not_null(impression_value))
             .and(impression_value =!= lit(0)),
           impression_value
      ),
      lit(null)
    )

  def f_view_measurable(
    view_detection_enabled: Column,
    impbus_view_result:     Column
  ): Column = {
    var l_view_measurable: org.apache.spark.sql.Column = lit(0)
    l_view_measurable = when(is_not_null(impbus_view_result)
                               .and(view_detection_enabled === lit(1))
                               .and(impbus_view_result.isin(lit(1), lit(2))),
                             lit(1)
    ).otherwise(l_view_measurable).cast(IntegerType)
    l_view_measurable
  }

  def f_should_zero_seller_revenue(
    bid:                       Column,
    imp_type:                  Column,
    revenue_type:              Column,
    seller_member_id:          Column,
    seller_revenue_microcents: Column
  ): Column = {
    var l_advertiser_id: org.apache.spark.sql.Column = lit(0)
    var l_retval:        org.apache.spark.sql.Column = lit(0)
    l_advertiser_id = when(is_not_null(bid).cast(BooleanType),
                           coalesce(bid.getField("advertiser_id"), lit(0))
    ).otherwise(l_advertiser_id)
    l_retval =
      when((coalesce(imp_type, lit(0)) === lit(4))
             .and(coalesce(seller_revenue_microcents, lit(0)) === lit(1))
             .and(l_advertiser_id > lit(0)),
           lit(1)
      ).otherwise(l_retval)
    l_retval = when(
      (coalesce(imp_type, lit(0)) === lit(5))
        .and(coalesce(revenue_type, lit(0)) === lit(6))
        .and(
          (coalesce(seller_member_id, lit(0)) === lit(280))
            .or(coalesce(seller_member_id, lit(0)) === lit(14007))
            .or(coalesce(seller_member_id, lit(0)) === lit(14120))
            .or(coalesce(seller_member_id, lit(0)) === lit(14136))
            .or(coalesce(seller_member_id, lit(0)) === lit(14634))
        ),
      lit(1)
    ).otherwise(l_retval)
    l_retval
  }

  def f_viewable(
    view_measurable:    Column,
    impbus_view_result: Column
  ): Column = {
    var l_viewable: org.apache.spark.sql.Column = lit(0)
    l_viewable = when((view_measurable === lit(1))
                        .and(is_not_null(impbus_view_result))
                        .and(impbus_view_result === lit(1)),
                      lit(1)
    ).otherwise(l_viewable).cast(IntegerType)
    l_viewable
  }

  def f_view_detection_enabled(
    impression_view_detection_enabled: Column,
    preempt_view_detection_enabled:    Column
  ): Column =
    coalesce(f_preempt_over_impression(impression_view_detection_enabled,
                                       preempt_view_detection_enabled
             ),
             lit(0)
    )

  def f_has_transacted(
    impression_transaction_def: Column,
    preempt_transaction_def:    Column
  ): Column = {
    var f_has_transacted: org.apache.spark.sql.Column = lit(0)
    f_has_transacted = when(
      (f_transaction_event(impression_transaction_def,
                           preempt_transaction_def
      ) === lit(0)).or(
        f_transaction_event(impression_transaction_def,
                            preempt_transaction_def
        ) === lit(1)
      ),
      lit(1)
    ).otherwise(f_has_transacted).cast(IntegerType)
    f_has_transacted
  }

  def seller_deduction_term_id_1(): Column =
    when(
      is_not_null(
        f_get_pricing_term(
          lit(1),
          when(
            is_not_null(
              f_get_transaction_event_pricing(
                col("log_impbus_impressions_pricing.impression_event_pricing"),
                col("log_impbus_auction_event.auction_event_pricing"),
                col("log_impbus_impressions_pricing.buyer_charges"),
                col("log_impbus_impressions_pricing.seller_charges"),
                f_should_process_views(
                  col("log_dw_view"),
                  f_transaction_event(
                    col("log_impbus_impressions.seller_transaction_def"),
                    col("log_impbus_preempt.seller_transaction_def")
                  ),
                  f_transaction_event(
                    col("log_impbus_impressions.buyer_transaction_def"),
                    col("log_impbus_preempt.buyer_transaction_def")
                  )
                )
              ).getField("seller_charges").getField("pricing_terms")
            ).cast(BooleanType),
            v_transaction_event_pricing()
              .getField("seller_charges")
              .getField("pricing_terms")
          ).otherwise(
            lit(null).cast(
              ArrayType(
                StructType(
                  List(
                    StructField("term_id",                 IntegerType, true),
                    StructField("amount",                  DoubleType,  true),
                    StructField("rate",                    DoubleType,  true),
                    StructField("is_deduction",            BooleanType, true),
                    StructField("is_media_cost_dependent", BooleanType, true),
                    StructField("data_member_id",          IntegerType, true)
                  )
                ),
                true
              )
            )
          )
        ).getField("amount")
      ).cast(BooleanType),
      when(
        is_not_null(
          f_get_pricing_term(
            lit(1),
            when(
              is_not_null(
                f_get_transaction_event_pricing(
                  col(
                    "log_impbus_impressions_pricing.impression_event_pricing"
                  ),
                  col("log_impbus_auction_event.auction_event_pricing"),
                  col("log_impbus_impressions_pricing.buyer_charges"),
                  col("log_impbus_impressions_pricing.seller_charges"),
                  f_should_process_views(
                    col("log_dw_view"),
                    f_transaction_event(
                      col("log_impbus_impressions.seller_transaction_def"),
                      col("log_impbus_preempt.seller_transaction_def")
                    ),
                    f_transaction_event(
                      col("log_impbus_impressions.buyer_transaction_def"),
                      col("log_impbus_preempt.buyer_transaction_def")
                    )
                  )
                ).getField("seller_charges").getField("pricing_terms")
              ).cast(BooleanType),
              v_transaction_event_pricing()
                .getField("seller_charges")
                .getField("pricing_terms")
            ).otherwise(
              lit(null).cast(
                ArrayType(
                  StructType(
                    List(
                      StructField("term_id",                 IntegerType, true),
                      StructField("amount",                  DoubleType,  true),
                      StructField("rate",                    DoubleType,  true),
                      StructField("is_deduction",            BooleanType, true),
                      StructField("is_media_cost_dependent", BooleanType, true),
                      StructField("data_member_id",          IntegerType, true)
                    )
                  ),
                  true
                )
              )
            )
          ).getField("is_deduction")
        ).and(
          f_get_pricing_term(
            lit(1),
            when(
              is_not_null(
                f_get_transaction_event_pricing(
                  col(
                    "log_impbus_impressions_pricing.impression_event_pricing"
                  ),
                  col("log_impbus_auction_event.auction_event_pricing"),
                  col("log_impbus_impressions_pricing.buyer_charges"),
                  col("log_impbus_impressions_pricing.seller_charges"),
                  f_should_process_views(
                    col("log_dw_view"),
                    f_transaction_event(
                      col("log_impbus_impressions.seller_transaction_def"),
                      col("log_impbus_preempt.seller_transaction_def")
                    ),
                    f_transaction_event(
                      col("log_impbus_impressions.buyer_transaction_def"),
                      col("log_impbus_preempt.buyer_transaction_def")
                    )
                  )
                ).getField("seller_charges").getField("pricing_terms")
              ).cast(BooleanType),
              v_transaction_event_pricing()
                .getField("seller_charges")
                .getField("pricing_terms")
            ).otherwise(
              lit(null).cast(
                ArrayType(
                  StructType(
                    List(
                      StructField("term_id",                 IntegerType, true),
                      StructField("amount",                  DoubleType,  true),
                      StructField("rate",                    DoubleType,  true),
                      StructField("is_deduction",            BooleanType, true),
                      StructField("is_media_cost_dependent", BooleanType, true),
                      StructField("data_member_id",          IntegerType, true)
                    )
                  ),
                  true
                )
              )
            )
          ).getField("is_deduction") === lit(1)
        ),
        f_get_pricing_term(
          lit(1),
          when(
            is_not_null(
              f_get_transaction_event_pricing(
                col("log_impbus_impressions_pricing.impression_event_pricing"),
                col("log_impbus_auction_event.auction_event_pricing"),
                col("log_impbus_impressions_pricing.buyer_charges"),
                col("log_impbus_impressions_pricing.seller_charges"),
                f_should_process_views(
                  col("log_dw_view"),
                  f_transaction_event(
                    col("log_impbus_impressions.seller_transaction_def"),
                    col("log_impbus_preempt.seller_transaction_def")
                  ),
                  f_transaction_event(
                    col("log_impbus_impressions.buyer_transaction_def"),
                    col("log_impbus_preempt.buyer_transaction_def")
                  )
                )
              ).getField("seller_charges").getField("pricing_terms")
            ).cast(BooleanType),
            v_transaction_event_pricing()
              .getField("seller_charges")
              .getField("pricing_terms")
          ).otherwise(
            lit(null).cast(
              ArrayType(
                StructType(
                  List(
                    StructField("term_id",                 IntegerType, true),
                    StructField("amount",                  DoubleType,  true),
                    StructField("rate",                    DoubleType,  true),
                    StructField("is_deduction",            BooleanType, true),
                    StructField("is_media_cost_dependent", BooleanType, true),
                    StructField("data_member_id",          IntegerType, true)
                  )
                ),
                true
              )
            )
          )
        ).getField("amount") / lit(1000)
      ).otherwise(lit(0))
    ).otherwise(lit(0)).cast(DoubleType)

  def seller_deduction_term_id_74(): Column =
    when(
      is_not_null(
        f_get_pricing_term(
          lit(74),
          when(
            is_not_null(
              f_get_transaction_event_pricing(
                col("log_impbus_impressions_pricing.impression_event_pricing"),
                col("log_impbus_auction_event.auction_event_pricing"),
                col("log_impbus_impressions_pricing.buyer_charges"),
                col("log_impbus_impressions_pricing.seller_charges"),
                f_should_process_views(
                  col("log_dw_view"),
                  f_transaction_event(
                    col("log_impbus_impressions.seller_transaction_def"),
                    col("log_impbus_preempt.seller_transaction_def")
                  ),
                  f_transaction_event(
                    col("log_impbus_impressions.buyer_transaction_def"),
                    col("log_impbus_preempt.buyer_transaction_def")
                  )
                )
              ).getField("seller_charges").getField("pricing_terms")
            ).cast(BooleanType),
            v_transaction_event_pricing()
              .getField("seller_charges")
              .getField("pricing_terms")
          ).otherwise(
            lit(null).cast(
              ArrayType(
                StructType(
                  List(
                    StructField("term_id",                 IntegerType, true),
                    StructField("amount",                  DoubleType,  true),
                    StructField("rate",                    DoubleType,  true),
                    StructField("is_deduction",            BooleanType, true),
                    StructField("is_media_cost_dependent", BooleanType, true),
                    StructField("data_member_id",          IntegerType, true)
                  )
                ),
                true
              )
            )
          )
        ).getField("amount")
      ).cast(BooleanType),
      when(
        is_not_null(
          f_get_pricing_term(
            lit(74),
            when(
              is_not_null(
                f_get_transaction_event_pricing(
                  col(
                    "log_impbus_impressions_pricing.impression_event_pricing"
                  ),
                  col("log_impbus_auction_event.auction_event_pricing"),
                  col("log_impbus_impressions_pricing.buyer_charges"),
                  col("log_impbus_impressions_pricing.seller_charges"),
                  f_should_process_views(
                    col("log_dw_view"),
                    f_transaction_event(
                      col("log_impbus_impressions.seller_transaction_def"),
                      col("log_impbus_preempt.seller_transaction_def")
                    ),
                    f_transaction_event(
                      col("log_impbus_impressions.buyer_transaction_def"),
                      col("log_impbus_preempt.buyer_transaction_def")
                    )
                  )
                ).getField("seller_charges").getField("pricing_terms")
              ).cast(BooleanType),
              v_transaction_event_pricing()
                .getField("seller_charges")
                .getField("pricing_terms")
            ).otherwise(
              lit(null).cast(
                ArrayType(
                  StructType(
                    List(
                      StructField("term_id",                 IntegerType, true),
                      StructField("amount",                  DoubleType,  true),
                      StructField("rate",                    DoubleType,  true),
                      StructField("is_deduction",            BooleanType, true),
                      StructField("is_media_cost_dependent", BooleanType, true),
                      StructField("data_member_id",          IntegerType, true)
                    )
                  ),
                  true
                )
              )
            )
          ).getField("is_deduction")
        ).and(
          f_get_pricing_term(
            lit(74),
            when(
              is_not_null(
                f_get_transaction_event_pricing(
                  col(
                    "log_impbus_impressions_pricing.impression_event_pricing"
                  ),
                  col("log_impbus_auction_event.auction_event_pricing"),
                  col("log_impbus_impressions_pricing.buyer_charges"),
                  col("log_impbus_impressions_pricing.seller_charges"),
                  f_should_process_views(
                    col("log_dw_view"),
                    f_transaction_event(
                      col("log_impbus_impressions.seller_transaction_def"),
                      col("log_impbus_preempt.seller_transaction_def")
                    ),
                    f_transaction_event(
                      col("log_impbus_impressions.buyer_transaction_def"),
                      col("log_impbus_preempt.buyer_transaction_def")
                    )
                  )
                ).getField("seller_charges").getField("pricing_terms")
              ).cast(BooleanType),
              v_transaction_event_pricing()
                .getField("seller_charges")
                .getField("pricing_terms")
            ).otherwise(
              lit(null).cast(
                ArrayType(
                  StructType(
                    List(
                      StructField("term_id",                 IntegerType, true),
                      StructField("amount",                  DoubleType,  true),
                      StructField("rate",                    DoubleType,  true),
                      StructField("is_deduction",            BooleanType, true),
                      StructField("is_media_cost_dependent", BooleanType, true),
                      StructField("data_member_id",          IntegerType, true)
                    )
                  ),
                  true
                )
              )
            )
          ).getField("is_deduction") === lit(1)
        ),
        f_get_pricing_term(
          lit(74),
          when(
            is_not_null(
              f_get_transaction_event_pricing(
                col("log_impbus_impressions_pricing.impression_event_pricing"),
                col("log_impbus_auction_event.auction_event_pricing"),
                col("log_impbus_impressions_pricing.buyer_charges"),
                col("log_impbus_impressions_pricing.seller_charges"),
                f_should_process_views(
                  col("log_dw_view"),
                  f_transaction_event(
                    col("log_impbus_impressions.seller_transaction_def"),
                    col("log_impbus_preempt.seller_transaction_def")
                  ),
                  f_transaction_event(
                    col("log_impbus_impressions.buyer_transaction_def"),
                    col("log_impbus_preempt.buyer_transaction_def")
                  )
                )
              ).getField("seller_charges").getField("pricing_terms")
            ).cast(BooleanType),
            v_transaction_event_pricing()
              .getField("seller_charges")
              .getField("pricing_terms")
          ).otherwise(
            lit(null).cast(
              ArrayType(
                StructType(
                  List(
                    StructField("term_id",                 IntegerType, true),
                    StructField("amount",                  DoubleType,  true),
                    StructField("rate",                    DoubleType,  true),
                    StructField("is_deduction",            BooleanType, true),
                    StructField("is_media_cost_dependent", BooleanType, true),
                    StructField("data_member_id",          IntegerType, true)
                  )
                ),
                true
              )
            )
          )
        ).getField("amount") / lit(1000)
      ).otherwise(lit(0))
    ).otherwise(lit(0)).cast(DoubleType)

  def buyer_charges_pricing_terms(): Column =
    when(
      is_not_null(
        when(
          is_not_null(
            f_get_transaction_event_pricing(
              col("log_impbus_impressions_pricing.impression_event_pricing"),
              col("log_impbus_auction_event.auction_event_pricing"),
              col("log_impbus_impressions_pricing.buyer_charges"),
              col("log_impbus_impressions_pricing.seller_charges"),
              f_should_process_views(
                col("log_dw_view"),
                f_transaction_event(
                  col("log_impbus_impressions.seller_transaction_def"),
                  col("log_impbus_preempt.seller_transaction_def")
                ),
                f_transaction_event(
                  col("log_impbus_impressions.buyer_transaction_def"),
                  col("log_impbus_preempt.buyer_transaction_def")
                )
              )
            ).getField("buyer_charges").getField("pricing_terms")
          ).cast(BooleanType),
          v_transaction_event_pricing()
            .getField("buyer_charges")
            .getField("pricing_terms")
        ).otherwise(
          lit(null).cast(
            ArrayType(
              StructType(
                List(
                  StructField("term_id",                 IntegerType, true),
                  StructField("amount",                  DoubleType,  true),
                  StructField("rate",                    DoubleType,  true),
                  StructField("is_deduction",            BooleanType, true),
                  StructField("is_media_cost_dependent", BooleanType, true),
                  StructField("data_member_id",          IntegerType, true)
                )
              ),
              true
            )
          )
        )
      ).cast(BooleanType),
      f_convert_pricing_term_amount(
        when(
          is_not_null(
            f_get_transaction_event_pricing(
              col("log_impbus_impressions_pricing.impression_event_pricing"),
              col("log_impbus_auction_event.auction_event_pricing"),
              col("log_impbus_impressions_pricing.buyer_charges"),
              col("log_impbus_impressions_pricing.seller_charges"),
              f_should_process_views(
                col("log_dw_view"),
                f_transaction_event(
                  col("log_impbus_impressions.seller_transaction_def"),
                  col("log_impbus_preempt.seller_transaction_def")
                ),
                f_transaction_event(
                  col("log_impbus_impressions.buyer_transaction_def"),
                  col("log_impbus_preempt.buyer_transaction_def")
                )
              )
            ).getField("buyer_charges").getField("pricing_terms")
          ).cast(BooleanType),
          v_transaction_event_pricing()
            .getField("buyer_charges")
            .getField("pricing_terms")
        ).otherwise(
          lit(null).cast(
            ArrayType(
              StructType(
                List(
                  StructField("term_id",                 IntegerType, true),
                  StructField("amount",                  DoubleType,  true),
                  StructField("rate",                    DoubleType,  true),
                  StructField("is_deduction",            BooleanType, true),
                  StructField("is_media_cost_dependent", BooleanType, true),
                  StructField("data_member_id",          IntegerType, true)
                )
              ),
              true
            )
          )
        ),
        lit(1000.0d)
      )
    ).otherwise(
      when(
        is_not_null(
          f_get_transaction_event_pricing(
            col("log_impbus_impressions_pricing.impression_event_pricing"),
            col("log_impbus_auction_event.auction_event_pricing"),
            col("log_impbus_impressions_pricing.buyer_charges"),
            col("log_impbus_impressions_pricing.seller_charges"),
            f_should_process_views(
              col("log_dw_view"),
              f_transaction_event(
                col("log_impbus_impressions.seller_transaction_def"),
                col("log_impbus_preempt.seller_transaction_def")
              ),
              f_transaction_event(
                col("log_impbus_impressions.buyer_transaction_def"),
                col("log_impbus_preempt.buyer_transaction_def")
              )
            )
          ).getField("buyer_charges").getField("pricing_terms")
        ).cast(BooleanType),
        v_transaction_event_pricing()
          .getField("buyer_charges")
          .getField("pricing_terms")
      ).otherwise(
        lit(null).cast(
          ArrayType(
            StructType(
              List(
                StructField("term_id",                 IntegerType, true),
                StructField("amount",                  DoubleType,  true),
                StructField("rate",                    DoubleType,  true),
                StructField("is_deduction",            BooleanType, true),
                StructField("is_media_cost_dependent", BooleanType, true),
                StructField("data_member_id",          IntegerType, true)
              )
            ),
            true
          )
        )
      )
    )

  def seller_charges_pricing_terms(): Column =
    when(
      is_not_null(
        when(
          is_not_null(
            f_get_transaction_event_pricing(
              col("log_impbus_impressions_pricing.impression_event_pricing"),
              col("log_impbus_auction_event.auction_event_pricing"),
              col("log_impbus_impressions_pricing.buyer_charges"),
              col("log_impbus_impressions_pricing.seller_charges"),
              f_should_process_views(
                col("log_dw_view"),
                f_transaction_event(
                  col("log_impbus_impressions.seller_transaction_def"),
                  col("log_impbus_preempt.seller_transaction_def")
                ),
                f_transaction_event(
                  col("log_impbus_impressions.buyer_transaction_def"),
                  col("log_impbus_preempt.buyer_transaction_def")
                )
              )
            ).getField("seller_charges").getField("pricing_terms")
          ).cast(BooleanType),
          v_transaction_event_pricing()
            .getField("seller_charges")
            .getField("pricing_terms")
        ).otherwise(
          lit(null).cast(
            ArrayType(
              StructType(
                List(
                  StructField("term_id",                 IntegerType, true),
                  StructField("amount",                  DoubleType,  true),
                  StructField("rate",                    DoubleType,  true),
                  StructField("is_deduction",            BooleanType, true),
                  StructField("is_media_cost_dependent", BooleanType, true),
                  StructField("data_member_id",          IntegerType, true)
                )
              ),
              true
            )
          )
        )
      ).cast(BooleanType),
      f_convert_pricing_term_amount(
        when(
          is_not_null(
            f_get_transaction_event_pricing(
              col("log_impbus_impressions_pricing.impression_event_pricing"),
              col("log_impbus_auction_event.auction_event_pricing"),
              col("log_impbus_impressions_pricing.buyer_charges"),
              col("log_impbus_impressions_pricing.seller_charges"),
              f_should_process_views(
                col("log_dw_view"),
                f_transaction_event(
                  col("log_impbus_impressions.seller_transaction_def"),
                  col("log_impbus_preempt.seller_transaction_def")
                ),
                f_transaction_event(
                  col("log_impbus_impressions.buyer_transaction_def"),
                  col("log_impbus_preempt.buyer_transaction_def")
                )
              )
            ).getField("seller_charges").getField("pricing_terms")
          ).cast(BooleanType),
          v_transaction_event_pricing()
            .getField("seller_charges")
            .getField("pricing_terms")
        ).otherwise(
          lit(null).cast(
            ArrayType(
              StructType(
                List(
                  StructField("term_id",                 IntegerType, true),
                  StructField("amount",                  DoubleType,  true),
                  StructField("rate",                    DoubleType,  true),
                  StructField("is_deduction",            BooleanType, true),
                  StructField("is_media_cost_dependent", BooleanType, true),
                  StructField("data_member_id",          IntegerType, true)
                )
              ),
              true
            )
          )
        ),
        lit(1000.0d)
      )
    ).otherwise(
      when(
        is_not_null(
          f_get_transaction_event_pricing(
            col("log_impbus_impressions_pricing.impression_event_pricing"),
            col("log_impbus_auction_event.auction_event_pricing"),
            col("log_impbus_impressions_pricing.buyer_charges"),
            col("log_impbus_impressions_pricing.seller_charges"),
            f_should_process_views(
              col("log_dw_view"),
              f_transaction_event(
                col("log_impbus_impressions.seller_transaction_def"),
                col("log_impbus_preempt.seller_transaction_def")
              ),
              f_transaction_event(
                col("log_impbus_impressions.buyer_transaction_def"),
                col("log_impbus_preempt.buyer_transaction_def")
              )
            )
          ).getField("seller_charges").getField("pricing_terms")
        ).cast(BooleanType),
        v_transaction_event_pricing()
          .getField("seller_charges")
          .getField("pricing_terms")
      ).otherwise(
        lit(null).cast(
          ArrayType(
            StructType(
              List(
                StructField("term_id",                 IntegerType, true),
                StructField("amount",                  DoubleType,  true),
                StructField("rate",                    DoubleType,  true),
                StructField("is_deduction",            BooleanType, true),
                StructField("is_media_cost_dependent", BooleanType, true),
                StructField("data_member_id",          IntegerType, true)
              )
            ),
            true
          )
        )
      )
    )

  def f_get_curator_margin(pricing_terms: Column): Column =
    when(is_not_null(pricing_terms).cast(BooleanType),
         temp692552_UDF(lit(0), lit(0), lit(0), pricing_terms, lit(0)).getField(
           "l_curator_margin"
         )
    ).otherwise(lit(0))

  def f_get_total_tech_fees(pricing_terms: Column): Column =
    when(is_not_null(pricing_terms).cast(BooleanType),
         temp1433148_UDF(lit(0), lit(0), lit(0), pricing_terms, lit(0))
           .getField("l_total_tech_fees")
    ).otherwise(lit(0))

  def f_get_seller_fees(pricing_terms: Column): Column =
    when(is_not_null(pricing_terms).cast(BooleanType),
         temp2173548_UDF(lit(0), lit(0), lit(0), pricing_terms, lit(0))
           .getField("l_seller_fees")
    ).otherwise(lit(0))

  def f_update_data_costs_deal(
    data_costs:                Column,
    member_sales_tax_rate_pct: Column
  ): Column =
    when(
      is_not_null(data_costs).cast(BooleanType),
      temp2912975_UDF(
        array(),
        member_sales_tax_rate_pct.cast(DoubleType),
        lit(0),
        lit(null).cast(
          StructType(
            List(
              StructField("data_member_id", IntegerType,                  true),
              StructField("cost",           DoubleType,                   true),
              StructField("used_segments",  ArrayType(IntegerType, true), true),
              StructField("cost_pct",       DoubleType,                   true)
            )
          )
        ),
        data_costs
      ).getField("l_data_costs")
    )

  def f_get_is_curator_margin_media_cost_dependent(
    pricing_terms: Column
  ): Column =
    when(is_not_null(pricing_terms).cast(BooleanType),
         temp3652655_UDF(lit(0), pricing_terms, lit(0), lit(0)).getField(
           "l_is_curator_margin_media_cost_dependent"
         )
    ).otherwise(lit(0)).cast(IntegerType)

  def f_get_curator_margin_type(pricing_terms: Column): Column =
    when(is_not_null(pricing_terms).cast(BooleanType),
         temp4393171_UDF(lit(0), pricing_terms, lit(0), lit(0)).getField(
           "l_curator_margin_type"
         )
    ).otherwise(lit(0)).cast(IntegerType)

  def is_not_null_right_n143(): Column =
    is_not_null(col("right.auction_id_64"))
      .or(is_not_null(col("right.date_time")))
      .or(is_not_null(col("right.is_delivered")))
      .or(is_not_null(col("right.is_dw")))
      .or(is_not_null(col("right.seller_member_id")))
      .or(is_not_null(col("right.buyer_member_id")))
      .or(is_not_null(col("right.member_id")))
      .or(is_not_null(col("right.publisher_id")))
      .or(is_not_null(col("right.site_id")))
      .or(is_not_null(col("right.tag_id")))
      .or(is_not_null(col("right.advertiser_id")))
      .or(is_not_null(col("right.campaign_group_id")))
      .or(is_not_null(col("right.campaign_id")))
      .or(is_not_null(col("right.insertion_order_id")))
      .or(is_not_null(col("right.imp_type")))
      .or(is_not_null(col("right.is_transactable")))
      .or(is_not_null(col("right.is_transacted_previously")))
      .or(is_not_null(col("right.is_deferred_impression")))
      .or(is_not_null(col("right.has_null_bid")))
      .or(is_not_null(col("right.log_impbus_impressions")))
      .or(is_not_null(col("right.log_impbus_preempt_count")))
      .or(is_not_null(col("right.log_impbus_preempt")))
      .or(is_not_null(col("right.log_impbus_preempt_dup")))
      .or(is_not_null(col("right.log_impbus_impressions_pricing_count")))
      .or(is_not_null(col("right.log_impbus_impressions_pricing")))
      .or(is_not_null(col("right.log_impbus_impressions_pricing_dup")))

  def f_convert_log_impbus_imptracker_to_log_impbus_impressions_pricing()
    : Column = {
    var l_buyer_charges: org.apache.spark.sql.Column = struct(
      lit(null).as("rate_card_id"),
      lit(null).as("member_id"),
      lit(null).as("is_dw"),
      array()
        .cast(
          ArrayType(
            StructType(
              List(
                StructField("term_id",                 IntegerType, true),
                StructField("amount",                  DoubleType,  true),
                StructField("rate",                    DoubleType,  true),
                StructField("is_deduction",            BooleanType, true),
                StructField("is_media_cost_dependent", BooleanType, true),
                StructField("data_member_id",          IntegerType, true)
              )
            ),
            true
          )
        )
        .as("pricing_terms"),
      lit(null).as("fx_margin_rate_id"),
      lit(null).as("marketplace_owner_id"),
      lit(null).as("virtual_marketplace_id"),
      lit(null).as("amino_enabled")
    )
    var l_seller_charges: org.apache.spark.sql.Column = struct(
      lit(null).as("rate_card_id"),
      lit(null).as("member_id"),
      lit(null).as("is_dw"),
      array()
        .cast(
          ArrayType(
            StructType(
              List(
                StructField("term_id",                 IntegerType, true),
                StructField("amount",                  DoubleType,  true),
                StructField("rate",                    DoubleType,  true),
                StructField("is_deduction",            BooleanType, true),
                StructField("is_media_cost_dependent", BooleanType, true),
                StructField("data_member_id",          IntegerType, true)
              )
            ),
            true
          )
        )
        .as("pricing_terms"),
      lit(null).as("fx_margin_rate_id"),
      lit(null).as("marketplace_owner_id"),
      lit(null).as("virtual_marketplace_id"),
      lit(null).as("amino_enabled")
    )
    var l_impression_event_pricing: org.apache.spark.sql.Column = struct(
      lit(null).as("gross_payment_value_microcents"),
      lit(null).as("net_payment_value_microcents"),
      lit(null).as("seller_revenue_microcents"),
      lit(null).as("buyer_charges"),
      lit(null).as("seller_charges"),
      lit(null).as("buyer_transacted"),
      lit(null).as("seller_transacted")
    )
    l_buyer_charges = struct(
      lit(null).cast(IntegerType).as("rate_card_id"),
      col("member_id").cast(IntegerType).as("member_id"),
      lit(1).as("is_dw"),
      array()
        .cast(
          ArrayType(
            StructType(
              List(
                StructField("term_id",                 IntegerType, true),
                StructField("amount",                  DoubleType,  true),
                StructField("rate",                    DoubleType,  true),
                StructField("is_deduction",            BooleanType, true),
                StructField("is_media_cost_dependent", BooleanType, true),
                StructField("data_member_id",          IntegerType, true)
              )
            ),
            true
          )
        )
        .as("pricing_terms"),
      lit(null).cast(IntegerType).as("fx_margin_rate_id"),
      lit(null).cast(IntegerType).as("marketplace_owner_id"),
      lit(null).cast(IntegerType).as("virtual_marketplace_id"),
      lit(null).cast(BooleanType).as("amino_enabled")
    )
    l_seller_charges = struct(
      lit(null).cast(IntegerType).as("rate_card_id"),
      col("member_id").cast(IntegerType).as("member_id"),
      lit(1).as("is_dw"),
      array()
        .cast(
          ArrayType(
            StructType(
              List(
                StructField("term_id",                 IntegerType, true),
                StructField("amount",                  DoubleType,  true),
                StructField("rate",                    DoubleType,  true),
                StructField("is_deduction",            BooleanType, true),
                StructField("is_media_cost_dependent", BooleanType, true),
                StructField("data_member_id",          IntegerType, true)
              )
            ),
            true
          )
        )
        .as("pricing_terms"),
      lit(null).cast(IntegerType).as("fx_margin_rate_id"),
      lit(null).cast(IntegerType).as("marketplace_owner_id"),
      lit(null).cast(IntegerType).as("virtual_marketplace_id"),
      lit(null).cast(BooleanType).as("amino_enabled")
    )
    l_impression_event_pricing = struct(
      lit(null).cast(LongType).as("gross_payment_value_microcents"),
      lit(null).cast(LongType).as("net_payment_value_microcents"),
      lit(null).cast(LongType).as("seller_revenue_microcents"),
      l_buyer_charges.as("buyer_charges"),
      l_seller_charges.as("seller_charges"),
      lit(null).cast(BooleanType).as("buyer_transacted"),
      lit(null).cast(BooleanType).as("seller_transacted")
    )
    struct(
      col("date_time").cast(LongType).as("date_time"),
      col("auction_id_64").cast(LongType).as("auction_id_64"),
      l_buyer_charges.as("buyer_charges"),
      l_seller_charges.as("seller_charges"),
      lit(0.0d).as("buyer_spend"),
      lit(0.0d).as("seller_revenue"),
      lit(null).cast(IntegerType).as("rate_card_auction_type"),
      lit(null).cast(IntegerType).as("rate_card_media_type"),
      lit(0).cast(BooleanType).as("direct_clear"),
      col("date_time").cast(LongType).as("auction_timestamp"),
      lit(null).cast(IntegerType).as("instance_id"),
      lit(0).cast(BooleanType).as("two_phase_reduction_applied"),
      lit(0).as("trade_agreement_id"),
      lit(null).cast(LongType).as("log_timestamp"),
      lit(null)
        .cast(
          StructType(
            List(
              StructField("applied_term_id",   IntegerType, true),
              StructField("applied_term_type", IntegerType, true),
              StructField("targeted_term_ids",
                          ArrayType(IntegerType, true),
                          true
              )
            )
          )
        )
        .as("trade_agreement_info"),
      lit(null).cast(BooleanType).as("is_buy_it_now"),
      lit(null).cast(DoubleType).as("net_buyer_spend"),
      l_impression_event_pricing.as("impression_event_pricing"),
      lit(null).cast(IntegerType).as("counterparty_ruleset_type"),
      lit(null).cast(FloatType).as("estimated_audience_imps"),
      lit(null).cast(FloatType).as("audience_imps")
    )
  }

  def f_convert_log_dw_imptracker_to_log_dw_bid(): Column =
    struct(
      col("date_time").cast(LongType).as("date_time"),
      col("auction_id_64").cast(LongType).as("auction_id_64"),
      lit(0.0d).as("price"),
      col("member_id").cast(IntegerType).as("member_id"),
      col("advertiser_id").cast(IntegerType).as("advertiser_id"),
      col("campaign_group_id").cast(IntegerType).as("campaign_group_id"),
      lit(0).as("campaign_id"),
      lit(0).as("creative_id"),
      lit(0).as("creative_freq"),
      lit(0).as("creative_rec"),
      lit(0).as("advertiser_freq"),
      lit(0).as("advertiser_rec"),
      lit(0).as("is_remarketing"),
      when(col("user_id_64").cast(LongType) =!= lit(0),
           col("user_id_64").cast(LongType) % lit(1000)
      ).otherwise(lit(-1)).as("user_group_id"),
      col("media_buy_cost").cast(DoubleType).as("media_buy_cost"),
      lit(0).as("is_default"),
      lit(0).as("pub_rule_id"),
      col("media_buy_rev_share_pct")
        .cast(DoubleType)
        .as("media_buy_rev_share_pct"),
      col("pricing_type").cast(StringType).as("pricing_type"),
      col("can_convert").cast(IntegerType).as("can_convert"),
      lit(0).as("is_control"),
      lit(0).as("control_pct"),
      lit(0).as("control_creative_id"),
      lit(0).as("cadence_modifier"),
      col("advertiser_currency").cast(StringType).as("advertiser_currency"),
      col("advertiser_exchange_rate")
        .cast(DoubleType)
        .as("advertiser_exchange_rate"),
      col("insertion_order_id").cast(IntegerType).as("insertion_order_id"),
      lit(-2).as("predict_type"),
      lit(0).as("predict_type_goal"),
      col("revenue_value").as("revenue_value_dollars"),
      col("revenue_value_adv_curr")
        .cast(DoubleType)
        .as("revenue_value_adv_curr"),
      col("commission_cpm").cast(DoubleType).as("commission_cpm"),
      col("commission_revshare").cast(DoubleType).as("commission_revshare"),
      lit(0).as("serving_fees_cpm"),
      lit(0).as("serving_fees_revshare"),
      lit("").as("publisher_currency"),
      lit(0).as("publisher_exchange_rate"),
      col("payment_type").cast(IntegerType).as("payment_type"),
      lit(null).cast(DoubleType).as("payment_value"),
      lit(0).as("creative_group_freq"),
      lit(0).as("creative_group_rec"),
      col("revenue_type").cast(IntegerType).as("revenue_type"),
      lit(0).as("apply_cost_on_default"),
      lit(0).as("instance_id"),
      lit(null).cast(IntegerType).as("vp_expose_age"),
      lit(null).cast(IntegerType).as("vp_expose_gender"),
      lit("").as("targeted_segments"),
      lit(null).cast(IntegerType).as("ttl"),
      col("date_time").cast(LongType).as("auction_timestamp"),
      lit(null)
        .cast(
          ArrayType(
            StructType(
              List(
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
          )
        )
        .as("data_costs"),
      lit(null).cast(ArrayType(IntegerType, true)).as("targeted_segment_list"),
      lit(0).as("campaign_group_freq"),
      lit(0).as("campaign_group_rec"),
      lit(0).as("insertion_order_freq"),
      lit(0).as("insertion_order_rec"),
      lit("u").as("buyer_gender"),
      lit(0).as("buyer_age"),
      lit(0).as("custom_model_id"),
      lit(0).as("custom_model_last_modified"),
      lit("").as("custom_model_output_code"),
      lit(null).cast(IntegerType).as("bid_priority"),
      lit(null).cast(IntegerType).as("explore_disposition"),
      lit(null).cast(IntegerType).as("revenue_auction_event_type"),
      lit(null)
        .cast(
          ArrayType(
            StructType(
              List(
                StructField("model_type", IntegerType, true),
                StructField("model_id",   IntegerType, true),
                StructField("leaf_code",  StringType,  true),
                StructField("origin",     IntegerType, true),
                StructField("experiment", IntegerType, true),
                StructField("value",      FloatType,   true)
              )
            ),
            true
          )
        )
        .as("campaign_group_models"),
      lit(null).cast(IntegerType).as("impression_transaction_type"),
      lit(null).cast(IntegerType).as("is_deferred"),
      lit(null).cast(IntegerType).as("log_type"),
      lit(null)
        .cast(
          StructType(
            List(StructField("graph_id", IntegerType, true),
                 StructField("group_id", BinaryType,  true)
            )
          )
        )
        .as("crossdevice_group_anon"),
      lit(null).cast(IntegerType).as("fx_rate_snapshot_id"),
      lit(null)
        .cast(
          StructType(
            List(StructField("graph_provider_member_id", IntegerType, true),
                 StructField("cost_cpm_usd",             DoubleType,  true)
            )
          )
        )
        .as("crossdevice_graph_cost"),
      lit(null).cast(IntegerType).as("revenue_event_type_id"),
      lit(null)
        .cast(
          ArrayType(StructType(
                      List(StructField("segment_id",    IntegerType, true),
                           StructField("last_seen_min", IntegerType, true)
                      )
                    ),
                    true
          )
        )
        .as("targeted_segment_details"),
      lit(null).cast(IntegerType).as("insertion_order_budget_interval_id"),
      lit(null).cast(IntegerType).as("campaign_group_budget_interval_id"),
      lit(null).cast(IntegerType).as("cold_start_price_type"),
      lit(null).cast(IntegerType).as("discovery_state"),
      col("revenue_info").as("revenue_info"),
      lit(null).cast(BooleanType).as("use_revenue_info"),
      lit(null).cast(DoubleType).as("sales_tax_rate_pct"),
      lit(null).cast(IntegerType).as("targeted_crossdevice_graph_id"),
      lit(null).cast(IntegerType).as("product_feed_id"),
      lit(null).cast(IntegerType).as("item_selection_strategy_id"),
      lit(null).cast(DoubleType).as("discovery_prediction"),
      lit(null).cast(IntegerType).as("bidding_host_id"),
      lit(null).cast(IntegerType).as("split_id"),
      lit(null)
        .cast(
          ArrayType(
            StructType(List(StructField("segment_id", IntegerType, true))),
            true
          )
        )
        .as("excluded_targeted_segment_details"),
      lit(null).cast(DoubleType).as("predicted_kpi_event_rate"),
      lit(null).cast(BooleanType).as("has_crossdevice_reach_extension"),
      lit(null).cast(DoubleType).as("advertiser_expected_value_ecpm_ac"),
      lit(null).cast(DoubleType).as("bpp_multiplier"),
      lit(null).cast(DoubleType).as("bpp_offset"),
      lit(null).cast(DoubleType).as("bid_modifier"),
      lit(null).cast(LongType).as("payment_value_microcents"),
      lit(null)
        .cast(
          ArrayType(StructType(
                      List(StructField("graph_id", IntegerType, true),
                           StructField("group_id", BinaryType,  true)
                      )
                    ),
                    true
          )
        )
        .as("crossdevice_graph_membership"),
      lit(null)
        .cast(
          ArrayType(
            StructType(
              List(
                StructField("kpi_event_id",               IntegerType, true),
                StructField("ev_kpi_event_ac",            DoubleType,  true),
                StructField("p_kpi_event",                DoubleType,  true),
                StructField("bpo_aggressiveness_factor",  DoubleType,  true),
                StructField("min_margin_pct",             DoubleType,  true),
                StructField("max_revenue_or_bid_value",   DoubleType,  true),
                StructField("min_revenue_or_bid_value",   DoubleType,  true),
                StructField("cold_start_price_ac",        DoubleType,  true),
                StructField("dynamic_bid_max_revenue_ac", DoubleType,  true),
                StructField("p_revenue_event",            DoubleType,  true),
                StructField("total_fees_deducted_ac",     DoubleType,  true)
              )
            ),
            true
          )
        )
        .as("valuation_landscape"),
      lit(null).cast(StringType).as("line_item_currency"),
      lit(null).cast(DoubleType).as("measurement_fee_cpm_usd"),
      lit(null).cast(IntegerType).as("measurement_provider_id"),
      lit(null).cast(IntegerType).as("measurement_provider_member_id"),
      lit(null).cast(IntegerType).as("offline_attribution_provider_member_id"),
      lit(null).cast(DoubleType).as("offline_attribution_cost_usd_cpm"),
      lit(null)
        .cast(
          ArrayType(
            StructType(
              List(
                StructField("identity_type", IntegerType, true),
                StructField(
                  "targeted_segment_details",
                  ArrayType(
                    StructType(
                      List(StructField("segment_id",    IntegerType, true),
                           StructField("last_seen_min", IntegerType, true)
                      )
                    ),
                    true
                  ),
                  true
                )
              )
            ),
            true
          )
        )
        .as("targeted_segment_details_by_id_type"),
      lit(null)
        .cast(
          ArrayType(StructType(
                      List(StructField("provider_member_id", IntegerType, true),
                           StructField("cost_usd_cpm",       DoubleType,  true)
                      )
                    ),
                    true
          )
        )
        .as("offline_attribution"),
      lit(null).cast(IntegerType).as("frequency_cap_type_internal"),
      lit(null)
        .cast(BooleanType)
        .as("modeled_cap_did_override_line_item_daily_cap"),
      lit(null).cast(DoubleType).as("modeled_cap_user_sample_rate"),
      lit(null).cast(DoubleType).as("bid_rate"),
      lit(null)
        .cast(ArrayType(IntegerType, true))
        .as("district_postal_code_lists"),
      lit(null).cast(DoubleType).as("pre_bpp_price"),
      lit(null).cast(IntegerType).as("feature_tests_bitmap")
    )

  def f_convert_log_impbus_imptracker_to_log_impbus_impressions(): Column = {
    var l_null_log_transaction_def: org.apache.spark.sql.Column = struct(
      lit(null).as("transaction_event"),
      lit(null).as("transaction_event_type_id")
    )
    l_null_log_transaction_def = struct(
      lit(1).as("transaction_event"),
      lit(null).cast(IntegerType).as("transaction_event_type_id")
    )
    struct(
      col("date_time").cast(LongType).as("date_time"),
      col("auction_id_64").cast(LongType).as("auction_id_64"),
      col("user_id_64").cast(LongType).as("user_id_64"),
      col("tag_id").cast(IntegerType).as("tag_id"),
      col("ip_address").cast(StringType).as("ip_address"),
      lit(0).as("venue_id"),
      col("site_domain").cast(StringType).as("site_domain"),
      lit(0).as("width"),
      lit(0).as("height"),
      col("geo_country").cast(StringType).as("geo_country"),
      coalesce(when(isnull(col("geo_region")).cast(BooleanType), lit("--"))
                 .otherwise(col("geo_region")),
               col("geo_region").cast(StringType)
      ).as("geo_region"),
      lit("u").as("gender"),
      lit(0).as("age"),
      lit(0).as("bidder_id"),
      col("member_id").cast(IntegerType).as("seller_member_id"),
      col("member_id").cast(IntegerType).as("buyer_member_id"),
      lit(0).as("creative_id"),
      lit(0).as("imp_blacklist_or_fraud"),
      lit(0).as("imp_bid_on"),
      lit(0).as("buyer_bid"),
      lit(0).as("buyer_spend"),
      lit(0.0d).as("seller_revenue"),
      lit(0).as("num_of_bids"),
      lit(0).as("ecp"),
      lit(0).as("reserve_price"),
      lit("").as("inv_code"),
      lit("").as("call_type"),
      lit(0).as("inventory_source_id"),
      lit(0).as("cookie_age"),
      lit(0).as("brand_id"),
      lit(0).as("cleared_direct"),
      lit(null).cast(DoubleType).as("forex_allowance"),
      lit(0).as("fold_position"),
      lit(0).as("external_inv_id"),
      lit(9).as("imp_type"),
      lit(1).as("is_delivered"),
      lit(1).as("is_dw"),
      col("publisher_id").cast(IntegerType).as("publisher_id"),
      col("site_id").cast(IntegerType).as("site_id"),
      col("content_category_id").cast(IntegerType).as("content_category_id"),
      col("datacenter_id").cast(IntegerType).as("datacenter_id"),
      lit(0).as("eap"),
      col("user_tz_offset").cast(IntegerType).as("user_tz_offset"),
      col("user_group_id").cast(IntegerType).as("user_group_id"),
      col("pub_rule_id").cast(IntegerType).as("pub_rule_id"),
      lit(0).as("media_type"),
      col("operating_system").cast(IntegerType).as("operating_system"),
      col("browser").cast(IntegerType).as("browser"),
      col("language").cast(IntegerType).as("language"),
      lit("").as("application_id"),
      lit("").as("user_locale"),
      lit(0).as("inventory_url_id"),
      lit(0).as("audit_type"),
      lit(0).as("shadow_price"),
      lit(0).as("impbus_id"),
      col("seller_currency").as("buyer_currency"),
      col("seller_exchange_rate").as("buyer_exchange_rate"),
      col("seller_currency").cast(StringType).as("seller_currency"),
      col("seller_exchange_rate").cast(DoubleType).as("seller_exchange_rate"),
      lit(1).as("vp_expose_domains"),
      lit(1).as("vp_expose_categories"),
      lit(1).as("vp_expose_pubs"),
      lit(1).as("vp_expose_tag"),
      lit(-1).as("is_exclusive"),
      lit(-1).as("bidder_instance_id"),
      lit(-1).as("visibility_profile_id"),
      lit(0).as("truncate_ip"),
      lit(0).as("device_id"),
      lit(0).as("carrier_id"),
      lit(0).as("creative_audit_status"),
      lit(0).as("is_creative_hosted"),
      lit(-1).as("city"),
      lit("").as("latitude"),
      lit("").as("longitude"),
      lit("").as("device_unique_id"),
      lit(0).as("supply_type"),
      lit(0).as("is_toolbar"),
      lit(0).as("deal_id"),
      lit(9223372036854775807L).as("vp_bitmap"),
      lit(null).cast(IntegerType).as("ttl"),
      lit(null).cast(IntegerType).as("view_detection_enabled"),
      lit(null).cast(IntegerType).as("ozone_id"),
      lit(null).cast(IntegerType).as("is_performance"),
      lit(null).cast(StringType).as("sdk_version"),
      lit(null).cast(IntegerType).as("inventory_session_frequency"),
      lit(null).cast(IntegerType).as("bid_price_type"),
      lit(null).cast(IntegerType).as("device_type"),
      lit(null).cast(IntegerType).as("dma"),
      lit(null).cast(StringType).as("postal"),
      lit(null).cast(IntegerType).as("package_id"),
      lit(null).cast(IntegerType).as("spend_protection"),
      lit(null).cast(IntegerType).as("is_secure"),
      lit(null).cast(DoubleType).as("estimated_view_rate"),
      lit(null).cast(StringType).as("external_request_id"),
      lit(null).cast(IntegerType).as("viewdef_definition_id_buyer_member"),
      lit(null).cast(IntegerType).as("spend_protection_pixel_id"),
      lit(null).cast(StringType).as("external_uid"),
      lit(null).cast(StringType).as("request_uuid"),
      lit(null).cast(IntegerType).as("mobile_app_instance_id"),
      lit(null).cast(StringType).as("traffic_source_code"),
      lit(null).cast(StringType).as("stitch_group_id"),
      lit(null).cast(IntegerType).as("deal_type"),
      lit(null).cast(IntegerType).as("ym_floor_id"),
      lit(null).cast(IntegerType).as("ym_bias_id"),
      lit(null).cast(DoubleType).as("estimated_view_rate_over_total"),
      lit(null).cast(IntegerType).as("device_make_id"),
      lit(1).as("operating_system_family_id"),
      lit(null)
        .cast(
          ArrayType(StructType(
                      List(StructField("width",  IntegerType, true),
                           StructField("height", IntegerType, true)
                      )
                    ),
                    true
          )
        )
        .as("tag_sizes"),
      l_null_log_transaction_def.as("seller_transaction_def"),
      l_null_log_transaction_def.as("buyer_transaction_def"),
      lit(null)
        .cast(
          StructType(
            List(
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
      lit(null)
        .cast(StructType(List(StructField("site_url", StringType, true))))
        .as("auction_url"),
      lit(null).cast(ArrayType(IntegerType, true)).as("allowed_media_types"),
      lit(null).cast(BooleanType).as("is_imp_rejecter_applied"),
      lit(1).cast(BooleanType).as("imp_rejecter_do_auction"),
      lit(null)
        .cast(
          StructType(
            List(StructField("latitude",  FloatType, true),
                 StructField("longitude", FloatType, true)
            )
          )
        )
        .as("geo_location"),
      lit(null).cast(DoubleType).as("seller_bid_currency_conversion_rate"),
      lit(null).cast(StringType).as("seller_bid_currency_code"),
      lit(null).cast(BooleanType).as("is_prebid"),
      lit(null).cast(StringType).as("default_referrer_url"),
      lit(null)
        .cast(
          ArrayType(
            StructType(
              List(StructField("engagement_rate_type",    IntegerType, true),
                   StructField("rate",                    DoubleType,  true),
                   StructField("engagement_rate_type_id", IntegerType, true)
              )
            ),
            true
          )
        )
        .as("engagement_rates"),
      col("fx_rate_snapshot_id").cast(IntegerType).as("fx_rate_snapshot_id"),
      lit(null).cast(IntegerType).as("payment_type"),
      lit(null).cast(IntegerType).as("apply_cost_on_default"),
      lit(null).cast(DoubleType).as("media_buy_cost"),
      lit(null).cast(DoubleType).as("media_buy_rev_share_pct"),
      lit(null).cast(IntegerType).as("auction_duration_ms"),
      lit(null).cast(IntegerType).as("expected_events"),
      when(is_not_null(col("anonymized_user_info")).cast(BooleanType),
           col("anonymized_user_info")
      ).as("anonymized_user_info"),
      coalesce(when(col("region_id").cast(IntegerType) =!= lit(0),
                    col("region_id").cast(IntegerType)
               ),
               col("region_id").cast(IntegerType)
      ).as("region_id"),
      lit(null).cast(IntegerType).as("media_company_id"),
      lit(null).cast(StringType).as("gdpr_consent_cookie"),
      lit(null).cast(BooleanType).as("subject_to_gdpr"),
      lit(null).cast(IntegerType).as("browser_code_id"),
      lit(null).cast(IntegerType).as("is_prebid_server_included"),
      lit(null).cast(IntegerType).as("seat_id"),
      lit(null).cast(IntegerType).as("uid_source"),
      lit(null).cast(BooleanType).as("is_whiteops_scanned"),
      lit(null).cast(IntegerType).as("pred_info"),
      lit(null)
        .cast(
          ArrayType(StructType(
                      List(StructField("graph_id", IntegerType, true),
                           StructField("group_id", LongType,    true)
                      )
                    ),
                    true
          )
        )
        .as("crossdevice_groups"),
      lit(null).cast(BooleanType).as("is_amp"),
      lit(null).cast(IntegerType).as("hb_source"),
      lit(null).cast(StringType).as("external_campaign_id"),
      lit(null)
        .cast(
          StructType(
            List(
              StructField("product_feed_id",            IntegerType, true),
              StructField("item_selection_strategy_id", IntegerType, true),
              StructField("product_uuid",               StringType,  true)
            )
          )
        )
        .as("log_product_ads"),
      lit(null).cast(BooleanType).as("ss_native_assembly_enabled"),
      lit(null).cast(DoubleType).as("emp"),
      lit(null)
        .cast(
          ArrayType(StructType(
                      List(StructField("identity_type",  IntegerType, true),
                           StructField("identity_value", StringType,  true)
                      )
                    ),
                    true
          )
        )
        .as("personal_identifiers"),
      lit(null)
        .cast(
          ArrayType(StructType(
                      List(StructField("identity_type",  IntegerType, true),
                           StructField("identity_value", StringType,  true)
                      )
                    ),
                    true
          )
        )
        .as("personal_identifiers_experimental"),
      lit(null).cast(IntegerType).as("postal_code_ext_id"),
      col("hashed_ip").cast(StringType).as("hashed_ip"),
      lit(null).cast(StringType).as("external_deal_code"),
      lit(null).cast(IntegerType).as("creative_duration"),
      lit(null).cast(StringType).as("openrtb_req_subdomain"),
      lit(null).cast(IntegerType).as("creative_media_subtype_id"),
      lit(null).cast(BooleanType).as("is_private_auction"),
      lit(null).cast(BooleanType).as("private_auction_eligible"),
      lit(null).cast(StringType).as("client_request_id"),
      lit(null).cast(IntegerType).as("chrome_traffic_label")
    )
  }

  def f_is_buy_side(
    in_log_dw_bid:   Column,
    buyer_member_id: Column,
    member_id:       Column
  ): Column = {
    var l_is_buy_side: org.apache.spark.sql.Column = lit(0)
    l_is_buy_side =
      when(is_not_null(in_log_dw_bid).and(buyer_member_id === member_id),
           lit(1)
      ).otherwise(l_is_buy_side)
    l_is_buy_side
  }

  def f_is_error_imp(imp_type: Column): Column = {
    var l_is_error_imp: org.apache.spark.sql.Column = lit(0)
    l_is_error_imp = when((coalesce(imp_type, lit(0)) === lit(1))
                            .or(coalesce(imp_type, lit(0)) === lit(3))
                            .or(coalesce(imp_type, lit(0)) === lit(8)),
                          lit(1)
    ).otherwise(l_is_error_imp).cast(IntegerType)
    l_is_error_imp
  }

  def f_is_non_cpm_payment_or_payment(
    imp_type:     Column,
    payment_type: Column,
    revenue_type: Column
  ): Column = {
    var l_is_non_cpm_payment_or_payment: org.apache.spark.sql.Column = lit(0)
    l_is_non_cpm_payment_or_payment = when(
      (coalesce(imp_type, lit(0)) === lit(7)).and(
        (coalesce(payment_type, lit(999)) === lit(1))
          .or(coalesce(payment_type, lit(999)) === lit(2))
          .or(coalesce(payment_type, lit(999)) === lit(5))
      ),
      lit(1)
    ).when(
        (coalesce(imp_type, lit(0)) === lit(6)).and(
          (coalesce(revenue_type, lit(999)) === lit(3))
            .or(coalesce(revenue_type, lit(999)) === lit(4))
            .or(coalesce(revenue_type, lit(999)) === lit(9))
        ),
        lit(1)
      )
      .otherwise(l_is_non_cpm_payment_or_payment)
      .cast(IntegerType)
    l_is_non_cpm_payment_or_payment
  }

  def f_transaction_event_type_id(
    impression_transaction_def: Column,
    preempt_transaction_def:    Column
  ): Column = {
    var l_transaction_event_type_id: org.apache.spark.sql.Column = lit(1)
    l_transaction_event_type_id = when(
      is_not_null(preempt_transaction_def).cast(BooleanType),
      coalesce(preempt_transaction_def.getField("transaction_event_type_id"),
               lit(0)
      )
    ).when(is_not_null(impression_transaction_def).cast(BooleanType),
            coalesce(
              impression_transaction_def.getField("transaction_event_type_id"),
              lit(0)
            )
      )
      .otherwise(l_transaction_event_type_id)
      .cast(IntegerType)
    l_transaction_event_type_id
  }

  def f_get_media_cost_dollars_cpm(
    imp_type:                Column,
    payment_type:            Column,
    revenue_type:            Column,
    media_buy_cost:          Column,
    media_buy_rev_share_pct: Column,
    seller_revenue_cpm:      Column,
    buyer_spend_cpm:         Column,
    booked_revenue_dollars:  Column,
    commission_cpm:          Column,
    commission_revshare:     Column,
    serving_fees_cpm:        Column,
    serving_fees_revshare:   Column,
    apply_cost_on_default:   Column,
    revenue_value:           Column
  ): Column = {
    var l_payment_type:                org.apache.spark.sql.Column = lit(0)
    var l_revenue_type:                org.apache.spark.sql.Column = lit(0)
    var l_media_buy_cost:              org.apache.spark.sql.Column = lit(0)
    var l_media_buy_rev_share_pct:     org.apache.spark.sql.Column = lit(0)
    var l_seller_revenue_cpm:          org.apache.spark.sql.Column = lit(0)
    var l_buyer_spend_cpm:             org.apache.spark.sql.Column = lit(0)
    var l_booked_revenue_dollars:      org.apache.spark.sql.Column = lit(0)
    var l_commission_cpm:              org.apache.spark.sql.Column = lit(0)
    var l_commission_revshare:         org.apache.spark.sql.Column = lit(0)
    var l_serving_fees_cpm:            org.apache.spark.sql.Column = lit(0)
    var l_serving_fees_revshare:       org.apache.spark.sql.Column = lit(0)
    var l_apply_cost_on_default:       org.apache.spark.sql.Column = lit(0)
    var l_revenue_dollars_cpm:         org.apache.spark.sql.Column = lit(0)
    var l_network_revenue_dollars_cpm: org.apache.spark.sql.Column = lit(0)
    var l_media_cost_dollars_cpm:      org.apache.spark.sql.Column = lit(0)
    var l_revenue_value:               org.apache.spark.sql.Column = lit(0)
    l_payment_type = coalesce(payment_type,     lit(0)).cast(IntegerType)
    l_revenue_type = coalesce(revenue_type,     lit(0)).cast(IntegerType)
    l_media_buy_cost = coalesce(media_buy_cost, lit(0)).cast(DoubleType)
    l_media_buy_rev_share_pct =
      coalesce(media_buy_rev_share_pct,                 lit(0)).cast(DoubleType)
    l_seller_revenue_cpm = coalesce(seller_revenue_cpm, lit(0)).cast(DoubleType)
    l_buyer_spend_cpm = coalesce(buyer_spend_cpm,       lit(0)).cast(DoubleType)
    l_booked_revenue_dollars =
      coalesce(booked_revenue_dollars,          lit(0)).cast(DoubleType)
    l_commission_cpm = coalesce(commission_cpm, lit(0)).cast(DoubleType)
    l_commission_revshare =
      coalesce(commission_revshare,                 lit(0)).cast(DoubleType)
    l_serving_fees_cpm = coalesce(serving_fees_cpm, lit(0)).cast(DoubleType)
    l_serving_fees_revshare =
      coalesce(serving_fees_revshare,         lit(0)).cast(DoubleType)
    l_revenue_value = coalesce(revenue_value, lit(0)).cast(DoubleType)
    l_media_cost_dollars_cpm = when(coalesce(imp_type, lit(0)) === lit(2),
                                    when(l_media_buy_rev_share_pct === lit(0),
                                         l_media_buy_cost
                                    ).otherwise(l_media_cost_dollars_cpm)
    ).when(
        coalesce(imp_type, lit(0)) === lit(4),
        when((l_apply_cost_on_default === lit(1)).and(
               l_media_buy_rev_share_pct <= lit(0)
             ),
             l_media_buy_cost
        ).otherwise(l_media_cost_dollars_cpm)
      )
      .when(
        coalesce(imp_type, lit(0)) === lit(5),
        when(
          l_media_buy_rev_share_pct > lit(0),
          when(l_revenue_type.isin(lit(1), lit(2)),
               l_media_buy_cost * l_media_buy_rev_share_pct
          ).otherwise {
            l_media_cost_dollars_cpm =
              ((l_media_buy_rev_share_pct * (l_booked_revenue_dollars * lit(
                1000
              ) - l_commission_cpm - l_commission_revshare * (l_booked_revenue_dollars * lit(
                1000
              ))) - l_serving_fees_cpm) / (lit(1) + l_serving_fees_revshare))
                .cast(DoubleType)
            l_media_cost_dollars_cpm = when(
              (l_media_buy_rev_share_pct * (l_booked_revenue_dollars * lit(
                1000
              ) - l_commission_cpm - l_commission_revshare * (l_booked_revenue_dollars * lit(
                1000
              ))) - l_serving_fees_cpm) / (lit(
                1
              ) + l_serving_fees_revshare) < lit(0),
              lit(0)
            ).otherwise(l_media_cost_dollars_cpm).cast(DoubleType)
            l_media_cost_dollars_cpm
          }
        ).otherwise(l_media_buy_cost)
      )
      .when(
        coalesce(imp_type, lit(0)) === lit(6),
        when(
          (l_payment_type === lit(4)).and(l_revenue_type.isin(lit(3), lit(4))),
          lit(0)
        ).when(l_media_buy_rev_share_pct > lit(0),
                l_seller_revenue_cpm * l_media_buy_rev_share_pct
          )
          .otherwise(l_media_buy_cost)
      )
      .when(coalesce(imp_type, lit(0)) === lit(7),
            when(not(l_payment_type.isin(lit(2), lit(-1), lit(1))),
                 l_buyer_spend_cpm
            ).otherwise(l_media_cost_dollars_cpm)
      )
      .when(
        coalesce(imp_type, lit(0)) === lit(9),
        when(
          l_media_buy_rev_share_pct > lit(0), {
            l_media_cost_dollars_cpm =
              (l_media_buy_rev_share_pct * (l_booked_revenue_dollars - l_commission_cpm / lit(
                1000
              ) - l_commission_revshare * l_booked_revenue_dollars))
                .cast(DoubleType)
            l_media_cost_dollars_cpm = when(
              l_media_buy_rev_share_pct * (l_booked_revenue_dollars - l_commission_cpm / lit(
                1000
              ) - l_commission_revshare * l_booked_revenue_dollars) < lit(0),
              lit(0)
            ).otherwise(l_media_cost_dollars_cpm).cast(DoubleType)
            l_media_cost_dollars_cpm
          }
        ).otherwise(l_media_buy_cost)
      )
      .when(
        coalesce(imp_type, lit(0)) === lit(10),
        when(
          l_media_buy_rev_share_pct > lit(0),
          lit(
            1000
          ) * (l_media_buy_rev_share_pct * (l_revenue_value - l_commission_revshare * l_revenue_value))
        ).otherwise(lit(1000) * l_media_buy_cost)
      )
      .otherwise(lit(0))
      .cast(DoubleType)
    l_media_cost_dollars_cpm
  }

  def f_zero_pricing_term_amount_if_non_cpm(
    imp_type:         Column,
    payment_type:     Column,
    revenue_type:     Column,
    in_pricing_terms: Column
  ): Column =
    when(
      is_not_null(in_pricing_terms).cast(BooleanType),
      when(
        is_not_null(imp_type)
          .and(is_not_null(payment_type))
          .and(is_not_null(revenue_type))
          .and(
            (imp_type === lit(6))
              .and(revenue_type.isin(lit(3),                          lit(4)))
              .or((imp_type === lit(7)).and(payment_type.isin(lit(1), lit(2))))
          ),
        temp6618434_UDF(in_pricing_terms, in_pricing_terms, lit(0)).getField(
          "l_pricing_terms"
        )
      ).otherwise(in_pricing_terms)
    ).otherwise(
      lit(null).cast(
        ArrayType(
          StructType(
            List(
              StructField("term_id",                 IntegerType, true),
              StructField("amount",                  DoubleType,  true),
              StructField("rate",                    DoubleType,  true),
              StructField("is_deduction",            BooleanType, true),
              StructField("is_media_cost_dependent", BooleanType, true),
              StructField("data_member_id",          IntegerType, true)
            )
          ),
          true
        )
      )
    )

  def f_find_personal_identifier(id_type: Column, id_list: Column): Column =
    when(
      is_not_null(
        when(is_not_null(id_list).cast(BooleanType), id_list).otherwise(
          lit(null).cast(
            ArrayType(StructType(
                        List(StructField("identity_type",  IntegerType, true),
                             StructField("identity_value", StringType,  true)
                        )
                      ),
                      true
            )
          )
        )
      ).cast(BooleanType),
      temp7361735_UDF(
        id_type,
        when(is_not_null(id_list).cast(BooleanType), id_list).otherwise(
          lit(null).cast(
            ArrayType(StructType(
                        List(StructField("identity_type",  IntegerType, true),
                             StructField("identity_value", StringType,  true)
                        )
                      ),
                      true
            )
          )
        ),
        lit(0),
        lit(0)
      ).getField("pi")
    ).otherwise(
      lit(null).cast(
        StructType(
          List(StructField("identity_type",  IntegerType, true),
               StructField("identity_value", StringType,  true)
          )
        )
      )
    )

  def f_string_to_anon_user_info(in1: Column): Column = {
    var retval: org.apache.spark.sql.Column = struct(lit(null).as("user_id"))
    retval = struct(
      when(is_not_null(in1).cast(BooleanType),
           raw_data_concat(lit("").cast(BinaryType), murmur(in1))
      ).otherwise(lit(null).cast(BinaryType)).as("user_id")
    )
    retval
  }

  def f_is_default_or_error_imp(imp_type: Column): Column = {
    var l_is_default_or_error_imp: org.apache.spark.sql.Column = lit(0)
    l_is_default_or_error_imp =
      when((coalesce(imp_type, lit(0)) === lit(4))
             .or(f_is_error_imp(coalesce(imp_type, lit(0))) === lit(1)),
           lit(1)
      ).otherwise(l_is_default_or_error_imp).cast(IntegerType)
    l_is_default_or_error_imp
  }

  def f_update_data_costs(
    data_costs:                Column,
    imp_type:                  Column,
    agg_type:                  Column,
    payment_type:              Column,
    media_cost_cpm:            Column,
    member_sales_tax_rate_pct: Column
  ): Column =
    when(
      is_not_null(data_costs).cast(BooleanType),
      when(
        (coalesce(imp_type, lit(1)).cast(IntegerType) === lit(5))
          .or(coalesce(imp_type, lit(1)).cast(IntegerType) === lit(7)),
        array_union(
          data_costs,
          temp10951013_UDF(
            array(),
            coalesce(payment_type,              lit(0)).cast(IntegerType),
            coalesce(member_sales_tax_rate_pct, lit(0)).cast(DoubleType),
            lit(0),
            coalesce(media_cost_cpm, lit(0)).cast(DoubleType),
            coalesce(agg_type,       lit(0)).cast(IntegerType),
            lit(null).cast(
              StructType(
                List(
                  StructField("data_member_id", IntegerType, true),
                  StructField("cost",           DoubleType,  true),
                  StructField("used_segments",
                              ArrayType(IntegerType, true),
                              true
                  ),
                  StructField("cost_pct", DoubleType, true)
                )
              )
            ),
            data_costs
          ).getField("l_data_costs")
        )
      )
    )

  def f_create_agg_dw_impressions_return_11108229(
    curator_member_id:               Column,
    discrepancy_allowance:           Column,
    should_process_views:            Column,
    buyer_member_id:                 Column,
    imps_for_budget_caps_pacing:     Column,
    imp_type:                        Column,
    is_curated:                      Column,
    view_detection_enabled:          Column,
    buyer_trx_event_type_id:         Column,
    commission_revshare:             Column,
    creative_overage_fees:           Column,
    viewdef_definition_id:           Column,
    virtual_log_dw_bid:              Column,
    has_buyer_transacted:            Column,
    payment_type_normalized:         Column,
    seller_trx_event_id:             Column,
    seller_revenue_cpm:              Column,
    insertion_order_id:              Column,
    revenue_type_normalized:         Column,
    billing_period_id:               Column,
    viewdef_viewable:                Column,
    split_id:                        Column,
    data_costs_deal:                 Column,
    seller_charges_pricing_terms:    Column,
    f_preempt_over_impression_94298: Column,
    commission_cpm:                  Column,
    v_transaction_event_pricing:     Column,
    buyer_trx_event_id:              Column,
    campaign_group_id:               Column,
    has_seller_transacted:           Column,
    buyer_charges_pricing_terms:     Column,
    revenue_value:                   Column,
    campaign_group_type_id:          Column,
    should_zero_seller_revenue:      Column,
    view_measurable:                 Column,
    advertiser_id:                   Column,
    media_cost_dollars_cpm:          Column,
    creative_id:                     Column,
    f_preempt_over_impression_95337: Column,
    flight_id:                       Column,
    viewable:                        Column,
    auction_service_deduction:       Column,
    media_buy_cost:                  Column,
    seller_deduction:                Column,
    buyer_spend_cpm:                 Column,
    seller_trx_event_type_id:        Column,
    auction_service_fees:            Column,
    serving_fees_cpm:                Column,
    data_costs:                      Column,
    sup_ip_range_lookup_count:       Column,
    ttl:                             Column,
    view_result:                     Column,
    is_dw_normalized:                Column,
    booked_revenue_adv_curr:         Column,
    booked_revenue_dollars:          Column,
    deal_id:                         Column,
    _f_is_buy_side:                  Column,
    deal_type:                       Column,
    campaign_id:                     Column,
    is_placeholder_bid:              Column,
    serving_fees_revshare:           Column,
    view_non_measurable_reason:      Column
  ): Column =
    struct(
      coalesce(
        col("log_dw_view.date_time").cast(LongType),
        when((ttl > lit(3600)).and(
               is_not_null(col("log_impbus_preempt.date_time").cast(LongType))
             ),
             col("log_impbus_preempt.date_time").cast(LongType)
        ),
        col("date_time").cast(LongType)
      ).as("date_time"),
      col("auction_id_64").cast(LongType).as("auction_id_64"),
      coalesce(
        when(col("log_dw_view.user_id_64").cast(LongType) =!= lit(0),
             col("log_dw_view.user_id_64").cast(LongType)
        ),
        when(col("log_impbus_impressions.user_id_64").cast(LongType) =!= lit(0),
             col("log_impbus_impressions.user_id_64").cast(LongType)
        )
      ).as("user_id_64"),
      when(col("log_impbus_impressions.tag_id").cast(IntegerType) =!= lit(0),
           col("log_impbus_impressions.tag_id").cast(IntegerType)
      ).as("tag_id"),
      when(col("log_impbus_impressions.venue_id").cast(IntegerType) =!= lit(0),
           col("log_impbus_impressions.venue_id").cast(IntegerType)
      ).as("venue_id"),
      when(col("log_impbus_impressions.inventory_source_id").cast(
             IntegerType
           ) =!= lit(0),
           col("log_impbus_impressions.inventory_source_id").cast(IntegerType)
      ).as("inventory_source_id"),
      lit(null).as("session_frequency"),
      when(string_compare(col("log_impbus_impressions.site_domain"),
                          lit("---")
           ) =!= lit(0),
           col("log_impbus_impressions.site_domain")
      ).as("site_domain"),
      f_preempt_over_impression_non_zero_explicit(
        is_not_null(col("log_impbus_preempt")),
        col("log_impbus_impressions.width").cast(IntegerType),
        col("log_impbus_preempt.width").cast(IntegerType)
      ).as("width"),
      f_preempt_over_impression_non_zero_explicit(
        is_not_null(col("log_impbus_preempt")),
        col("log_impbus_impressions.height").cast(IntegerType),
        col("log_impbus_preempt.height").cast(IntegerType)
      ).as("height"),
      when(string_compare(col("log_impbus_impressions.geo_country"),
                          lit("--")
           ) =!= lit(0),
           col("log_impbus_impressions.geo_country")
      ).as("geo_country"),
      when(string_compare(col("log_impbus_impressions.geo_region"),
                          lit("--")
           ) =!= lit(0),
           col("log_impbus_impressions.geo_region")
      ).as("geo_region"),
      when(string_compare(col("log_impbus_impressions.gender"),
                          lit("u")
           ) =!= lit(0),
           col("log_impbus_impressions.gender")
      ).as("gender"),
      when(col("log_impbus_impressions.age").cast(IntegerType) =!= lit(0),
           col("log_impbus_impressions.age").cast(IntegerType)
      ).as("age"),
      col("seller_member_id").cast(IntegerType).as("seller_member_id"),
      when(col("imp_type").cast(IntegerType) =!= lit(1), buyer_member_id)
        .otherwise(lit(0))
        .as("buyer_member_id"),
      creative_id.as("creative_id"),
      when(string_compare(col("log_impbus_impressions.seller_currency"),
                          lit("")
           ) =!= lit(0),
           col("log_impbus_impressions.seller_currency")
      ).as("seller_currency"),
      when(string_compare(col("log_impbus_impressions.buyer_currency"),
                          lit("")
           ) =!= lit(0),
           col("log_impbus_impressions.buyer_currency")
      ).as("buyer_currency"),
      f_preempt_over_impression_non_zero_explicit(
        is_not_null(col("log_impbus_preempt")),
        col("log_impbus_impressions.buyer_bid"),
        col("log_impbus_preempt.buyer_bid")
      ).as("buyer_bid"),
      coalesce(when(imp_type.isin(lit(8), lit(1), lit(3)), lit(0)),
               when(imp_type === lit(5), col("log_dw_bid.price")),
               buyer_spend_cpm
      ).as("buyer_spend"),
      when(col("log_impbus_impressions.ecp") =!= lit(0),
           col("log_impbus_impressions.ecp")
      ).as("ecp"),
      when(col("log_impbus_impressions.reserve_price") =!= lit(0.0d),
           col("log_impbus_impressions.reserve_price")
      ).as("reserve_price"),
      coalesce(when((col("imp_type").cast(IntegerType) === lit(1))
                      .or(buyer_member_id === lit(0))
                      .and(is_not_null(advertiser_id)),
                    lit(0)
               ),
               advertiser_id
      ).as("advertiser_id"),
      coalesce(when((col("imp_type").cast(IntegerType) === lit(1))
                      .or(buyer_member_id === lit(0))
                      .and(is_not_null(campaign_group_id)),
                    lit(0)
               ),
               campaign_group_id
      ).as("campaign_group_id"),
      coalesce(
        when((col("imp_type").cast(IntegerType) === lit(1))
               .or(buyer_member_id === lit(0))
               .and(is_not_null(campaign_id)),
             lit(0)
        ),
        when((imp_type =!= lit(6)).and(campaign_id =!= lit(0)), campaign_id)
      ).as("campaign_id"),
      coalesce(
        when((_f_is_buy_side === lit(1)).and(
               col("log_dw_bid.creative_freq").cast(IntegerType) =!= lit(0)
             ),
             col("log_dw_bid.creative_freq").cast(IntegerType)
        ),
        when(_f_is_buy_side === lit(0),
             virtual_log_dw_bid.getField("creative_freq")
        )
      ).as("creative_freq"),
      coalesce(
        when((_f_is_buy_side === lit(1)).and(
               col("log_dw_bid.creative_rec").cast(IntegerType) =!= lit(0)
             ),
             col("log_dw_bid.creative_rec").cast(IntegerType)
        ),
        when(_f_is_buy_side === lit(0),
             virtual_log_dw_bid.getField("creative_rec")
        )
      ).as("creative_rec"),
      coalesce(
        when((_f_is_buy_side === lit(1)).and(
               col("log_dw_bid.predict_type").cast(IntegerType) === lit(1)
             ),
             lit(1)
        ),
        when(
          (_f_is_buy_side === lit(1)).and(
            (col("log_dw_bid.predict_type").cast(IntegerType) === lit(0)).or(
              (col("log_dw_bid.predict_type").cast(IntegerType) >= lit(2)).and(
                col("log_dw_bid.predict_type").cast(IntegerType) =!= lit(9)
              )
            )
          ),
          lit(2)
        ),
        lit(0)
      ).as("is_learn"),
      when((_f_is_buy_side === lit(1)).and(
             col("log_dw_bid.is_remarketing").cast(IntegerType) =!= lit(0)
           ),
           col("log_dw_bid.is_remarketing").cast(IntegerType)
      ).as("is_remarketing"),
      coalesce(
        when((_f_is_buy_side === lit(1)).and(
               col("log_dw_bid.advertiser_freq").cast(IntegerType) =!= lit(0)
             ),
             col("log_dw_bid.advertiser_freq").cast(IntegerType)
        ),
        when((_f_is_buy_side === lit(1)).and(imp_type === lit(9)), lit(0)),
        when(_f_is_buy_side === lit(0),
             virtual_log_dw_bid.getField("advertiser_freq")
        )
      ).as("advertiser_frequency"),
      coalesce(
        when((_f_is_buy_side === lit(1)).and(
               col("log_dw_bid.advertiser_rec").cast(IntegerType) =!= lit(0)
             ),
             col("log_dw_bid.advertiser_rec").cast(IntegerType)
        ),
        when((_f_is_buy_side === lit(1)).and(imp_type === lit(9)), lit(0)),
        when(_f_is_buy_side === lit(0),
             virtual_log_dw_bid.getField("advertiser_rec")
        )
      ).as("advertiser_recency"),
      coalesce(
        when(col("log_impbus_impressions.user_id_64").cast(LongType) > lit(
               9223372036854775807L
             ).cast(LongType),
             lit(-1)
        ),
        when(
          (is_placeholder_bid === lit(1))
            .or(col("imp_type").cast(IntegerType) === lit(1))
            .or(col("imp_type").cast(IntegerType) === lit(3))
            .or(imp_type === lit(8))
            .and(
              is_not_null(
                col("log_impbus_impressions.user_id_64").cast(LongType)
              )
            ),
          col("log_impbus_impressions.user_id_64").cast(LongType) % lit(1000)
        ),
        when(
          (virtual_log_dw_bid.getField("log_type") === lit(2)).or(
            is_not_null(col("log_dw_bid.log_type").cast(IntegerType))
              .and(col("log_dw_bid.log_type").cast(IntegerType) === lit(2))
          ),
          col("log_impbus_impressions.user_group_id").cast(IntegerType)
        ),
        col("log_dw_bid.user_group_id").cast(IntegerType)
      ).as("user_group_id"),
      lit(null).as("camp_dp_id"),
      coalesce(when(imp_type.isin(lit(8), lit(1), lit(3)), lit(0)),
               col("publisher_id").cast(IntegerType)
      ).as("media_buy_id"),
      media_buy_cost.as("media_buy_cost"),
      f_preempt_over_impression_non_zero_explicit(
        is_not_null(col("log_impbus_preempt")),
        col("log_impbus_impressions.brand_id").cast(IntegerType),
        col("log_impbus_preempt.brand_id").cast(IntegerType)
      ).as("brand_id"),
      f_preempt_over_impression_non_zero_explicit(
        is_not_null(col("log_impbus_preempt")),
        col("log_impbus_impressions.cleared_direct").cast(IntegerType),
        col("log_impbus_preempt.cleared_direct").cast(IntegerType)
      ).as("cleared_direct"),
      lit(null).as("clear_fees"),
      when(imp_type =!= lit(9),
           col("log_impbus_impressions.media_buy_rev_share_pct")
      ).otherwise(col("log_dw_bid.media_buy_rev_share_pct"))
        .as("media_buy_rev_share_pct"),
      revenue_value.as("revenue_value"),
      coalesce(
        when(
          (_f_is_buy_side === lit(1)).and(
            string_compare(col("log_dw_bid.pricing_type"), lit("--")) =!= lit(0)
          ),
          col("log_dw_bid.pricing_type")
        ),
        when(_f_is_buy_side === lit(0),
             virtual_log_dw_bid.getField("pricing_type")
        )
      ).as("pricing_type"),
      when((_f_is_buy_side === lit(1))
             .and(col("log_dw_bid.can_convert").cast(IntegerType) =!= lit(0)),
           col("log_dw_bid.can_convert").cast(IntegerType)
      ).as("can_convert"),
      coalesce(
        when(
          (is_placeholder_bid === lit(1))
            .or(col("imp_type").cast(IntegerType) === lit(1))
            .or(col("imp_type").cast(IntegerType) === lit(3))
            .or(imp_type === lit(8)),
          lit(0)
        ),
        col("log_impbus_impressions.pub_rule_id").cast(IntegerType)
      ).as("pub_rule_id"),
      coalesce(
        when((_f_is_buy_side === lit(1))
               .and(col("log_dw_bid.is_control").cast(IntegerType) =!= lit(0)),
             col("log_dw_bid.is_control").cast(IntegerType)
        ),
        when((_f_is_buy_side === lit(0))
               .and(virtual_log_dw_bid.getField("is_control") =!= lit(0)),
             virtual_log_dw_bid.getField("is_control")
        )
      ).as("is_control"),
      when(col("log_dw_bid.control_pct") =!= lit(0),
           col("log_dw_bid.control_pct")
      ).as("control_pct"),
      when(col("log_dw_bid.control_creative_id").cast(IntegerType) =!= lit(0),
           col("log_dw_bid.control_creative_id").cast(IntegerType)
      ).as("control_creative_id"),
      lit(null).as("predicted_cpm"),
      coalesce(
        when(
          (_f_is_buy_side === lit(1)).and(col("log_dw_bid.price") > lit(999)),
          lit(999)
        ),
        when(
          (_f_is_buy_side === lit(1)).and(col("log_dw_bid.price") <= lit(999)),
          col("log_dw_bid.price")
        ),
        lit(0)
      ).as("actual_bid"),
      when(col("log_impbus_impressions.site_id").cast(IntegerType) =!= lit(0),
           col("log_impbus_impressions.site_id").cast(IntegerType)
      ).as("site_id"),
      when(col("log_impbus_impressions.content_category_id").cast(
             IntegerType
           ) =!= lit(0),
           col("log_impbus_impressions.content_category_id").cast(IntegerType)
      ).as("content_category_id"),
      coalesce(
        when((imp_type === lit(6))
               .and(is_dw_normalized === lit(1))
               .and(should_process_views === lit(1)),
             lit(0)
        ),
        when((imp_type === lit(7))
               .and(is_dw_normalized === lit(1))
               .and(should_process_views === lit(1)),
             auction_service_fees
        ),
        when((imp_type === lit(7))
               .and(is_dw_normalized === lit(0))
               .and(should_process_views === lit(1)),
             auction_service_fees
        ),
        when(f_is_non_cpm_payment_or_payment(imp_type,
                                             payment_type_normalized,
                                             revenue_type_normalized
             ) === lit(1),
             lit(0)
        ).otherwise(auction_service_fees)
      ).as("auction_service_fees"),
      coalesce(
        when((imp_type === lit(6))
               .and(is_dw_normalized === lit(1))
               .and(should_process_views === lit(1)),
             lit(0)
        ),
        when((imp_type === lit(7))
               .and(is_dw_normalized === lit(1))
               .and(should_process_views === lit(1)),
             discrepancy_allowance
        ),
        when((imp_type === lit(7))
               .and(is_dw_normalized === lit(0))
               .and(should_process_views === lit(1)),
             discrepancy_allowance
        ),
        when(f_is_non_cpm_payment_or_payment(imp_type,
                                             payment_type_normalized,
                                             revenue_type_normalized
             ) === lit(1),
             lit(0)
        ).otherwise(discrepancy_allowance)
      ).as("discrepancy_allowance"),
      lit(null).as("forex_allowance"),
      coalesce(
        when((imp_type === lit(6))
               .and(is_dw_normalized === lit(1))
               .and(should_process_views === lit(1)),
             lit(0)
        ),
        when((imp_type === lit(7))
               .and(is_dw_normalized === lit(1))
               .and(should_process_views === lit(1)),
             creative_overage_fees
        ),
        when((imp_type === lit(7))
               .and(is_dw_normalized === lit(0))
               .and(should_process_views === lit(1)),
             creative_overage_fees
        ),
        when(f_is_non_cpm_payment_or_payment(imp_type,
                                             payment_type_normalized,
                                             revenue_type_normalized
             ) === lit(1),
             lit(0)
        ).otherwise(creative_overage_fees)
      ).as("creative_overage_fees"),
      coalesce(col("log_impbus_impressions.fold_position").cast(IntegerType),
               lit(0)
      ).as("fold_position"),
      when(col("log_impbus_impressions.external_inv_id").cast(
             IntegerType
           ) =!= lit(0),
           col("log_impbus_impressions.external_inv_id").cast(IntegerType)
      ).as("external_inv_id"),
      coalesce(
        when((_f_is_buy_side === lit(1))
               .and(col("log_dw_bid.cadence_modifier") =!= lit(0)),
             col("log_dw_bid.cadence_modifier")
        ),
        when((_f_is_buy_side === lit(0))
               .and(virtual_log_dw_bid.getField("cadence_modifier") =!= lit(0)),
             virtual_log_dw_bid.getField("cadence_modifier")
        )
      ).as("cadence_modifier"),
      col("imp_type").cast(IntegerType).as("imp_type"),
      coalesce(
        when(_f_is_buy_side === lit(1), col("log_dw_bid.advertiser_currency")),
        lit("USD")
      ).as("advertiser_currency"),
      coalesce(
        when(
          (_f_is_buy_side === lit(1))
            .and(is_not_null(col("log_dw_bid.advertiser_exchange_rate")))
            .and(col("log_dw_bid.advertiser_exchange_rate") =!= lit(1.0d))
            .and(col("log_dw_bid.advertiser_exchange_rate") =!= lit(0.0d)),
          col("log_dw_bid.advertiser_exchange_rate")
        ),
        lit(1.0d)
      ).as("advertiser_exchange_rate"),
      col("log_impbus_impressions.ip_address").as("ip_address"),
      when(
        col("log_impbus_impressions.publisher_id").cast(IntegerType) =!= lit(0),
        col("log_impbus_impressions.publisher_id").cast(IntegerType)
      ).as("publisher_id"),
      coalesce(
        when((imp_type === lit(6))
               .and(is_dw_normalized === lit(1))
               .and(should_process_views === lit(1)),
             lit(0)
        ),
        when((imp_type === lit(7))
               .and(is_dw_normalized === lit(1))
               .and(should_process_views === lit(1)),
             auction_service_deduction
        ),
        when((imp_type === lit(7))
               .and(is_dw_normalized === lit(0))
               .and(should_process_views === lit(1)),
             auction_service_deduction
        ),
        when(f_is_non_cpm_payment_or_payment(imp_type,
                                             payment_type_normalized,
                                             revenue_type_normalized
             ) === lit(1),
             lit(0)
        ).otherwise(auction_service_deduction)
      ).as("auction_service_deduction"),
      coalesce(
        when((col("imp_type").cast(IntegerType) === lit(1))
               .or(buyer_member_id === lit(0))
               .and(is_not_null(insertion_order_id)),
             lit(0)
        ),
        when(insertion_order_id =!= lit(0), insertion_order_id)
      ).as("insertion_order_id"),
      coalesce(
        when(_f_is_buy_side === lit(1),
             col("log_dw_bid.predict_type").cast(IntegerType)
        ),
        when(_f_is_buy_side === lit(0),
             virtual_log_dw_bid.getField("predict_type")
        )
      ).as("predict_type_rev"),
      coalesce(
        when((_f_is_buy_side === lit(1)).and(
               col("log_dw_bid.predict_type_goal").cast(IntegerType) =!= lit(0)
             ),
             col("log_dw_bid.predict_type_goal").cast(IntegerType)
        ),
        when(_f_is_buy_side === lit(0),
             virtual_log_dw_bid.getField("predict_type_goal")
        )
      ).as("predict_type_goal"),
      coalesce(
        when(_f_is_buy_side === lit(1),
             col("log_dw_bid.predict_type").cast(IntegerType)
        ),
        when(_f_is_buy_side === lit(0),
             virtual_log_dw_bid.getField("predict_type")
        )
      ).as("predict_type_cost"),
      booked_revenue_dollars.as("booked_revenue_dollars"),
      booked_revenue_adv_curr.as("booked_revenue_adv_curr"),
      when(commission_cpm =!= lit(0),      commission_cpm).as("commission_cpm"),
      when(commission_revshare =!= lit(0), commission_revshare)
        .as("commission_revshare"),
      when(serving_fees_cpm =!= lit(0), serving_fees_cpm)
        .as("serving_fees_cpm"),
      when(serving_fees_revshare =!= lit(0), serving_fees_revshare)
        .as("serving_fees_revshare"),
      when(col("log_impbus_impressions.user_tz_offset").cast(
             IntegerType
           ) =!= lit(0),
           col("log_impbus_impressions.user_tz_offset").cast(IntegerType)
      ).as("user_tz_offset"),
      f_preempt_over_impression_non_zero_explicit(
        is_not_null(col("log_impbus_preempt.media_type").cast(IntegerType)),
        col("log_impbus_impressions.media_type").cast(IntegerType),
        col("log_impbus_preempt.media_type").cast(IntegerType)
      ).as("media_type"),
      when(col("log_impbus_impressions.operating_system").cast(
             IntegerType
           ) =!= lit(0),
           col("log_impbus_impressions.operating_system").cast(IntegerType)
      ).as("operating_system"),
      when(col("log_impbus_impressions.browser").cast(IntegerType) =!= lit(0),
           col("log_impbus_impressions.browser").cast(IntegerType)
      ).as("browser"),
      when(col("log_impbus_impressions.language").cast(IntegerType) =!= lit(0),
           col("log_impbus_impressions.language").cast(IntegerType)
      ).as("language"),
      col("log_impbus_impressions.seller_currency").as("publisher_currency"),
      coalesce(
        when(
          is_not_null(col("log_impbus_impressions.seller_exchange_rate")).and(
            col("log_impbus_impressions.seller_exchange_rate") =!= lit(1.0d)
          ),
          col("log_impbus_impressions.seller_exchange_rate")
        ),
        lit(1.0d)
      ).as("publisher_exchange_rate"),
      media_cost_dollars_cpm.as("media_cost_dollars_cpm"),
      coalesce(
        when(
          (is_placeholder_bid === lit(1)).or(
            (col("imp_type").cast(IntegerType) === lit(1))
              .or(col("imp_type").cast(IntegerType) === lit(3))
              .or(imp_type === lit(8))
          ),
          lit(0)
        ),
        payment_type_normalized
      ).as("payment_type"),
      coalesce(
        when(
          (is_placeholder_bid === lit(1)).or(
            (col("imp_type").cast(IntegerType) === lit(1))
              .or(imp_type === lit(2))
              .or(col("imp_type").cast(IntegerType) === lit(3))
              .or(imp_type === lit(8))
          ),
          lit(-1)
        ),
        revenue_type_normalized
      ).as("revenue_type"),
      coalesce(
        when(should_zero_seller_revenue.cast(BooleanType), lit(0)),
        when(
          (v_transaction_event_pricing
            .getField("buyer_charges")
            .getField("is_dw") === lit(1)).and(
            col("seller_member_id").cast(IntegerType) === col("buyer_member_id")
              .cast(IntegerType)
          ),
          seller_revenue_cpm
        ),
        when(_f_is_buy_side === lit(0), seller_revenue_cpm),
        when(
          !(v_transaction_event_pricing
            .getField("buyer_charges")
            .getField("is_dw") === lit(1)).and(
            col("seller_member_id").cast(IntegerType) === col("buyer_member_id")
              .cast(IntegerType)
          ),
          lit(0)
        ),
        when(imp_type === lit(9), lit(0))
      ).as("seller_revenue_cpm"),
      coalesce(
        col("log_impbus_preempt.bidder_id").cast(IntegerType),
        when(is_not_null(col("log_impbus_preempt")), lit(0)),
        col("log_impbus_impressions.bidder_id").cast(IntegerType)
      ).as("bidder_id"),
      when(string_compare(col("log_impbus_impressions.inv_code"),
                          lit("---")
           ) =!= lit(0),
           col("log_impbus_impressions.inv_code")
      ).as("inv_code"),
      when(string_compare(col("log_impbus_impressions.application_id"),
                          lit("---")
           ) =!= lit(0),
           col("log_impbus_impressions.application_id")
      ).as("application_id"),
      when(col("log_impbus_impressions.shadow_price") =!= lit(0.0d),
           col("log_impbus_impressions.shadow_price")
      ).as("shadow_price"),
      when(col("log_impbus_impressions.eap") =!= lit(0),
           col("log_impbus_impressions.eap")
      ).as("eap"),
      when(col("log_impbus_impressions.datacenter_id").cast(
             IntegerType
           ) =!= lit(0),
           col("log_impbus_impressions.datacenter_id").cast(IntegerType)
      ).as("datacenter_id"),
      when(
        col("log_impbus_impressions.imp_blacklist_or_fraud").cast(
          IntegerType
        ) =!= lit(0),
        col("log_impbus_impressions.imp_blacklist_or_fraud").cast(IntegerType)
      ).as("imp_blacklist_or_fraud"),
      f_preempt_over_impression_non_zero_explicit(
        is_not_null(col("log_impbus_preempt")),
        col("log_impbus_impressions.vp_expose_domains").cast(IntegerType),
        col("log_impbus_preempt.vp_expose_domains").cast(IntegerType)
      ).as("vp_expose_domains"),
      f_preempt_over_impression_non_zero_explicit(
        is_not_null(col("log_impbus_preempt")),
        col("log_impbus_impressions.vp_expose_categories").cast(IntegerType),
        col("log_impbus_preempt.vp_expose_categories").cast(IntegerType)
      ).as("vp_expose_categories"),
      f_preempt_over_impression_non_zero_explicit(
        is_not_null(col("log_impbus_preempt")),
        col("log_impbus_impressions.vp_expose_pubs").cast(IntegerType),
        col("log_impbus_preempt.vp_expose_pubs").cast(IntegerType)
      ).as("vp_expose_pubs"),
      f_preempt_over_impression_non_zero_explicit(
        is_not_null(col("log_impbus_preempt")),
        col("log_impbus_impressions.vp_expose_tag").cast(IntegerType),
        col("log_impbus_preempt.vp_expose_tag").cast(IntegerType)
      ).as("vp_expose_tag"),
      when(col("log_dw_bid.vp_expose_age").cast(IntegerType) =!= lit(0),
           col("log_dw_bid.vp_expose_age").cast(IntegerType)
      ).as("vp_expose_age"),
      when(col("log_dw_bid.vp_expose_gender").cast(IntegerType) =!= lit(0),
           col("log_dw_bid.vp_expose_gender").cast(IntegerType)
      ).as("vp_expose_gender"),
      when(col("log_impbus_impressions.inventory_url_id").cast(
             IntegerType
           ) =!= lit(0),
           col("log_impbus_impressions.inventory_url_id").cast(IntegerType)
      ).as("inventory_url_id"),
      when(
        col("log_impbus_impressions.audit_type").cast(IntegerType) =!= lit(0),
        col("log_impbus_impressions.audit_type").cast(IntegerType)
      ).as("audit_type"),
      when(
        col("log_impbus_impressions.is_exclusive").cast(IntegerType) =!= lit(0),
        col("log_impbus_impressions.is_exclusive").cast(IntegerType)
      ).as("is_exclusive"),
      when(
        col("log_impbus_impressions.truncate_ip").cast(IntegerType) =!= lit(0),
        col("log_impbus_impressions.truncate_ip").cast(IntegerType)
      ).as("truncate_ip"),
      when(col("log_impbus_impressions.device_id").cast(IntegerType) =!= lit(0),
           col("log_impbus_impressions.device_id").cast(IntegerType)
      ).as("device_id"),
      when(
        col("log_impbus_impressions.carrier_id").cast(IntegerType) =!= lit(0),
        col("log_impbus_impressions.carrier_id").cast(IntegerType)
      ).as("carrier_id"),
      f_preempt_over_impression_non_zero_explicit(
        is_not_null(col("log_impbus_preempt")),
        col("log_impbus_impressions.creative_audit_status").cast(IntegerType),
        col("log_impbus_preempt.creative_audit_status").cast(IntegerType)
      ).as("creative_audit_status"),
      f_preempt_over_impression_non_zero_explicit(
        is_not_null(col("log_impbus_preempt")),
        col("log_impbus_impressions.is_creative_hosted").cast(IntegerType),
        col("log_impbus_preempt.is_creative_hosted").cast(IntegerType)
      ).as("is_creative_hosted"),
      coalesce(when(should_zero_seller_revenue.cast(BooleanType), lit(0)),
               seller_deduction
      ).as("seller_deduction"),
      when(col("log_impbus_impressions.city").cast(IntegerType) =!= lit(0),
           col("log_impbus_impressions.city").cast(IntegerType)
      ).as("city"),
      when(string_compare(col("log_impbus_impressions.latitude"),
                          lit("")
           ) =!= lit(0),
           col("log_impbus_impressions.latitude")
      ).as("latitude"),
      when(string_compare(col("log_impbus_impressions.longitude"),
                          lit("")
           ) =!= lit(0),
           col("log_impbus_impressions.longitude")
      ).as("longitude"),
      when(string_compare(col("log_impbus_impressions.device_unique_id"),
                          lit("")
           ) =!= lit(0),
           col("log_impbus_impressions.device_unique_id")
      ).as("device_unique_id"),
      when(
        col("log_impbus_impressions.package_id").cast(IntegerType) =!= lit(0),
        col("log_impbus_impressions.package_id").cast(IntegerType)
      ).as("package_id"),
      when((_f_is_buy_side === lit(1)).and(
             string_compare(col("log_dw_bid.targeted_segments"),
                            lit("")
             ) =!= lit(0)
           ),
           col("log_dw_bid.targeted_segments")
      ).as("targeted_segments"),
      when(
        col("log_impbus_impressions.supply_type").cast(IntegerType) =!= lit(0),
        col("log_impbus_impressions.supply_type").cast(IntegerType)
      ).as("supply_type"),
      when(
        col("log_impbus_impressions.is_toolbar").cast(IntegerType) =!= lit(0),
        col("log_impbus_impressions.is_toolbar").cast(IntegerType)
      ).as("is_toolbar"),
      deal_id.as("deal_id"),
      f_preempt_over_impression_non_zero_explicit(
        is_not_null(col("log_impbus_preempt")),
        col("log_impbus_impressions.vp_bitmap").cast(LongType),
        col("log_impbus_preempt.vp_bitmap").cast(LongType)
      ).as("vp_bitmap"),
      view_detection_enabled.as("view_detection_enabled"),
      view_result.as("view_result"),
      when(col("log_impbus_impressions.ozone_id").cast(IntegerType) =!= lit(0),
           col("log_impbus_impressions.ozone_id").cast(IntegerType)
      ).as("ozone_id"),
      when(col("log_impbus_impressions.is_performance").cast(
             IntegerType
           ) =!= lit(0),
           col("log_impbus_impressions.is_performance").cast(IntegerType)
      ).as("is_performance"),
      when(string_compare(col("log_impbus_impressions.sdk_version"),
                          lit("---")
           ) =!= lit(0),
           col("log_impbus_impressions.sdk_version")
      ).as("sdk_version"),
      when(
        col("log_impbus_impressions.inventory_session_frequency").cast(
          IntegerType
        ) =!= lit(0),
        col("log_impbus_impressions.inventory_session_frequency").cast(
          IntegerType
        )
      ).as("inventory_session_frequency"),
      when(
        col("log_impbus_impressions.device_type").cast(IntegerType) =!= lit(0),
        col("log_impbus_impressions.device_type").cast(IntegerType)
      ).as("device_type"),
      when(col("log_impbus_impressions.dma").cast(IntegerType) =!= lit(0),
           col("log_impbus_impressions.dma").cast(IntegerType)
      ).as("dma"),
      when(string_compare(col("log_impbus_impressions.postal"),
                          lit("---")
           ) =!= lit(0),
           col("log_impbus_impressions.postal")
      ).as("postal"),
      viewdef_definition_id.as("viewdef_definition_id"),
      viewdef_viewable.as("viewdef_viewable"),
      view_measurable.as("view_measurable"),
      viewable.as("viewable"),
      when(col("log_impbus_impressions.is_secure").cast(IntegerType) =!= lit(0),
           col("log_impbus_impressions.is_secure").cast(IntegerType)
      ).as("is_secure"),
      view_non_measurable_reason.as("view_non_measurable_reason"),
      coalesce(when((buyer_trx_event_id === lit(1)).and(imp_type === lit(6)),
                    data_costs_deal
               ),
               when(_f_is_buy_side === lit(1), data_costs)
      ).as("data_costs"),
      when(col("log_impbus_impressions.bidder_instance_id").cast(
             IntegerType
           ) =!= lit(0),
           col("log_impbus_impressions.bidder_instance_id").cast(IntegerType)
      ).as("bidder_instance_id"),
      coalesce(
        when(
          (_f_is_buy_side === lit(1)).and(
            col("log_dw_bid.campaign_group_freq").cast(IntegerType) =!= lit(-2)
          ),
          col("log_dw_bid.campaign_group_freq").cast(IntegerType)
        ),
        when((_f_is_buy_side === lit(0)).and(
               virtual_log_dw_bid.getField("campaign_group_freq") =!= lit(-2)
             ),
             virtual_log_dw_bid.getField("campaign_group_freq")
        )
      ).as("campaign_group_freq"),
      coalesce(
        when((_f_is_buy_side === lit(1)).and(
               col("log_dw_bid.campaign_group_rec").cast(IntegerType) =!= lit(0)
             ),
             col("log_dw_bid.campaign_group_rec").cast(IntegerType)
        ),
        when(_f_is_buy_side === lit(0),
             virtual_log_dw_bid.getField("campaign_group_rec")
        )
      ).as("campaign_group_rec"),
      coalesce(
        when(
          (_f_is_buy_side === lit(1)).and(
            col("log_dw_bid.insertion_order_freq").cast(IntegerType) =!= lit(-2)
          ),
          col("log_dw_bid.insertion_order_freq").cast(IntegerType)
        ),
        when((_f_is_buy_side === lit(0)).and(
               virtual_log_dw_bid.getField("insertion_order_freq") =!= lit(-2)
             ),
             virtual_log_dw_bid.getField("insertion_order_freq")
        )
      ).as("insertion_order_freq"),
      coalesce(
        when(
          (_f_is_buy_side === lit(1)).and(
            col("log_dw_bid.insertion_order_rec").cast(IntegerType) =!= lit(0)
          ),
          col("log_dw_bid.insertion_order_rec").cast(IntegerType)
        ),
        when(_f_is_buy_side === lit(0),
             virtual_log_dw_bid.getField("insertion_order_rec")
        )
      ).as("insertion_order_rec"),
      when((_f_is_buy_side === lit(1)).and(
             string_compare(col("log_dw_bid.buyer_gender"), lit("u")) =!= lit(0)
           ),
           col("log_dw_bid.buyer_gender")
      ).as("buyer_gender"),
      coalesce(
        when(
          (_f_is_buy_side === lit(1))
            .and(is_not_null(col("log_dw_bid.buyer_age").cast(IntegerType)))
            .and(col("log_dw_bid.buyer_age").cast(IntegerType) =!= lit(-1)),
          col("log_dw_bid.buyer_age").cast(IntegerType)
        ),
        lit(0)
      ).as("buyer_age"),
      when((_f_is_buy_side === lit(1))
             .and(is_not_null(col("log_dw_bid.targeted_segment_list"))),
           col("log_dw_bid.targeted_segment_list")
      ).as("targeted_segment_list"),
      when((_f_is_buy_side === lit(1)).and(
             col("log_dw_bid.custom_model_id").cast(IntegerType) =!= lit(0)
           ),
           col("log_dw_bid.custom_model_id").cast(IntegerType)
      ).as("custom_model_id"),
      when(
        (_f_is_buy_side === lit(1)).and(
          col("log_dw_bid.custom_model_last_modified").cast(LongType) =!= lit(0)
        ),
        col("log_dw_bid.custom_model_last_modified").cast(LongType)
      ).as("custom_model_last_modified"),
      coalesce(
        when((_f_is_buy_side === lit(1)).and(
               string_compare(col("log_dw_bid.custom_model_output_code"),
                              lit("")
               ) =!= lit(0)
             ),
             col("log_dw_bid.custom_model_output_code")
        ),
        when(_f_is_buy_side === lit(0),
             virtual_log_dw_bid.getField("custom_model_output_code")
        )
      ).as("custom_model_output_code"),
      when(string_compare(col("log_impbus_impressions.external_uid"),
                          lit("---")
           ) =!= lit(0),
           col("log_impbus_impressions.external_uid")
      ).as("external_uid"),
      when(string_compare(col("log_impbus_impressions.request_uuid"),
                          lit("---")
           ) =!= lit(0),
           col("log_impbus_impressions.request_uuid")
      ).as("request_uuid"),
      when(
        col("log_impbus_impressions.mobile_app_instance_id").cast(
          IntegerType
        ) =!= lit(0),
        col("log_impbus_impressions.mobile_app_instance_id").cast(IntegerType)
      ).as("mobile_app_instance_id"),
      when(string_compare(col("log_impbus_impressions.traffic_source_code"),
                          lit("---")
           ) =!= lit(0),
           col("log_impbus_impressions.traffic_source_code")
      ).as("traffic_source_code"),
      when(string_compare(col("log_impbus_impressions.external_request_id"),
                          lit("---")
           ) =!= lit(0),
           col("log_impbus_impressions.external_request_id")
      ).as("external_request_id"),
      when(string_compare(col("log_impbus_impressions.stitch_group_id"),
                          lit("---")
           ) =!= lit(0),
           col("log_impbus_impressions.stitch_group_id")
      ).as("stitch_group_id"),
      deal_type.as("deal_type"),
      f_preempt_over_impression_non_zero_explicit(
        is_not_null(col("log_impbus_preempt")),
        col("log_impbus_impressions.ym_floor_id").cast(IntegerType),
        col("log_impbus_preempt.ym_floor_id").cast(IntegerType)
      ).as("ym_floor_id"),
      f_preempt_over_impression_non_zero_explicit(
        is_not_null(col("log_impbus_preempt")),
        col("log_impbus_impressions.ym_bias_id").cast(IntegerType),
        col("log_impbus_preempt.ym_bias_id").cast(IntegerType)
      ).as("ym_bias_id"),
      when(col("log_dw_bid.bid_priority").cast(IntegerType) =!= lit(0),
           col("log_dw_bid.bid_priority").cast(IntegerType)
      ).as("bid_priority"),
      struct(
        coalesce(
          col(
            "log_impbus_auction_event.auction_event_pricing.buyer_charges.rate_card_id"
          ).cast(IntegerType),
          col(
            "log_impbus_impressions_pricing.impression_event_pricing.buyer_charges.rate_card_id"
          ).cast(IntegerType),
          col("log_impbus_impressions_pricing.buyer_charges.rate_card_id").cast(
            IntegerType
          )
        ).as("rate_card_id"),
        coalesce(
          col(
            "log_impbus_auction_event.auction_event_pricing.buyer_charges.member_id"
          ).cast(IntegerType),
          col(
            "log_impbus_impressions_pricing.impression_event_pricing.buyer_charges.member_id"
          ).cast(IntegerType),
          col("log_impbus_impressions_pricing.buyer_charges.member_id").cast(
            IntegerType
          )
        ).as("member_id"),
        coalesce(
          col(
            "log_impbus_auction_event.auction_event_pricing.buyer_charges.is_dw"
          ).cast(BooleanType),
          col(
            "log_impbus_impressions_pricing.impression_event_pricing.buyer_charges.is_dw"
          ).cast(BooleanType),
          col("log_impbus_impressions_pricing.buyer_charges.is_dw").cast(
            BooleanType
          )
        ).as("is_dw"),
        coalesce(
          col(
            "log_impbus_auction_event.auction_event_pricing.buyer_charges.pricing_terms"
          ),
          col(
            "log_impbus_impressions_pricing.impression_event_pricing.buyer_charges.pricing_terms"
          ),
          buyer_charges_pricing_terms,
          col("log_impbus_impressions_pricing.buyer_charges.pricing_terms")
        ).as("pricing_terms"),
        coalesce(
          col(
            "log_impbus_auction_event.auction_event_pricing.buyer_charges.fx_margin_rate_id"
          ).cast(IntegerType),
          col(
            "log_impbus_impressions_pricing.impression_event_pricing.buyer_charges.fx_margin_rate_id"
          ).cast(IntegerType),
          col("log_impbus_impressions_pricing.buyer_charges.fx_margin_rate_id")
            .cast(IntegerType)
        ).as("fx_margin_rate_id"),
        coalesce(
          col(
            "log_impbus_auction_event.auction_event_pricing.buyer_charges.marketplace_owner_id"
          ).cast(IntegerType),
          col(
            "log_impbus_impressions_pricing.impression_event_pricing.buyer_charges.marketplace_owner_id"
          ).cast(IntegerType),
          col(
            "log_impbus_impressions_pricing.buyer_charges.marketplace_owner_id"
          ).cast(IntegerType)
        ).as("marketplace_owner_id"),
        coalesce(
          col(
            "log_impbus_auction_event.auction_event_pricing.buyer_charges.virtual_marketplace_id"
          ).cast(IntegerType),
          col(
            "log_impbus_impressions_pricing.impression_event_pricing.buyer_charges.virtual_marketplace_id"
          ).cast(IntegerType),
          col(
            "log_impbus_impressions_pricing.buyer_charges.virtual_marketplace_id"
          ).cast(IntegerType)
        ).as("virtual_marketplace_id"),
        coalesce(
          col(
            "log_impbus_auction_event.auction_event_pricing.buyer_charges.amino_enabled"
          ).cast(BooleanType),
          col(
            "log_impbus_impressions_pricing.impression_event_pricing.buyer_charges.amino_enabled"
          ).cast(BooleanType),
          col("log_impbus_impressions_pricing.buyer_charges.amino_enabled")
            .cast(BooleanType)
        ).as("amino_enabled")
      ).as("buyer_charges"),
      struct(
        coalesce(
          col(
            "log_impbus_auction_event.auction_event_pricing.seller_charges.rate_card_id"
          ).cast(IntegerType),
          col(
            "log_impbus_impressions_pricing.impression_event_pricing.seller_charges.rate_card_id"
          ).cast(IntegerType),
          col("log_impbus_impressions_pricing.seller_charges.rate_card_id")
            .cast(IntegerType)
        ).as("rate_card_id"),
        coalesce(
          col(
            "log_impbus_auction_event.auction_event_pricing.seller_charges.member_id"
          ).cast(IntegerType),
          col(
            "log_impbus_impressions_pricing.impression_event_pricing.seller_charges.member_id"
          ).cast(IntegerType),
          col("log_impbus_impressions_pricing.seller_charges.member_id").cast(
            IntegerType
          )
        ).as("member_id"),
        coalesce(
          col(
            "log_impbus_auction_event.auction_event_pricing.seller_charges.is_dw"
          ).cast(BooleanType),
          col(
            "log_impbus_impressions_pricing.impression_event_pricing.seller_charges.is_dw"
          ).cast(BooleanType),
          col("log_impbus_impressions_pricing.seller_charges.is_dw").cast(
            BooleanType
          )
        ).as("is_dw"),
        coalesce(
          when(should_zero_seller_revenue.cast(BooleanType),
               f_drop_is_deduction_pricing_terms(seller_charges_pricing_terms)
          ),
          col(
            "log_impbus_auction_event.auction_event_pricing.seller_charges.pricing_terms"
          ),
          col(
            "log_impbus_impressions_pricing.impression_event_pricing.seller_charges.pricing_terms"
          ),
          seller_charges_pricing_terms,
          col("log_impbus_impressions_pricing.seller_charges.pricing_terms")
        ).as("pricing_terms"),
        coalesce(
          col(
            "log_impbus_auction_event.auction_event_pricing.seller_charges.fx_margin_rate_id"
          ).cast(IntegerType),
          col(
            "log_impbus_impressions_pricing.impression_event_pricing.seller_charges.fx_margin_rate_id"
          ).cast(IntegerType),
          col("log_impbus_impressions_pricing.seller_charges.fx_margin_rate_id")
            .cast(IntegerType)
        ).as("fx_margin_rate_id"),
        coalesce(
          col(
            "log_impbus_auction_event.auction_event_pricing.seller_charges.marketplace_owner_id"
          ).cast(IntegerType),
          col(
            "log_impbus_impressions_pricing.impression_event_pricing.seller_charges.marketplace_owner_id"
          ).cast(IntegerType),
          col(
            "log_impbus_impressions_pricing.seller_charges.marketplace_owner_id"
          ).cast(IntegerType)
        ).as("marketplace_owner_id"),
        coalesce(
          col(
            "log_impbus_auction_event.auction_event_pricing.seller_charges.virtual_marketplace_id"
          ).cast(IntegerType),
          col(
            "log_impbus_impressions_pricing.impression_event_pricing.seller_charges.virtual_marketplace_id"
          ).cast(IntegerType),
          col(
            "log_impbus_impressions_pricing.seller_charges.virtual_marketplace_id"
          ).cast(IntegerType)
        ).as("virtual_marketplace_id"),
        coalesce(
          col(
            "log_impbus_auction_event.auction_event_pricing.seller_charges.amino_enabled"
          ).cast(BooleanType),
          col(
            "log_impbus_impressions_pricing.impression_event_pricing.seller_charges.amino_enabled"
          ).cast(BooleanType),
          col("log_impbus_impressions_pricing.seller_charges.amino_enabled")
            .cast(BooleanType)
        ).as("amino_enabled")
      ).as("seller_charges"),
      when(col("log_dw_bid.explore_disposition").cast(IntegerType) =!= lit(0),
           col("log_dw_bid.explore_disposition").cast(IntegerType)
      ).as("explore_disposition"),
      when(col("log_impbus_impressions.device_make_id").cast(
             IntegerType
           ) =!= lit(0),
           col("log_impbus_impressions.device_make_id").cast(IntegerType)
      ).as("device_make_id"),
      when(
        col("log_impbus_impressions.operating_system_family_id").cast(
          IntegerType
        ) =!= lit(1),
        col("log_impbus_impressions.operating_system_family_id").cast(
          IntegerType
        )
      ).as("operating_system_family_id"),
      col("log_impbus_impressions.tag_sizes").as("tag_sizes"),
      lit(null).as("campaign_group_models"),
      coalesce(col("log_impbus_impressions_pricing.rate_card_media_type").cast(
                 IntegerType
               ),
               lit(0)
      ).as("pricing_media_type"),
      buyer_trx_event_id.as("buyer_trx_event_id"),
      seller_trx_event_id.as("seller_trx_event_id"),
      coalesce(
        when(
          (_f_is_buy_side === lit(0)).and(
            virtual_log_dw_bid.getField("revenue_auction_event_type") =!= lit(0)
          ),
          virtual_log_dw_bid.getField("revenue_auction_event_type")
        ),
        when(
          (_f_is_buy_side === lit(1)).and(
            col("log_dw_bid.revenue_auction_event_type")
              .cast(IntegerType) =!= lit(0)
          ),
          col("log_dw_bid.revenue_auction_event_type").cast(IntegerType)
        )
      ).as("revenue_auction_event_type"),
      when(col("log_impbus_impressions.is_prebid").cast(ByteType) =!= lit(0),
           col("log_impbus_impressions.is_prebid").cast(BooleanType)
      ).as("is_prebid"),
      coalesce(
        when(imp_type === lit(6), has_seller_transacted.cast(BooleanType)),
        when(imp_type.isin(lit(7), lit(11)),
             has_buyer_transacted.cast(BooleanType)
        ),
        lit(1).cast(BooleanType)
      ).as("is_unit_of_trx"),
      imps_for_budget_caps_pacing.as("imps_for_budget_caps_pacing"),
      col("log_impbus_impressions.date_time")
        .cast(LongType)
        .as("auction_timestamp"),
      when(
        col("log_impbus_impressions_pricing.two_phase_reduction_applied").cast(
          ByteType
        ) =!= lit(0),
        col("log_impbus_impressions_pricing.two_phase_reduction_applied").cast(
          BooleanType
        )
      ).as("two_phase_reduction_applied"),
      when(col("log_impbus_impressions.region_id").cast(IntegerType) =!= lit(0),
           col("log_impbus_impressions.region_id").cast(IntegerType)
      ).as("region_id"),
      when(col("log_impbus_impressions.media_company_id").cast(
             IntegerType
           ) =!= lit(0),
           col("log_impbus_impressions.media_company_id").cast(IntegerType)
      ).as("media_company_id"),
      when(
        col("log_impbus_impressions_pricing.trade_agreement_id").cast(
          IntegerType
        ) =!= lit(0),
        col("log_impbus_impressions_pricing.trade_agreement_id").cast(
          IntegerType
        )
      ).as("trade_agreement_id"),
      struct(
        lit(null).cast(LongType).as("user_id_64"),
        lit(null).cast(StringType).as("device_unique_id"),
        lit(null).cast(StringType).as("external_uid"),
        lit(null).cast(BinaryType).as("ip_address"),
        struct(lit(null).cast(IntegerType).as("graph_id"),
               lit(null).cast(LongType).as("group_id")
        ).as("crossdevice_group"),
        lit(null).cast(DoubleType).as("latitude"),
        lit(null).cast(DoubleType).as("longitude"),
        lit(null).cast(BinaryType).as("ipv6_address"),
        lit(null).cast(BooleanType).as("subject_to_gdpr"),
        lit(null).cast(StringType).as("geo_country"),
        lit(null).cast(StringType).as("gdpr_consent_string"),
        lit(null).cast(BinaryType).as("preempt_ip_address"),
        lit(null).cast(IntegerType).as("device_type"),
        lit(null).cast(IntegerType).as("device_make_id"),
        lit(null).cast(IntegerType).as("device_model_id"),
        lit(null).cast(LongType).as("new_user_id_64"),
        lit(null).cast(BooleanType).as("is_service_provider_mode"),
        lit(null).cast(BooleanType).as("is_personal_info_sale")
      ).as("personal_data"),
      struct(lit(null).cast(BinaryType).as("user_id"))
        .as("anonymized_user_info"),
      when(string_compare(col("log_impbus_impressions.gdpr_consent_cookie"),
                          lit("")
           ) =!= lit(0),
           col("log_impbus_impressions.gdpr_consent_cookie")
      ).as("gdpr_consent_cookie"),
      col("additional_clearing_events").as("additional_clearing_events"),
      col("log_impbus_impressions.fx_rate_snapshot_id")
        .cast(IntegerType)
        .as("fx_rate_snapshot_id"),
      struct(lit(null).cast(IntegerType).as("graph_id"),
             lit(null).cast(BinaryType).as("group_id")
      ).as("crossdevice_group_anon"),
      struct(lit(null).cast(IntegerType).as("graph_provider_member_id"),
             lit(null).cast(DoubleType).as("cost_cpm_usd")
      ).as("crossdevice_graph_cost"),
      when(
        (_f_is_buy_side === lit(1)).and(
          col("log_dw_bid.revenue_event_type_id").cast(IntegerType) =!= lit(0)
        ),
        col("log_dw_bid.revenue_event_type_id").cast(IntegerType)
      ).as("revenue_event_type_id"),
      buyer_trx_event_type_id.as("buyer_trx_event_type_id"),
      seller_trx_event_type_id.as("seller_trx_event_type_id"),
      coalesce(col("log_impbus_preempt.external_creative_id"),
               when(is_not_null(col("log_impbus_preempt")), lit("---"))
      ).as("external_creative_id"),
      coalesce(
        when(is_not_null(col("log_dw_bid_deal")).and(imp_type === lit(6)),
             col("log_dw_bid_deal.targeted_segment_details")
        ),
        when(_f_is_buy_side === lit(1),
             col("log_dw_bid.targeted_segment_details")
        )
      ).as("targeted_segment_details"),
      coalesce(col("log_impbus_preempt.seat_id").cast(IntegerType),
               when(is_not_null(col("log_impbus_preempt")), lit(0)),
               lit(0)
      ).as("bidder_seat_id"),
      when(col("log_impbus_impressions.is_whiteops_scanned").cast(
             ByteType
           ) =!= lit(0),
           col("log_impbus_impressions.is_whiteops_scanned").cast(BooleanType)
      ).as("is_whiteops_scanned"),
      lit(null).as("default_referrer_url"),
      when(is_curated === lit(1), lit(1).cast(BooleanType)).as("is_curated"),
      curator_member_id.as("curator_member_id"),
      coalesce(
        when(
          (_f_is_buy_side === lit(1))
            .and(
              is_not_null(
                col("log_dw_view.revenue_info.total_partner_fees_microcents")
                  .cast(LongType)
              )
            )
            .and(
              is_not_null(
                col("log_dw_bid.revenue_info.total_partner_fees_microcents")
                  .cast(LongType)
              )
            ),
          col("log_dw_view.revenue_info.total_partner_fees_microcents")
            .cast(LongType) + col(
            "log_dw_bid.revenue_info.total_partner_fees_microcents"
          ).cast(LongType)
        ),
        when(
          (_f_is_buy_side === lit(1)).and(
            is_not_null(
              col("log_dw_bid.revenue_info.total_partner_fees_microcents")
                .cast(LongType)
            )
          ),
          col("log_dw_bid.revenue_info.total_partner_fees_microcents").cast(
            LongType
          )
        ),
        when(_f_is_buy_side === lit(1), lit(0))
      ).as("total_partner_fees_microcents"),
      lit(null).as("net_buyer_spend"),
      when(col("log_impbus_preempt.is_prebid_server").cast(ByteType) =!= lit(0),
           col("log_impbus_preempt.is_prebid_server").cast(BooleanType)
      ).as("is_prebid_server"),
      when(
        (_f_is_buy_side === lit(1)).and(
          col("log_dw_bid.cold_start_price_type").cast(IntegerType) =!= lit(-1)
        ),
        col("log_dw_bid.cold_start_price_type").cast(IntegerType)
      ).as("cold_start_price_type"),
      when((_f_is_buy_side === lit(1)).and(
             col("log_dw_bid.discovery_state").cast(IntegerType) =!= lit(-1)
           ),
           col("log_dw_bid.discovery_state").cast(IntegerType)
      ).as("discovery_state"),
      coalesce(
        when(
          _f_is_buy_side === lit(1),
          col("log_dw_bid.insertion_order_budget_interval_id").cast(IntegerType)
        ),
        billing_period_id,
        lit(0)
      ).as("billing_period_id"),
      coalesce(
        when(
          _f_is_buy_side === lit(1),
          col("log_dw_bid.campaign_group_budget_interval_id").cast(IntegerType)
        ),
        flight_id,
        lit(0)
      ).as("flight_id"),
      split_id.as("split_id"),
      when(
        (imp_type === lit(7)).and(
          v_transaction_event_pricing.getField("buyer_transacted") === lit(1)
        ),
        v_transaction_event_pricing
          .getField("net_payment_value_microcents")
          .cast(DoubleType) / lit(100000.0d)
      ).as("net_media_cost_dollars_cpm"),
      coalesce(
        when(
          (_f_is_buy_side === lit(1))
            .and(
              is_not_null(
                col("log_dw_view.revenue_info.total_data_costs_microcents")
                  .cast(LongType)
              )
            )
            .and(
              is_not_null(
                col("log_dw_bid.revenue_info.total_data_costs_microcents")
                  .cast(LongType)
              )
            ),
          col("log_dw_view.revenue_info.total_data_costs_microcents")
            .cast(LongType) + col(
            "log_dw_bid.revenue_info.total_data_costs_microcents"
          ).cast(LongType)
        ),
        when(
          (_f_is_buy_side === lit(1)).and(
            is_not_null(
              col("log_dw_bid.revenue_info.total_data_costs_microcents")
                .cast(LongType)
            )
          ),
          col("log_dw_bid.revenue_info.total_data_costs_microcents").cast(
            LongType
          )
        ),
        when(_f_is_buy_side === lit(1), lit(0))
      ).as("total_data_costs_microcents"),
      coalesce(
        when(
          (_f_is_buy_side === lit(1))
            .and(
              is_not_null(
                col("log_dw_view.revenue_info.total_profit_microcents")
                  .cast(LongType)
              )
            )
            .and(
              is_not_null(
                col("log_dw_bid.revenue_info.total_profit_microcents")
                  .cast(LongType)
              )
            ),
          col("log_dw_view.revenue_info.total_profit_microcents")
            .cast(LongType) + col(
            "log_dw_bid.revenue_info.total_profit_microcents"
          ).cast(LongType)
        ),
        when(
          (_f_is_buy_side === lit(1)).and(
            is_not_null(
              col("log_dw_bid.revenue_info.total_profit_microcents")
                .cast(LongType)
            )
          ),
          col("log_dw_bid.revenue_info.total_profit_microcents").cast(LongType)
        ),
        when(_f_is_buy_side === lit(1), lit(0))
      ).as("total_profit_microcents"),
      when(
        (_f_is_buy_side === lit(1)).and(
          col("log_dw_bid.targeted_crossdevice_graph_id")
            .cast(IntegerType) =!= lit(0)
        ),
        col("log_dw_bid.targeted_crossdevice_graph_id").cast(IntegerType)
      ).as("targeted_crossdevice_graph_id"),
      when((_f_is_buy_side === lit(1))
             .and(col("log_dw_bid.discovery_prediction") =!= lit(0.0d)),
           col("log_dw_bid.discovery_prediction")
      ).as("discovery_prediction"),
      campaign_group_type_id.as("campaign_group_type_id"),
      when(col("log_impbus_impressions.hb_source").cast(IntegerType) =!= lit(0),
           col("log_impbus_impressions.hb_source").cast(IntegerType)
      ).as("hb_source"),
      f_preempt_over_impression_94298.as("external_campaign_id"),
      when(_f_is_buy_side === lit(1),
           col("log_dw_bid.excluded_targeted_segment_details")
      ).as("excluded_targeted_segment_details"),
      lit(null).cast(StringType).as("trust_id"),
      when((_f_is_buy_side === lit(1))
             .and(col("log_dw_bid.predicted_kpi_event_rate") =!= lit(0.0d)),
           col("log_dw_bid.predicted_kpi_event_rate")
      ).as("predicted_kpi_event_rate"),
      coalesce(
        when(_f_is_buy_side === lit(1),
             col("log_dw_bid.has_crossdevice_reach_extension").cast(BooleanType)
        ),
        when(imp_type === lit(6),
             col("log_dw_bid_deal.has_crossdevice_reach_extension").cast(
               BooleanType
             )
        )
      ).as("has_crossdevice_reach_extension"),
      coalesce(
        when(_f_is_buy_side === lit(1),
             col("log_dw_bid.crossdevice_graph_membership")
        ),
        when(imp_type === lit(6),
             col("log_dw_bid_deal.crossdevice_graph_membership")
        )
      ).as("crossdevice_graph_membership"),
      when(
        _f_is_buy_side === lit(1),
        coalesce(col(
                   "log_dw_bid.revenue_info.total_segment_data_costs_microcents"
                 ).cast(LongType),
                 lit(0)
        ) + coalesce(
          col("log_dw_view.revenue_info.total_segment_data_costs_microcents")
            .cast(LongType),
          lit(0)
        )
      ).otherwise(lit(0)).as("total_segment_data_costs_microcents"),
      when(
        _f_is_buy_side === lit(1),
        coalesce(col("log_dw_bid.revenue_info.total_feature_costs_microcents")
                   .cast(LongType),
                 lit(0)
        ) + coalesce(col(
                       "log_dw_view.revenue_info.total_feature_costs_microcents"
                     ).cast(LongType),
                     lit(0)
        )
      ).otherwise(lit(0)).as("total_feature_costs_microcents"),
      when(
        col("log_impbus_impressions_pricing.counterparty_ruleset_type").cast(
          IntegerType
        ) =!= lit(0),
        col("log_impbus_impressions_pricing.counterparty_ruleset_type").cast(
          IntegerType
        )
      ).as("counterparty_ruleset_type"),
      struct(
        lit(null).cast(IntegerType).as("product_feed_id"),
        lit(null).cast(IntegerType).as("item_selection_strategy_id"),
        lit(null).cast(StringType).as("product_uuid")
      ).as("log_product_ads"),
      coalesce(when(imp_type.isin(lit(7),                          lit(5)),
                    coalesce(col("log_dw_bid.line_item_currency"), lit("---"))
               ),
               lit("---")
      ).as("buyer_line_item_currency"),
      coalesce(
        when(imp_type === lit(6),
             coalesce(col("log_dw_bid_deal.line_item_currency"), lit("---"))
        ),
        lit("---")
      ).as("deal_line_item_currency"),
      coalesce(when(_f_is_buy_side === lit(1),
                    col("log_dw_bid.measurement_fee_cpm_usd")
               ),
               lit(0)
      ).as("measurement_fee_usd"),
      coalesce(
        when(_f_is_buy_side === lit(1),
             col("log_dw_bid.measurement_provider_member_id").cast(IntegerType)
        ),
        lit(0)
      ).as("measurement_provider_member_id"),
      when(_f_is_buy_side === lit(1),
           col("log_dw_bid.offline_attribution_provider_member_id").cast(
             IntegerType
           )
      ).as("offline_attribution_provider_member_id"),
      when(_f_is_buy_side === lit(1),
           col("log_dw_bid.offline_attribution_cost_usd_cpm")
      ).as("offline_attribution_cost_usd_cpm"),
      when(col("log_impbus_impressions.pred_info").cast(IntegerType) =!= lit(0),
           col("log_impbus_impressions.pred_info").cast(IntegerType)
      ).as("pred_info"),
      col("log_impbus_impressions.imp_rejecter_do_auction")
        .cast(BooleanType)
        .as("imp_rejecter_do_auction"),
      col("log_impbus_impressions.personal_identifiers")
        .as("personal_identifiers"),
      col("log_impbus_impressions.is_imp_rejecter_applied")
        .cast(BooleanType)
        .as("imp_rejecter_applied"),
      lit(null).cast(DoubleType).as("ip_derived_latitude"),
      lit(null).cast(DoubleType).as("ip_derived_longitude"),
      col("log_impbus_impressions.personal_identifiers_experimental")
        .as("personal_identifiers_experimental"),
      col("log_impbus_impressions.postal_code_ext_id")
        .cast(IntegerType)
        .as("postal_code_ext_id"),
      coalesce(when(buyer_trx_event_id === lit(2),
                    col("log_dw_view.ecpm_conversion_rate")
               ),
               lit(1.0d)
      ).as("ecpm_conversion_rate"),
      when(sup_ip_range_lookup_count > lit(0), lit(1).cast(BooleanType))
        .otherwise(lit(0).cast(BooleanType))
        .as("is_residential_ip"),
      col("log_impbus_impressions.hashed_ip").as("hashed_ip"),
      coalesce(
        when(is_not_null(col("log_dw_bid_deal")).and(imp_type === lit(6)),
             col("log_dw_bid_deal.targeted_segment_details_by_id_type")
        ),
        when(_f_is_buy_side === lit(1),
             col("log_dw_bid.targeted_segment_details_by_id_type")
        )
      ).as("targeted_segment_details_by_id_type"),
      when(_f_is_buy_side === lit(1), col("log_dw_bid.offline_attribution"))
        .as("offline_attribution"),
      col("log_dw_bid.frequency_cap_type_internal")
        .cast(IntegerType)
        .as("frequency_cap_type_internal"),
      col("log_dw_bid.modeled_cap_did_override_line_item_daily_cap")
        .cast(BooleanType)
        .as("modeled_cap_did_override_line_item_daily_cap"),
      col("log_dw_bid.modeled_cap_user_sample_rate")
        .as("modeled_cap_user_sample_rate"),
      when(
        coalesce(col("log_impbus_impressions.device_type").cast(IntegerType),
                 lit(99)
        ) === lit(8),
        coalesce(
          col("log_impbus_impressions_pricing_dup.estimated_audience_imps"),
          lit(1.0d)
        )
      ).otherwise(lit(1.0d)).as("estimated_audience_imps"),
      when(
        coalesce(col("log_impbus_impressions.device_type").cast(IntegerType),
                 lit(99)
        ) === lit(8),
        coalesce(col("log_impbus_impressions_pricing_dup.audience_imps"),
                 lit(1.0d)
        )
      ).otherwise(lit(1.0d)).as("audience_imps"),
      col("log_dw_bid.district_postal_code_lists")
        .as("district_postal_code_lists"),
      coalesce(
        when(imp_type.isin(lit(4), lit(7), lit(5)),
             coalesce(col("log_dw_bid.bidding_host_id").cast(IntegerType),
                      lit(0)
             )
        ),
        when((imp_type === lit(6)).and(is_not_null(col("log_dw_bid_deal"))),
             coalesce(col("log_dw_bid_deal.bidding_host_id").cast(IntegerType),
                      lit(0)
             )
        )
      ).as("bidding_host_id"),
      lit(null).cast(LongType).as("buyer_dpvp_bitmap"),
      lit(null).cast(LongType).as("seller_dpvp_bitmap"),
      col("log_impbus_impressions.browser_code_id")
        .cast(IntegerType)
        .as("browser_code_id"),
      col("log_impbus_impressions.is_prebid_server_included")
        .cast(IntegerType)
        .as("is_prebid_server_included"),
      coalesce(col("log_dw_bid.feature_tests_bitmap").cast(IntegerType), lit(0))
        .as("feature_tests_bitmap"),
      coalesce(col("log_impbus_impressions.private_auction_eligible").cast(
                 BooleanType
               ),
               lit(0).cast(BooleanType)
      ).as("private_auction_eligible"),
      coalesce(
        col("log_impbus_impressions.chrome_traffic_label").cast(IntegerType),
        lit(0)
      ).as("chrome_traffic_label"),
      coalesce(
        col("log_impbus_impressions.is_private_auction").cast(BooleanType),
        lit(0).cast(BooleanType)
      ).as("is_private_auction"),
      f_preempt_over_impression_95337.as("creative_media_subtype_id"),
      col("log_impbus_impressions.allowed_media_types").as(
        "allowed_media_types"
      )
    )

  def f_create_agg_dw_impressions(
    sup_ip_range_lookup_count:         Column,
    sup_common_deal_lookup_2:          Column,
    sup_common_deal_lookup:            Column,
    sup_bidder_campaign_lookup:        Column,
    _f_view_detection_enabled:         Column,
    f_transaction_event_type_id_87067: Column,
    _f_find_personal_identifier:       Column,
    _f_viewdef_definition_id:          Column,
    _f_is_buy_side:                    Column,
    _f_has_transacted:                 Column,
    f_transaction_event_87047:         Column,
    f_transaction_event_87057:         Column,
    f_preempt_over_impression_95337:   Column,
    f_transaction_event_type_id_87077: Column,
    f_preempt_over_impression_94298:   Column,
    f_preempt_over_impression_88439:   Column,
    f_preempt_over_impression_88639:   Column
  ): Column = {
    var imp_type:         org.apache.spark.sql.Column = lit(1)
    var is_dw_normalized: org.apache.spark.sql.Column = lit(0)
    var buyer_member_id:  org.apache.spark.sql.Column = lit(0)
    var campaign: org.apache.spark.sql.Column = lit(null).cast(
      StructType(
        List(
          StructField("campaign_id",            IntegerType, true),
          StructField("campaign_type_id",       IntegerType, true),
          StructField("campaign_group_id",      IntegerType, true),
          StructField("campaign_group_type_id", IntegerType, true)
        )
      )
    )
    var split_id: org.apache.spark.sql.Column =
      coalesce(col("log_dw_bid.split_id").cast(IntegerType), lit(0))
        .cast(IntegerType)
    var seller_trx_event_id:      org.apache.spark.sql.Column = lit(0)
    var buyer_trx_event_id:       org.apache.spark.sql.Column = lit(0)
    var seller_trx_event_type_id: org.apache.spark.sql.Column = lit(0)
    var buyer_trx_event_type_id:  org.apache.spark.sql.Column = lit(0)
    var payment_type_normalized:  org.apache.spark.sql.Column = lit(999)
    var revenue_type_normalized:  org.apache.spark.sql.Column = lit(999)
    var has_seller_transacted: org.apache.spark.sql.Column =
      lit(0).cast(BooleanType)
    var has_buyer_transacted: org.apache.spark.sql.Column =
      lit(0).cast(BooleanType)
    var view_detection_enabled:     org.apache.spark.sql.Column = lit(0)
    var view_measurable:            org.apache.spark.sql.Column = lit(0)
    var viewable:                   org.apache.spark.sql.Column = lit(0)
    var view_non_measurable_reason: org.apache.spark.sql.Column = lit(0)
    var viewdef_definition_id:      org.apache.spark.sql.Column = lit(0)
    var viewdef_viewable:           org.apache.spark.sql.Column = lit(0)
    var view_result:                org.apache.spark.sql.Column = lit(0)
    var is_curated:                 org.apache.spark.sql.Column = lit(0)
    var curator_member_id: org.apache.spark.sql.Column =
      lit(null).cast(IntegerType)
    var member_id_by_deal_id: org.apache.spark.sql.Column = lit(null).cast(
      StructType(
        List(StructField("id",           IntegerType, true),
             StructField("member_id",    IntegerType, true),
             StructField("deal_type_id", IntegerType, true)
        )
      )
    )
    var deal_id:                     org.apache.spark.sql.Column = lit(0)
    var deal_type:                   org.apache.spark.sql.Column = lit(null).cast(IntegerType)
    var ttl:                         org.apache.spark.sql.Column = lit(0)
    var imps_for_budget_caps_pacing: org.apache.spark.sql.Column = lit(0)
    var is_budget_table_imp_type:    org.apache.spark.sql.Column = lit(0)
    var is_not_roadblock_secondary:  org.apache.spark.sql.Column = lit(0)
    var is_external_imp_type:        org.apache.spark.sql.Column = lit(0)
    var is_not_video_imp:            org.apache.spark.sql.Column = lit(0)
    var creative_id:                 org.apache.spark.sql.Column = lit(null).cast(IntegerType)
    var advertiser_id:               org.apache.spark.sql.Column = lit(null).cast(IntegerType)
    var campaign_group_id: org.apache.spark.sql.Column =
      lit(null).cast(IntegerType)
    var insertion_order_id: org.apache.spark.sql.Column =
      lit(null).cast(IntegerType)
    var campaign_id:            org.apache.spark.sql.Column = lit(null).cast(IntegerType)
    var campaign_group_type_id: org.apache.spark.sql.Column = lit(0)
    var is_placeholder_bid:     org.apache.spark.sql.Column = lit(0)
    var flight_id:              org.apache.spark.sql.Column = lit(null).cast(IntegerType)
    var billing_period_id: org.apache.spark.sql.Column =
      lit(null).cast(IntegerType)
    var should_process_views: org.apache.spark.sql.Column =
      lit(null).cast(IntegerType)
    var should_zero_seller_revenue: org.apache.spark.sql.Column =
      lit(0).cast(BooleanType)
    var netflix_ppid: org.apache.spark.sql.Column = lit(null).cast(StringType)
    var v_transaction_event_pricing: org.apache.spark.sql.Column =
      lit(null).cast(
        StructType(
          List(
            StructField("gross_payment_value_microcents", LongType, true),
            StructField("net_payment_value_microcents",   LongType, true),
            StructField("seller_revenue_microcents",      LongType, true),
            StructField(
              "buyer_charges",
              StructType(
                List(
                  StructField("rate_card_id", IntegerType, true),
                  StructField("member_id",    IntegerType, true),
                  StructField("is_dw",        BooleanType, true),
                  StructField(
                    "pricing_terms",
                    ArrayType(
                      StructType(
                        List(
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
                  StructField("fx_margin_rate_id",      IntegerType, true),
                  StructField("marketplace_owner_id",   IntegerType, true),
                  StructField("virtual_marketplace_id", IntegerType, true),
                  StructField("amino_enabled",          BooleanType, true)
                )
              ),
              true
            ),
            StructField(
              "seller_charges",
              StructType(
                List(
                  StructField("rate_card_id", IntegerType, true),
                  StructField("member_id",    IntegerType, true),
                  StructField("is_dw",        BooleanType, true),
                  StructField(
                    "pricing_terms",
                    ArrayType(
                      StructType(
                        List(
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
                  StructField("fx_margin_rate_id",      IntegerType, true),
                  StructField("marketplace_owner_id",   IntegerType, true),
                  StructField("virtual_marketplace_id", IntegerType, true),
                  StructField("amino_enabled",          BooleanType, true)
                )
              ),
              true
            ),
            StructField("buyer_transacted",  BooleanType, true),
            StructField("seller_transacted", BooleanType, true)
          )
        )
      )
    var buyer_charges_pricing_terms: org.apache.spark.sql.Column =
      lit(null).cast(
        ArrayType(
          StructType(
            List(
              StructField("term_id",                 IntegerType, true),
              StructField("amount",                  DoubleType,  true),
              StructField("rate",                    DoubleType,  true),
              StructField("is_deduction",            BooleanType, true),
              StructField("is_media_cost_dependent", BooleanType, true),
              StructField("data_member_id",          IntegerType, true)
            )
          ),
          true
        )
      )
    var seller_charges_pricing_terms: org.apache.spark.sql.Column =
      lit(null).cast(
        ArrayType(
          StructType(
            List(
              StructField("term_id",                 IntegerType, true),
              StructField("amount",                  DoubleType,  true),
              StructField("rate",                    DoubleType,  true),
              StructField("is_deduction",            BooleanType, true),
              StructField("is_media_cost_dependent", BooleanType, true),
              StructField("data_member_id",          IntegerType, true)
            )
          ),
          true
        )
      )
    var v_pricing_term: org.apache.spark.sql.Column = lit(null).cast(
      StructType(
        List(
          StructField("term_id",                 IntegerType, true),
          StructField("amount",                  DoubleType,  true),
          StructField("rate",                    DoubleType,  true),
          StructField("is_deduction",            BooleanType, true),
          StructField("is_media_cost_dependent", BooleanType, true),
          StructField("data_member_id",          IntegerType, true)
        )
      )
    )
    var auction_service_deduction:   org.apache.spark.sql.Column = lit(0)
    var auction_service_fees:        org.apache.spark.sql.Column = lit(0)
    var discrepancy_allowance:       org.apache.spark.sql.Column = lit(0)
    var creative_overage_fees:       org.apache.spark.sql.Column = lit(0)
    var buyer_spend_cpm:             org.apache.spark.sql.Column = lit(0)
    var buyer_spend_microcents:      org.apache.spark.sql.Column = lit(0)
    var seller_revenue_cpm:          org.apache.spark.sql.Column = lit(0)
    var seller_revenue_microcents:   org.apache.spark.sql.Column = lit(0)
    var seller_deduction_term_id_1:  org.apache.spark.sql.Column = lit(0)
    var seller_deduction_term_id_74: org.apache.spark.sql.Column = lit(0)
    var seller_deduction:            org.apache.spark.sql.Column = lit(0)
    var commission_cpm:              org.apache.spark.sql.Column = lit(0)
    var commission_revshare:         org.apache.spark.sql.Column = lit(0)
    var serving_fees_cpm:            org.apache.spark.sql.Column = lit(0)
    var serving_fees_revshare:       org.apache.spark.sql.Column = lit(0)
    var booked_revenue_dollars:      org.apache.spark.sql.Column = lit(0)
    var media_buy_rev_share_pct:     org.apache.spark.sql.Column = lit(0)
    var media_buy_cost:              org.apache.spark.sql.Column = lit(0)
    var booked_revenue_adv_curr:     org.apache.spark.sql.Column = lit(0)
    var media_cost_dollars_cpm:      org.apache.spark.sql.Column = lit(0)
    var member_sales_tax_rate_pct:   org.apache.spark.sql.Column = lit(0)
    var revenue_value: org.apache.spark.sql.Column =
      coalesce(col("log_dw_bid.revenue_value_dollars"), lit(0)).cast(DoubleType)
    var data_costs: org.apache.spark.sql.Column = lit(null).cast(
      ArrayType(
        StructType(
          List(
            StructField("data_member_id", IntegerType,                  true),
            StructField("cost",           DoubleType,                   true),
            StructField("used_segments",  ArrayType(IntegerType, true), true),
            StructField("cost_pct",       DoubleType,                   true)
          )
        ),
        true
      )
    )
    var virtual_log_dw_bid: org.apache.spark.sql.Column = struct(
      lit(0L).cast(LongType).as("date_time"),
      lit(0L).cast(LongType).as("auction_id_64"),
      lit(null).as("price"),
      lit(null).as("member_id"),
      lit(null).as("advertiser_id"),
      lit(null).as("campaign_group_id"),
      lit(null).as("campaign_id"),
      lit(null).as("creative_id"),
      lit(null).as("creative_freq"),
      lit(null).as("creative_rec"),
      lit(null).as("advertiser_freq"),
      lit(null).as("advertiser_rec"),
      lit(null).as("is_remarketing"),
      lit(null).as("user_group_id"),
      lit(null).as("media_buy_cost"),
      lit(null).as("is_default"),
      lit(null).as("pub_rule_id"),
      lit(null).as("media_buy_rev_share_pct"),
      lit(null).as("pricing_type"),
      lit(null).as("can_convert"),
      lit(null).as("is_control"),
      lit(null).as("control_pct"),
      lit(null).as("control_creative_id"),
      lit(null).as("cadence_modifier"),
      lit(null).as("advertiser_currency"),
      lit(null).as("advertiser_exchange_rate"),
      lit(null).as("insertion_order_id"),
      lit(null).as("predict_type"),
      lit(null).as("predict_type_goal"),
      lit(null).as("revenue_value_dollars"),
      lit(null).as("revenue_value_adv_curr"),
      lit(null).as("commission_cpm"),
      lit(null).as("commission_revshare"),
      lit(null).as("serving_fees_cpm"),
      lit(null).as("serving_fees_revshare"),
      lit(null).as("publisher_currency"),
      lit(null).as("publisher_exchange_rate"),
      lit(null).as("payment_type"),
      lit(null).as("payment_value"),
      lit(null).as("creative_group_freq"),
      lit(null).as("creative_group_rec"),
      lit(null).as("revenue_type"),
      lit(null).as("apply_cost_on_default"),
      lit(null).as("instance_id"),
      lit(null).as("vp_expose_age"),
      lit(null).as("vp_expose_gender"),
      lit(null).as("targeted_segments"),
      lit(null).as("ttl"),
      lit(0L).cast(LongType).as("auction_timestamp"),
      array()
        .cast(
          ArrayType(
            StructType(
              List(
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
          )
        )
        .as("data_costs"),
      array().cast(ArrayType(IntegerType, true)).as("targeted_segment_list"),
      lit(null).as("campaign_group_freq"),
      lit(null).as("campaign_group_rec"),
      lit(null).as("insertion_order_freq"),
      lit(null).as("insertion_order_rec"),
      lit(null).as("buyer_gender"),
      lit(null).as("buyer_age"),
      lit(null).as("custom_model_id"),
      lit(null).as("custom_model_last_modified"),
      lit(null).as("custom_model_output_code"),
      lit(null).as("bid_priority"),
      lit(null).as("explore_disposition"),
      lit(null).as("revenue_auction_event_type"),
      array()
        .cast(
          ArrayType(
            StructType(
              List(
                StructField("model_type", IntegerType, true),
                StructField("model_id",   IntegerType, true),
                StructField("leaf_code",  StringType,  true),
                StructField("origin",     IntegerType, true),
                StructField("experiment", IntegerType, true),
                StructField("value",      FloatType,   true)
              )
            ),
            true
          )
        )
        .as("campaign_group_models"),
      lit(null).as("impression_transaction_type"),
      lit(null).as("is_deferred"),
      lit(null).as("log_type"),
      lit(null).as("crossdevice_group_anon"),
      lit(null).as("fx_rate_snapshot_id"),
      lit(null).as("crossdevice_graph_cost"),
      lit(null).as("revenue_event_type_id"),
      array()
        .cast(
          ArrayType(StructType(
                      List(StructField("segment_id",    IntegerType, true),
                           StructField("last_seen_min", IntegerType, true)
                      )
                    ),
                    true
          )
        )
        .as("targeted_segment_details"),
      lit(null).as("insertion_order_budget_interval_id"),
      lit(null).as("campaign_group_budget_interval_id"),
      lit(null).as("cold_start_price_type"),
      lit(null).as("discovery_state"),
      lit(null).as("revenue_info"),
      lit(null).as("use_revenue_info"),
      lit(null).as("sales_tax_rate_pct"),
      lit(null).as("targeted_crossdevice_graph_id"),
      lit(null).as("product_feed_id"),
      lit(null).as("item_selection_strategy_id"),
      lit(null).as("discovery_prediction"),
      lit(null).as("bidding_host_id"),
      lit(null).as("split_id"),
      array()
        .cast(
          ArrayType(
            StructType(List(StructField("segment_id", IntegerType, true))),
            true
          )
        )
        .as("excluded_targeted_segment_details"),
      lit(null).as("predicted_kpi_event_rate"),
      lit(null).as("has_crossdevice_reach_extension"),
      lit(null).as("advertiser_expected_value_ecpm_ac"),
      lit(null).as("bpp_multiplier"),
      lit(null).as("bpp_offset"),
      lit(null).as("bid_modifier"),
      lit(null).as("payment_value_microcents"),
      array()
        .cast(
          ArrayType(StructType(
                      List(StructField("graph_id", IntegerType, true),
                           StructField("group_id", BinaryType,  true)
                      )
                    ),
                    true
          )
        )
        .as("crossdevice_graph_membership"),
      array()
        .cast(
          ArrayType(
            StructType(
              List(
                StructField("kpi_event_id",               IntegerType, true),
                StructField("ev_kpi_event_ac",            DoubleType,  true),
                StructField("p_kpi_event",                DoubleType,  true),
                StructField("bpo_aggressiveness_factor",  DoubleType,  true),
                StructField("min_margin_pct",             DoubleType,  true),
                StructField("max_revenue_or_bid_value",   DoubleType,  true),
                StructField("min_revenue_or_bid_value",   DoubleType,  true),
                StructField("cold_start_price_ac",        DoubleType,  true),
                StructField("dynamic_bid_max_revenue_ac", DoubleType,  true),
                StructField("p_revenue_event",            DoubleType,  true),
                StructField("total_fees_deducted_ac",     DoubleType,  true)
              )
            ),
            true
          )
        )
        .as("valuation_landscape"),
      lit(null).as("line_item_currency"),
      lit(null).as("measurement_fee_cpm_usd"),
      lit(null).as("measurement_provider_id"),
      lit(null).as("measurement_provider_member_id"),
      lit(null).as("offline_attribution_provider_member_id"),
      lit(null).as("offline_attribution_cost_usd_cpm"),
      array()
        .cast(
          ArrayType(
            StructType(
              List(
                StructField("identity_type", IntegerType, true),
                StructField(
                  "targeted_segment_details",
                  ArrayType(
                    StructType(
                      List(StructField("segment_id",    IntegerType, true),
                           StructField("last_seen_min", IntegerType, true)
                      )
                    ),
                    true
                  ),
                  true
                )
              )
            ),
            true
          )
        )
        .as("targeted_segment_details_by_id_type"),
      array()
        .cast(
          ArrayType(StructType(
                      List(StructField("provider_member_id", IntegerType, true),
                           StructField("cost_usd_cpm",       DoubleType,  true)
                      )
                    ),
                    true
          )
        )
        .as("offline_attribution"),
      lit(null).as("frequency_cap_type_internal"),
      lit(null).as("modeled_cap_did_override_line_item_daily_cap"),
      lit(null).as("modeled_cap_user_sample_rate"),
      lit(null).as("bid_rate"),
      array()
        .cast(ArrayType(IntegerType, true))
        .as("district_postal_code_lists"),
      lit(null).as("pre_bpp_price"),
      lit(null).as("feature_tests_bitmap")
    )
    var data_costs_deal: org.apache.spark.sql.Column = lit(null).cast(
      ArrayType(
        StructType(
          List(
            StructField("data_member_id", IntegerType,                  true),
            StructField("cost",           DoubleType,                   true),
            StructField("used_segments",  ArrayType(IntegerType, true), true),
            StructField("cost_pct",       DoubleType,                   true)
          )
        ),
        true
      )
    )
    var l_seller_deal_member_id: org.apache.spark.sql.Column =
      lit(null).cast(IntegerType)
    imp_type =
      when(is_not_null(col("imp_type").cast(IntegerType)).cast(BooleanType),
           col("imp_type").cast(IntegerType)
      ).otherwise(imp_type).cast(IntegerType)
    ttl = coalesce(col("log_impbus_impressions.ttl").cast(IntegerType), lit(0))
      .cast(IntegerType)
    buyer_member_id = f_preempt_over_impression_non_zero_explicit(
      is_not_null(col("log_impbus_preempt.buyer_member_id").cast(IntegerType)),
      col("log_impbus_impressions.buyer_member_id").cast(IntegerType),
      col("log_impbus_preempt.buyer_member_id").cast(IntegerType)
    ).cast(IntegerType)
    is_dw_normalized = when(
      is_not_null(col("log_impbus_impressions_pricing.seller_charges.is_dw"))
        .and(
          col("log_impbus_impressions_pricing.seller_charges.is_dw") === lit(1)
        )
        .and(
          is_not_null(col("log_impbus_impressions_pricing.buyer_charges.is_dw"))
        )
        .and(
          col("log_impbus_impressions_pricing.buyer_charges.is_dw") === lit(1)
        )
        .and(
          (imp_type =!= lit(1))
            .and(imp_type =!= lit(2))
            .and(imp_type =!= lit(3))
            .and(imp_type =!= lit(8))
            .and(imp_type =!= lit(5))
            .and(imp_type =!= lit(9))
        )
        .and(is_not_null(col("seller_member_id").cast(IntegerType)))
        .and(col("seller_member_id").cast(IntegerType) =!= lit(0))
        .and(
          is_not_null(
            f_preempt_over_impression_non_zero_explicit(
              is_not_null(
                col("log_impbus_preempt.buyer_member_id").cast(IntegerType)
              ),
              col("log_impbus_impressions.buyer_member_id").cast(IntegerType),
              col("log_impbus_preempt.buyer_member_id").cast(IntegerType)
            )
          )
        )
        .and(
          f_preempt_over_impression_non_zero_explicit(
            is_not_null(
              col("log_impbus_preempt.buyer_member_id").cast(IntegerType)
            ),
            col("log_impbus_impressions.buyer_member_id").cast(IntegerType),
            col("log_impbus_preempt.buyer_member_id").cast(IntegerType)
          ) =!= lit(0)
        )
        .and(
          col("seller_member_id")
            .cast(IntegerType) =!= f_preempt_over_impression_non_zero_explicit(
            is_not_null(
              col("log_impbus_preempt.buyer_member_id").cast(IntegerType)
            ),
            col("log_impbus_impressions.buyer_member_id").cast(IntegerType),
            col("log_impbus_preempt.buyer_member_id").cast(IntegerType)
          )
        ),
      lit(1)
    ).otherwise(is_dw_normalized).cast(IntegerType)
    campaign_group_type_id = when(
      is_not_null(col("campaign_id").cast(IntegerType))
        .and(col("campaign_id").cast(IntegerType) =!= lit(0))
        .and(
          is_not_null(sup_bidder_campaign_lookup).and(
            is_not_null(
              sup_bidder_campaign_lookup.getField("campaign_group_type_id")
            )
          )
        )
        .and(_f_is_buy_side === lit(1)),
      sup_bidder_campaign_lookup.getField("campaign_group_type_id")
    ).otherwise(campaign_group_type_id).cast(IntegerType)
    seller_trx_event_id = f_transaction_event_87047.cast(IntegerType)
    buyer_trx_event_id = f_transaction_event_87057.cast(IntegerType)
    seller_trx_event_type_id =
      f_transaction_event_type_id_87067.cast(IntegerType)
    buyer_trx_event_type_id =
      f_transaction_event_type_id_87077.cast(IntegerType)
    should_process_views = f_should_process_views(col("log_dw_view"),
                                                  seller_trx_event_id,
                                                  buyer_trx_event_id
    ).cast(IntegerType)
    v_transaction_event_pricing = f_get_transaction_event_pricing(
      col("log_impbus_impressions_pricing.impression_event_pricing"),
      col("log_impbus_auction_event.auction_event_pricing"),
      col("log_impbus_impressions_pricing.buyer_charges"),
      col("log_impbus_impressions_pricing.seller_charges"),
      should_process_views
    )
    buyer_charges_pricing_terms = when(
      is_not_null(
        f_get_transaction_event_pricing(
          col("log_impbus_impressions_pricing.impression_event_pricing"),
          col("log_impbus_auction_event.auction_event_pricing"),
          col("log_impbus_impressions_pricing.buyer_charges"),
          col("log_impbus_impressions_pricing.seller_charges"),
          f_should_process_views(col("log_dw_view"),
                                 f_transaction_event_87047,
                                 f_transaction_event_87057
          )
        ).getField("buyer_charges").getField("pricing_terms")
      ).cast(BooleanType),
      v_transaction_event_pricing
        .getField("buyer_charges")
        .getField("pricing_terms")
    ).otherwise(buyer_charges_pricing_terms)
    seller_charges_pricing_terms = when(
      is_not_null(
        f_get_transaction_event_pricing(
          col("log_impbus_impressions_pricing.impression_event_pricing"),
          col("log_impbus_auction_event.auction_event_pricing"),
          col("log_impbus_impressions_pricing.buyer_charges"),
          col("log_impbus_impressions_pricing.seller_charges"),
          f_should_process_views(col("log_dw_view"),
                                 f_transaction_event_87047,
                                 f_transaction_event_87057
          )
        ).getField("seller_charges").getField("pricing_terms")
      ).cast(BooleanType),
      v_transaction_event_pricing
        .getField("seller_charges")
        .getField("pricing_terms")
    ).otherwise(seller_charges_pricing_terms)
    has_seller_transacted = _f_has_transacted.cast(BooleanType)
    has_seller_transacted = when(_f_has_transacted.cast(BooleanType) === lit(0),
                                 v_transaction_event_pricing
                                   .getField("seller_transacted")
                                   .cast(BooleanType)
    ).otherwise(has_seller_transacted.cast(BooleanType))
    has_buyer_transacted = _f_has_transacted.cast(BooleanType)
    has_buyer_transacted = when(
      _f_has_transacted.cast(BooleanType) === lit(0),
      coalesce(v_transaction_event_pricing
                 .getField("buyer_transacted")
                 .cast(BooleanType),
               lit(0).cast(BooleanType)
      )
    ).otherwise(has_buyer_transacted)
    payment_type_normalized = when(
      _f_is_buy_side === lit(1),
      coalesce(col("log_dw_bid.payment_type").cast(IntegerType),
               col("log_impbus_impressions.payment_type").cast(IntegerType),
               lit(999)
      )
    ).otherwise(
        coalesce(col("log_impbus_impressions.payment_type").cast(IntegerType),
                 lit(999)
        )
      )
      .cast(IntegerType)
    payment_type_normalized = when(
      (_f_is_buy_side === lit(1))
        .and(is_not_null(col("buyer_member_id").cast(IntegerType)))
        .and(is_not_null(col("log_dw_bid.member_id").cast(IntegerType)))
        .and(imp_type.isin(lit(6), lit(7)))
        .and(
          col("buyer_member_id")
            .cast(IntegerType) === col("log_dw_bid.member_id").cast(IntegerType)
        )
        .and(f_transaction_event_87047 =!= lit(0))
        .and(f_transaction_event_87057 =!= lit(0))
        .and(f_transaction_event_87047 =!= f_transaction_event_87057), {
        payment_type_normalized = when(buyer_trx_event_id === lit(1), lit(0))
          .otherwise(payment_type_normalized)
          .cast(IntegerType)
        payment_type_normalized = when(buyer_trx_event_id.isin(lit(16),
                                                               lit(15),
                                                               lit(14),
                                                               lit(13),
                                                               lit(9),
                                                               lit(8),
                                                               lit(7),
                                                               lit(6),
                                                               lit(2),
                                                               lit(5)
                                       ),
                                       lit(5)
        ).otherwise(payment_type_normalized).cast(IntegerType)
        payment_type_normalized
      }
    ).otherwise(payment_type_normalized).cast(IntegerType)
    revenue_type_normalized = when(
      _f_is_buy_side === lit(1),
      coalesce(col("log_dw_bid.revenue_type").cast(IntegerType), lit(0))
    ).when(is_not_null(col("log_dw_bid.payment_type").cast(IntegerType))
              .and(col("log_dw_bid.payment_type").cast(IntegerType) === lit(1)),
            lit(3)
      )
      .when(is_not_null(col("log_dw_bid.payment_type").cast(IntegerType))
              .and(col("log_dw_bid.payment_type").cast(IntegerType) === lit(2)),
            lit(4)
      )
      .otherwise(lit(0))
      .cast(IntegerType)
    revenue_type_normalized = when(
      (_f_is_buy_side === lit(1))
        .and(is_not_null(col("log_dw_bid.log_type").cast(IntegerType)))
        .and(imp_type.isin(lit(6), lit(7)))
        .and(col("log_dw_bid.log_type").cast(IntegerType) === lit(2)), {
        revenue_type_normalized = when(
          (seller_trx_event_id =!= lit(0))
            .and(buyer_trx_event_id =!= lit(0))
            .and(seller_trx_event_id =!= buyer_trx_event_id), {
            revenue_type_normalized = when(seller_trx_event_id === lit(1),
                                           lit(0)
            ).otherwise(revenue_type_normalized).cast(IntegerType)
            revenue_type_normalized = when(seller_trx_event_id.isin(lit(16),
                                                                    lit(15),
                                                                    lit(14),
                                                                    lit(13),
                                                                    lit(9),
                                                                    lit(8),
                                                                    lit(7),
                                                                    lit(6),
                                                                    lit(2),
                                                                    lit(5)
                                           ),
                                           lit(9)
            ).otherwise(revenue_type_normalized).cast(IntegerType)
            revenue_type_normalized
          }
        ).otherwise(revenue_type_normalized).cast(IntegerType)
        revenue_type_normalized = when(
          (seller_trx_event_id === lit(2)).and(buyer_trx_event_id === lit(2)),
          lit(9)
        ).otherwise(revenue_type_normalized).cast(IntegerType)
        revenue_type_normalized
      }
    ).otherwise(revenue_type_normalized).cast(IntegerType)
    buyer_charges_pricing_terms = f_zero_pricing_term_amount_if_non_cpm(
      imp_type,
      payment_type_normalized,
      revenue_type_normalized,
      buyer_charges_pricing_terms
    )
    seller_charges_pricing_terms = f_zero_pricing_term_amount_if_non_cpm(
      imp_type,
      payment_type_normalized,
      revenue_type_normalized,
      seller_charges_pricing_terms
    )
    view_detection_enabled = _f_view_detection_enabled.cast(IntegerType)
    view_measurable =
      f_view_measurable(view_detection_enabled,
                        col("log_impbus_view.view_result").cast(IntegerType)
      ).cast(IntegerType)
    viewable = f_viewable(view_measurable,
                          col("log_impbus_view.view_result").cast(IntegerType)
    ).cast(IntegerType)
    view_non_measurable_reason = f_view_non_measurable_reason(
      view_detection_enabled,
      col("log_impbus_view.view_result").cast(IntegerType)
    ).cast(IntegerType)
    viewdef_definition_id = _f_viewdef_definition_id.cast(IntegerType)
    viewdef_viewable = f_viewdef_viewable(
      viewdef_definition_id,
      view_measurable,
      col("log_impbus_view.viewdef_view_result").cast(IntegerType)
    ).cast(IntegerType)
    view_result =
      f_view_result(view_detection_enabled,
                    col("log_impbus_view.view_result").cast(IntegerType)
      ).cast(IntegerType)
    is_budget_table_imp_type = when(
      (col("imp_type").cast(IntegerType) === lit(5))
        .or(col("imp_type").cast(IntegerType) === lit(7))
        .or(col("imp_type").cast(IntegerType) === lit(9))
        .or(col("imp_type").cast(IntegerType) === lit(11))
        .or(
          is_not_null(col("log_dw_bid_deal"))
            .and(col("imp_type").cast(IntegerType) === lit(6))
        ),
      lit(1)
    ).otherwise(is_budget_table_imp_type).cast(IntegerType)
    is_not_roadblock_secondary =
      when(isnull(col("log_dw_bid.impression_transaction_type")).or(
             col("log_dw_bid.impression_transaction_type")
               .cast(IntegerType) =!= lit(2)
           ),
           lit(1)
      ).otherwise(is_not_roadblock_secondary).cast(IntegerType)
    is_external_imp_type = when(col("imp_type").cast(IntegerType) === lit(9),
                                lit(1)
    ).otherwise(is_external_imp_type).cast(IntegerType)
    is_not_video_imp = when(
      isnull(col("log_dw_bid.revenue_event_type_id")).or(
        is_not_null(col("log_dw_bid.revenue_event_type_id").cast(IntegerType))
          .and(
            col("log_dw_bid.revenue_event_type_id").cast(IntegerType) =!= lit(5)
          )
      ),
      lit(1)
    ).otherwise(is_not_video_imp).cast(IntegerType)
    imps_for_budget_caps_pacing = (is_budget_table_imp_type === lit(1))
      .and(
        (is_not_roadblock_secondary === lit(1))
          .or(is_external_imp_type === lit(1))
      )
      .and(is_not_video_imp === lit(1))
      .cast(IntegerType)
    commission_cpm =
      coalesce(col("log_dw_bid.commission_cpm"), lit(0)).cast(DoubleType)
    commission_revshare =
      coalesce(col("log_dw_bid.commission_revshare"), lit(0)).cast(DoubleType)
    serving_fees_cpm =
      coalesce(col("log_dw_bid.serving_fees_cpm"), lit(0)).cast(DoubleType)
    serving_fees_revshare =
      coalesce(col("log_dw_bid.serving_fees_revshare"), lit(0)).cast(DoubleType)
    media_buy_cost = when(
      imp_type === lit(6),
      when(is_not_null(col("log_impbus_impressions.media_buy_cost")).cast(
             BooleanType
           ),
           math_min(col("log_impbus_impressions.media_buy_cost"), lit(999.0d))
      ).otherwise(media_buy_cost)
    ).when(is_not_null(col("log_dw_bid.media_buy_cost")).cast(BooleanType),
            math_min(col("log_dw_bid.media_buy_cost"), lit(999.0d))
      )
      .otherwise(media_buy_cost)
      .cast(DoubleType)
    media_buy_rev_share_pct =
      coalesce(col("log_impbus_impressions.media_buy_rev_share_pct"), lit(0))
        .cast(DoubleType)
    booked_revenue_dollars =
      when(is_not_null(col("log_dw_bid.revenue_info.booked_revenue_dollars"))
             .cast(BooleanType),
           col("log_dw_bid.revenue_info.booked_revenue_dollars")
      ).otherwise(booked_revenue_dollars).cast(DoubleType)
    booked_revenue_dollars = when(
      is_not_null(col("log_dw_view.revenue_info.booked_revenue_dollars")).cast(
        BooleanType
      ),
      booked_revenue_dollars + col(
        "log_dw_view.revenue_info.booked_revenue_dollars"
      )
    ).otherwise(booked_revenue_dollars).cast(DoubleType)
    booked_revenue_adv_curr =
      when(is_not_null(col("log_dw_bid.revenue_info.booked_revenue_adv_curr"))
             .cast(BooleanType),
           col("log_dw_bid.revenue_info.booked_revenue_adv_curr")
      ).otherwise(booked_revenue_adv_curr).cast(DoubleType)
    booked_revenue_adv_curr = when(
      is_not_null(col("log_dw_view.revenue_info.booked_revenue_adv_curr")).cast(
        BooleanType
      ),
      booked_revenue_adv_curr + col(
        "log_dw_view.revenue_info.booked_revenue_adv_curr"
      )
    ).otherwise(booked_revenue_adv_curr).cast(DoubleType)
    v_pricing_term = when(
      (f_is_non_cpm_payment_or_payment(imp_type,
                                       payment_type_normalized,
                                       revenue_type_normalized
      ) === lit(0))
        .or(is_dw_normalized === lit(1))
        .or(
          f_should_process_views(col("log_dw_view"),
                                 f_transaction_event_87047,
                                 f_transaction_event_87057
          ) === lit(1)
        ),
      f_get_pricing_term(lit(1), buyer_charges_pricing_terms)
    ).otherwise(v_pricing_term)
    auction_service_fees = when(
      (f_is_non_cpm_payment_or_payment(imp_type,
                                       payment_type_normalized,
                                       revenue_type_normalized
      ) === lit(0))
        .or(is_dw_normalized === lit(1))
        .or(
          f_should_process_views(col("log_dw_view"),
                                 f_transaction_event_87047,
                                 f_transaction_event_87057
          ) === lit(1)
        )
        .and(
          is_not_null(
            f_get_pricing_term(lit(1), buyer_charges_pricing_terms)
              .getField("amount")
          ).and(
            f_get_pricing_term(lit(1), buyer_charges_pricing_terms)
              .getField("amount") > lit(0)
          )
        )
        .and(
          isnull(
            f_get_pricing_term(lit(1), buyer_charges_pricing_terms)
              .getField("is_deduction")
          ).or(
            is_not_null(
              f_get_pricing_term(lit(1), buyer_charges_pricing_terms)
                .getField("is_deduction")
            ).and(
              f_get_pricing_term(lit(1), buyer_charges_pricing_terms)
                .getField("is_deduction") === lit(0)
            )
          )
        ),
      f_get_pricing_term(lit(1), buyer_charges_pricing_terms).getField(
        "amount"
      ) / lit(1000)
    ).otherwise(auction_service_fees).cast(DoubleType)
    v_pricing_term = when(
      (f_is_non_cpm_payment_or_payment(imp_type,
                                       payment_type_normalized,
                                       revenue_type_normalized
      ) === lit(0))
        .or(is_dw_normalized === lit(1))
        .or(
          f_should_process_views(col("log_dw_view"),
                                 f_transaction_event_87047,
                                 f_transaction_event_87057
          ) === lit(1)
        ),
      f_get_pricing_term(lit(1), buyer_charges_pricing_terms)
    ).otherwise(v_pricing_term)
    auction_service_deduction = when(
      (f_is_non_cpm_payment_or_payment(imp_type,
                                       payment_type_normalized,
                                       revenue_type_normalized
      ) === lit(0))
        .or(is_dw_normalized === lit(1))
        .or(
          f_should_process_views(col("log_dw_view"),
                                 f_transaction_event_87047,
                                 f_transaction_event_87057
          ) === lit(1)
        )
        .and(
          is_not_null(
            f_get_pricing_term(lit(1), buyer_charges_pricing_terms)
              .getField("amount")
          ).and(
            f_get_pricing_term(lit(1), buyer_charges_pricing_terms)
              .getField("amount") > lit(0)
          )
        )
        .and(
          is_not_null(
            f_get_pricing_term(lit(1), buyer_charges_pricing_terms)
              .getField("is_deduction")
          ).and(
            f_get_pricing_term(lit(1), buyer_charges_pricing_terms)
              .getField("is_deduction") === lit(1)
          )
        ),
      f_get_pricing_term(lit(1), buyer_charges_pricing_terms).getField(
        "amount"
      ) / lit(1000)
    ).otherwise(auction_service_deduction).cast(DoubleType)
    v_pricing_term = when(
      (f_is_non_cpm_payment_or_payment(imp_type,
                                       payment_type_normalized,
                                       revenue_type_normalized
      ) === lit(0))
        .or(is_dw_normalized === lit(1))
        .or(
          f_should_process_views(col("log_dw_view"),
                                 f_transaction_event_87047,
                                 f_transaction_event_87057
          ) === lit(1)
        ),
      f_get_pricing_term(lit(11), buyer_charges_pricing_terms)
    ).otherwise(v_pricing_term)
    creative_overage_fees = when(
      (f_is_non_cpm_payment_or_payment(imp_type,
                                       payment_type_normalized,
                                       revenue_type_normalized
      ) === lit(0))
        .or(is_dw_normalized === lit(1))
        .or(
          f_should_process_views(col("log_dw_view"),
                                 f_transaction_event_87047,
                                 f_transaction_event_87057
          ) === lit(1)
        ),
      when(
        is_not_null(
          f_get_pricing_term(lit(11), buyer_charges_pricing_terms)
            .getField("amount")
        ).and(
          f_get_pricing_term(lit(11), buyer_charges_pricing_terms)
            .getField("amount") > lit(0)
        ),
        f_get_pricing_term(lit(11), buyer_charges_pricing_terms).getField(
          "amount"
        ) / lit(1000)
      ).otherwise(creative_overage_fees)
    ).otherwise(creative_overage_fees).cast(DoubleType)
    v_pricing_term = when(
      (f_is_non_cpm_payment_or_payment(imp_type,
                                       payment_type_normalized,
                                       revenue_type_normalized
      ) === lit(0))
        .or(is_dw_normalized === lit(1))
        .or(
          f_should_process_views(col("log_dw_view"),
                                 f_transaction_event_87047,
                                 f_transaction_event_87057
          ) === lit(1)
        ),
      f_get_pricing_term(lit(51), buyer_charges_pricing_terms)
    ).otherwise(v_pricing_term)
    discrepancy_allowance = when(
      (f_is_non_cpm_payment_or_payment(imp_type,
                                       payment_type_normalized,
                                       revenue_type_normalized
      ) === lit(0))
        .or(is_dw_normalized === lit(1))
        .or(
          f_should_process_views(col("log_dw_view"),
                                 f_transaction_event_87047,
                                 f_transaction_event_87057
          ) === lit(1)
        ),
      when(
        is_not_null(
          f_get_pricing_term(lit(51), buyer_charges_pricing_terms)
            .getField("amount")
        ).and(
          f_get_pricing_term(lit(51), buyer_charges_pricing_terms)
            .getField("amount") > lit(0)
        ),
        f_get_pricing_term(lit(51), buyer_charges_pricing_terms).getField(
          "amount"
        ) / lit(1000)
      ).otherwise(discrepancy_allowance)
    ).otherwise(discrepancy_allowance).cast(DoubleType)
    buyer_spend_microcents = when(
      isnull(col("log_dw_bid.payment_type")).or(
        is_not_null(col("log_dw_bid.payment_type").cast(IntegerType))
          .and(col("log_dw_bid.payment_type").cast(IntegerType) =!= lit(1))
          .and(col("log_dw_bid.payment_type").cast(IntegerType) =!= lit(2))
      ),
      coalesce(
        v_transaction_event_pricing.getField("gross_payment_value_microcents"),
        lit(0)
      )
    ).otherwise(buyer_spend_microcents).cast(DoubleType)
    seller_revenue_microcents = when(
      isnull(col("log_dw_bid.payment_type")).or(
        is_not_null(col("log_dw_bid.payment_type").cast(IntegerType))
          .and(col("log_dw_bid.payment_type").cast(IntegerType) =!= lit(1))
          .and(col("log_dw_bid.payment_type").cast(IntegerType) =!= lit(2))
      ),
      coalesce(
        v_transaction_event_pricing.getField("seller_revenue_microcents"),
        lit(0)
      )
    ).otherwise(seller_revenue_microcents).cast(DoubleType)
    buyer_spend_cpm = (buyer_spend_microcents / lit(100000)).cast(DoubleType)
    seller_revenue_cpm =
      (seller_revenue_microcents / lit(100000)).cast(DoubleType)
    media_cost_dollars_cpm = f_get_media_cost_dollars_cpm(
      imp_type,
      payment_type_normalized,
      revenue_type_normalized,
      media_buy_cost,
      media_buy_rev_share_pct,
      seller_revenue_cpm,
      buyer_spend_cpm,
      booked_revenue_dollars,
      commission_cpm,
      commission_revshare,
      serving_fees_cpm,
      serving_fees_revshare,
      col("log_impbus_impressions.apply_cost_on_default").cast(IntegerType),
      lit(0.0d).cast(DoubleType)
    ).cast(DoubleType)
    seller_deduction_term_id_1 = when(
      (f_is_non_cpm_payment_or_payment(imp_type,
                                       payment_type_normalized,
                                       revenue_type_normalized
      ) === lit(0))
        .and(
          is_not_null(
            f_get_pricing_term(lit(1), seller_charges_pricing_terms)
              .getField("amount")
          ).cast(BooleanType)
        )
        .and(
          is_not_null(
            f_get_pricing_term(lit(1), seller_charges_pricing_terms)
              .getField("is_deduction")
          ).and(
            f_get_pricing_term(lit(1), seller_charges_pricing_terms)
              .getField("is_deduction") === lit(1)
          )
        ),
      f_get_pricing_term(lit(1), seller_charges_pricing_terms).getField(
        "amount"
      ) / lit(1000)
    ).otherwise(seller_deduction_term_id_1).cast(DoubleType)
    seller_deduction_term_id_74 = when(
      (f_is_non_cpm_payment_or_payment(imp_type,
                                       payment_type_normalized,
                                       revenue_type_normalized
      ) === lit(0))
        .and(
          is_not_null(
            f_get_pricing_term(lit(74), seller_charges_pricing_terms)
              .getField("amount")
          ).cast(BooleanType)
        )
        .and(
          is_not_null(
            f_get_pricing_term(lit(74), seller_charges_pricing_terms)
              .getField("is_deduction")
          ).and(
            f_get_pricing_term(lit(74), seller_charges_pricing_terms)
              .getField("is_deduction") === lit(1)
          )
        ),
      f_get_pricing_term(lit(74), seller_charges_pricing_terms).getField(
        "amount"
      ) / lit(1000)
    ).otherwise(seller_deduction_term_id_74).cast(DoubleType)
    seller_deduction = when(
      f_is_non_cpm_payment_or_payment(imp_type,
                                      payment_type_normalized,
                                      revenue_type_normalized
      ) === lit(0),
      math_max(seller_deduction_term_id_1 + seller_deduction_term_id_74, lit(0))
    ).otherwise(seller_deduction).cast(DoubleType)
    deal_type = f_preempt_over_impression_88439.cast(IntegerType)
    deal_id = when(
      is_not_null(col("log_impbus_preempt.deal_id").cast(IntegerType))
        .and(col("log_impbus_preempt.deal_id").cast(IntegerType) > lit(0)),
      col("log_impbus_preempt.deal_id").cast(IntegerType)
    ).otherwise(deal_id).cast(IntegerType)
    is_curated = when(
      is_not_null(col("log_impbus_preempt.curated_deal_id").cast(IntegerType))
        .and(
          col("log_impbus_preempt.curated_deal_id").cast(IntegerType) > lit(0)
        ),
      lit(1)
    ).otherwise(is_curated).cast(IntegerType)
    curator_member_id = when(
      is_not_null(col("log_impbus_preempt.curated_deal_id").cast(IntegerType))
        .and(
          col("log_impbus_preempt.curated_deal_id").cast(IntegerType) > lit(0)
        ),
      when(is_not_null(sup_common_deal_lookup).cast(BooleanType),
           sup_common_deal_lookup.getField("member_id")
      ).otherwise(curator_member_id)
    ).otherwise(curator_member_id).cast(IntegerType)
    deal_id = when(
      is_not_null(col("log_impbus_preempt.curated_deal_id").cast(IntegerType))
        .and(
          col("log_impbus_preempt.curated_deal_id").cast(IntegerType) > lit(0)
        )
        .and(
          (imp_type === lit(7))
            .or((is_dw_normalized === lit(1)).and(imp_type === lit(6)))
        )
        .and(imp_type === lit(7)),
      col("log_impbus_preempt.curated_deal_id").cast(IntegerType)
    ).otherwise(deal_id).cast(IntegerType)
    deal_type = when(
      is_not_null(col("log_impbus_preempt.curated_deal_id").cast(IntegerType))
        .and(
          col("log_impbus_preempt.curated_deal_id").cast(IntegerType) > lit(0)
        )
        .and(
          (imp_type === lit(7))
            .or((is_dw_normalized === lit(1)).and(imp_type === lit(6)))
        )
        .and(imp_type === lit(7)),
      lit(5)
    ).otherwise(deal_type).cast(IntegerType)
    creative_id = when(
      isnull(col("log_dw_bid"))
        .or(imp_type === lit(3))
        .or(imp_type === lit(4))
        .or(
          is_not_null(col("log_dw_bid.creative_id").cast(IntegerType))
            .and(col("log_dw_bid.creative_id").cast(IntegerType) === lit(0))
        ),
      f_preempt_over_impression_88639
    ).otherwise(col("log_dw_bid.creative_id").cast(IntegerType))
      .cast(IntegerType)
    advertiser_id = col("advertiser_id").cast(IntegerType)
    campaign_group_id = col("campaign_group_id").cast(IntegerType)
    insertion_order_id = col("insertion_order_id").cast(IntegerType)
    campaign_id = col("campaign_id").cast(IntegerType)
    campaign_id = when(
      is_not_null(col("log_dw_bid.campaign_id").cast(IntegerType))
        .and(col("log_dw_bid.campaign_id").cast(IntegerType) =!= lit(0)),
      col("log_dw_bid.campaign_id").cast(IntegerType)
    ).otherwise(campaign_id).cast(IntegerType)
    member_sales_tax_rate_pct = when(
      is_not_null(col("log_dw_bid.data_costs")).cast(BooleanType),
      coalesce(lookup("sup_bidder_member_sales_tax_rate", buyer_member_id)
                 .getField("sales_tax_rate_pct"),
               lit(0)
      )
    ).otherwise(member_sales_tax_rate_pct).cast(DoubleType)
    data_costs = when(
      is_not_null(col("log_dw_bid.data_costs")).cast(BooleanType), {
        data_costs = col("log_dw_bid.data_costs")
        data_costs = f_update_data_costs(
          col("log_dw_bid.data_costs"),
          imp_type,
          lit(0),
          payment_type_normalized,
          media_cost_dollars_cpm,
          coalesce(lookup("sup_bidder_member_sales_tax_rate", buyer_member_id)
                     .getField("sales_tax_rate_pct"),
                   lit(0)
          )
        )
        data_costs
      }
    ).otherwise(data_costs)
    data_costs_deal = when(
      f_transaction_event_87057 === lit(1),
      when(
        is_not_null(col("log_dw_bid_deal.data_costs")).cast(BooleanType), {
          data_costs_deal = col("log_dw_bid_deal.data_costs")
          data_costs_deal = f_update_data_costs_deal(
            col("log_dw_bid_deal.data_costs"),
            coalesce(
              lookup("sup_bidder_member_sales_tax_rate",
                     coalesce(sup_common_deal_lookup_2.getField("member_id"),
                              lit(0)
                     )
              ).getField("sales_tax_rate_pct"),
              lit(0)
            )
          )
          data_costs_deal
        }
      ).otherwise(data_costs_deal)
    ).otherwise(data_costs_deal)
    media_buy_cost = when(
      _f_is_buy_side === lit(0),
      when(
        isnull(col("log_dw_bid")).and(
          (f_is_error_imp(imp_type) === lit(1)).or(
            is_not_null(col("is_dw").cast(IntegerType))
              .and(col("is_dw").cast(IntegerType) === lit(1))
              .and(imp_type.isin(lit(2), lit(4)))
          )
        ),
        lit(0)
      ).otherwise(media_buy_cost)
    ).otherwise(media_buy_cost).cast(DoubleType)
    is_placeholder_bid = when(
      _f_is_buy_side === lit(0),
      when(
        isnull(col("log_dw_bid")).and(
          (f_is_error_imp(imp_type) === lit(1)).or(
            is_not_null(col("is_dw").cast(IntegerType))
              .and(col("is_dw").cast(IntegerType) === lit(1))
              .and(imp_type.isin(lit(2), lit(4)))
          )
        ),
        lit(1)
      ).otherwise(is_placeholder_bid)
    ).otherwise(is_placeholder_bid).cast(IntegerType)
    advertiser_id = when(
      _f_is_buy_side === lit(0),
      when(
        isnull(col("log_dw_bid")).and(
          (f_is_error_imp(imp_type) === lit(1)).or(
            is_not_null(col("is_dw").cast(IntegerType))
              .and(col("is_dw").cast(IntegerType) === lit(1))
              .and(imp_type.isin(lit(2), lit(4)))
          )
        ),
        lit(null).cast(IntegerType)
      ).when(
          f_is_default_or_error_imp(imp_type) === lit(1), {
            advertiser_id = when(f_is_default_or_error_imp(imp_type) === lit(1),
                                 lit(null).cast(IntegerType)
            ).otherwise(advertiser_id).cast(IntegerType)
            advertiser_id = when(f_is_error_imp(imp_type) === lit(1),
                                 lit(null).cast(IntegerType)
            ).otherwise(advertiser_id).cast(IntegerType)
            advertiser_id
          }
        )
        .when(not(imp_type.isin(lit(5), lit(9), lit(2))),
              lit(null).cast(IntegerType)
        )
        .otherwise(advertiser_id)
    ).otherwise(advertiser_id).cast(IntegerType)
    serving_fees_revshare = when(
      _f_is_buy_side === lit(0),
      when(
        not(
          isnull(col("log_dw_bid")).and(
            (f_is_error_imp(imp_type) === lit(1)).or(
              is_not_null(col("is_dw").cast(IntegerType))
                .and(col("is_dw").cast(IntegerType) === lit(1))
                .and(imp_type.isin(lit(2), lit(4)))
            )
          )
        ).and(not(f_is_default_or_error_imp(imp_type) === lit(1)))
          .and(not(imp_type.isin(lit(5), lit(9), lit(2)))),
        lit(null).cast(DoubleType)
      ).otherwise(serving_fees_revshare)
    ).otherwise(serving_fees_revshare).cast(DoubleType)
    split_id = when(
      _f_is_buy_side === lit(0),
      when(
        not(
          isnull(col("log_dw_bid")).and(
            (f_is_error_imp(imp_type) === lit(1)).or(
              is_not_null(col("is_dw").cast(IntegerType))
                .and(col("is_dw").cast(IntegerType) === lit(1))
                .and(imp_type.isin(lit(2), lit(4)))
            )
          )
        ).and(not(f_is_default_or_error_imp(imp_type) === lit(1)))
          .and(not(imp_type.isin(lit(5), lit(9), lit(2)))),
        lit(0)
      ).otherwise(split_id)
    ).otherwise(split_id).cast(IntegerType)
    booked_revenue_dollars = when(
      _f_is_buy_side === lit(0),
      when(
        not(
          isnull(col("log_dw_bid")).and(
            (f_is_error_imp(imp_type) === lit(1)).or(
              is_not_null(col("is_dw").cast(IntegerType))
                .and(col("is_dw").cast(IntegerType) === lit(1))
                .and(imp_type.isin(lit(2), lit(4)))
            )
          )
        ).and(not(f_is_default_or_error_imp(imp_type) === lit(1)))
          .and(not(imp_type.isin(lit(5), lit(9), lit(2)))),
        lit(0)
      ).otherwise(booked_revenue_dollars)
    ).otherwise(booked_revenue_dollars).cast(DoubleType)
    serving_fees_cpm = when(
      _f_is_buy_side === lit(0),
      when(
        not(
          isnull(col("log_dw_bid")).and(
            (f_is_error_imp(imp_type) === lit(1)).or(
              is_not_null(col("is_dw").cast(IntegerType))
                .and(col("is_dw").cast(IntegerType) === lit(1))
                .and(imp_type.isin(lit(2), lit(4)))
            )
          )
        ).and(not(f_is_default_or_error_imp(imp_type) === lit(1)))
          .and(not(imp_type.isin(lit(5), lit(9), lit(2)))),
        lit(null).cast(DoubleType)
      ).otherwise(serving_fees_cpm)
    ).otherwise(serving_fees_cpm).cast(DoubleType)
    insertion_order_id = when(
      _f_is_buy_side === lit(0),
      when(
        not(
          isnull(col("log_dw_bid")).and(
            (f_is_error_imp(imp_type) === lit(1)).or(
              is_not_null(col("is_dw").cast(IntegerType))
                .and(col("is_dw").cast(IntegerType) === lit(1))
                .and(imp_type.isin(lit(2), lit(4)))
            )
          )
        ).and(not(f_is_default_or_error_imp(imp_type) === lit(1)))
          .and(not(imp_type.isin(lit(5), lit(9), lit(2)))),
        lit(null).cast(IntegerType)
      ).otherwise(insertion_order_id)
    ).otherwise(insertion_order_id).cast(IntegerType)
    campaign_group_id = when(
      _f_is_buy_side === lit(0),
      when(
        isnull(col("log_dw_bid")).and(
          (f_is_error_imp(imp_type) === lit(1)).or(
            is_not_null(col("is_dw").cast(IntegerType))
              .and(col("is_dw").cast(IntegerType) === lit(1))
              .and(imp_type.isin(lit(2), lit(4)))
          )
        ),
        lit(null).cast(IntegerType)
      ).when(
          f_is_default_or_error_imp(imp_type) === lit(1), {
            campaign_group_id =
              when(f_is_default_or_error_imp(imp_type) === lit(1),
                   lit(null).cast(IntegerType)
              ).otherwise(campaign_group_id).cast(IntegerType)
            campaign_group_id = when(f_is_error_imp(imp_type) === lit(1),
                                     lit(null).cast(IntegerType)
            ).otherwise(campaign_group_id).cast(IntegerType)
            campaign_group_id
          }
        )
        .when(not(imp_type.isin(lit(5), lit(9), lit(2))),
              lit(null).cast(IntegerType)
        )
        .otherwise(campaign_group_id)
    ).otherwise(campaign_group_id).cast(IntegerType)
    commission_cpm = when(
      _f_is_buy_side === lit(0),
      when(
        not(
          isnull(col("log_dw_bid")).and(
            (f_is_error_imp(imp_type) === lit(1)).or(
              is_not_null(col("is_dw").cast(IntegerType))
                .and(col("is_dw").cast(IntegerType) === lit(1))
                .and(imp_type.isin(lit(2), lit(4)))
            )
          )
        ).and(not(f_is_default_or_error_imp(imp_type) === lit(1)))
          .and(not(imp_type.isin(lit(5), lit(9), lit(2)))),
        lit(0)
      ).otherwise(commission_cpm)
    ).otherwise(commission_cpm).cast(DoubleType)
    commission_revshare = when(
      _f_is_buy_side === lit(0),
      when(
        not(
          isnull(col("log_dw_bid")).and(
            (f_is_error_imp(imp_type) === lit(1)).or(
              is_not_null(col("is_dw").cast(IntegerType))
                .and(col("is_dw").cast(IntegerType) === lit(1))
                .and(imp_type.isin(lit(2), lit(4)))
            )
          )
        ).and(not(f_is_default_or_error_imp(imp_type) === lit(1)))
          .and(not(imp_type.isin(lit(5), lit(9), lit(2)))),
        lit(null).cast(DoubleType)
      ).otherwise(commission_revshare)
    ).otherwise(commission_revshare).cast(DoubleType)
    campaign_id = when(
      _f_is_buy_side === lit(0),
      when(
        not(
          isnull(col("log_dw_bid")).and(
            (f_is_error_imp(imp_type) === lit(1)).or(
              is_not_null(col("is_dw").cast(IntegerType))
                .and(col("is_dw").cast(IntegerType) === lit(1))
                .and(imp_type.isin(lit(2), lit(4)))
            )
          )
        ).and(not(f_is_default_or_error_imp(imp_type) === lit(1)))
          .and(not(imp_type.isin(lit(5), lit(9), lit(2)))),
        lit(null).cast(IntegerType)
      ).otherwise(campaign_id)
    ).otherwise(campaign_id).cast(IntegerType)
    revenue_value = when(
      _f_is_buy_side === lit(0),
      when(
        not(
          isnull(col("log_dw_bid")).and(
            (f_is_error_imp(imp_type) === lit(1)).or(
              is_not_null(col("is_dw").cast(IntegerType))
                .and(col("is_dw").cast(IntegerType) === lit(1))
                .and(imp_type.isin(lit(2), lit(4)))
            )
          )
        ).and(not(f_is_default_or_error_imp(imp_type) === lit(1)))
          .and(not(imp_type.isin(lit(5), lit(9), lit(2)))),
        lit(0)
      ).otherwise(revenue_value)
    ).otherwise(revenue_value).cast(DoubleType)
    virtual_log_dw_bid =
      f_create_agg_dw_impressions_virtual_log_dw_bid_10949800(
        virtual_log_dw_bid,
        _f_is_buy_side,
        col("log_dw_bid"),
        f_is_error_imp(imp_type),
        col("is_dw"),
        imp_type,
        f_is_default_or_error_imp(imp_type),
        col("log_impbus_impressions.payment_type"),
        seller_trx_event_id
      )
    booked_revenue_adv_curr = when(
      _f_is_buy_side === lit(0),
      when(
        not(
          isnull(col("log_dw_bid")).and(
            (f_is_error_imp(imp_type) === lit(1)).or(
              is_not_null(col("is_dw").cast(IntegerType))
                .and(col("is_dw").cast(IntegerType) === lit(1))
                .and(imp_type.isin(lit(2), lit(4)))
            )
          )
        ).and(not(f_is_default_or_error_imp(imp_type) === lit(1)))
          .and(not(imp_type.isin(lit(5), lit(9), lit(2)))),
        lit(0)
      ).otherwise(booked_revenue_adv_curr)
    ).otherwise(booked_revenue_adv_curr).cast(DoubleType)
    campaign_id = when(is_not_null(col("log_dw_bid_deal")).cast(BooleanType),
                       col("campaign_id").cast(IntegerType)
    ).otherwise(campaign_id).cast(IntegerType)
    advertiser_id = when(
      is_not_null(col("log_dw_bid_deal")).cast(BooleanType), {
        advertiser_id = col("advertiser_id").cast(IntegerType)
        advertiser_id = when(
          imp_type === lit(6),
          when(
            is_not_null(col("log_dw_bid_deal.advertiser_id").cast(IntegerType))
              .and(
                col("log_dw_bid_deal.advertiser_id").cast(IntegerType) =!= lit(
                  0
                )
              ),
            col("log_dw_bid_deal.advertiser_id").cast(IntegerType)
          ).otherwise(advertiser_id)
        ).otherwise(advertiser_id).cast(IntegerType)
        advertiser_id
      }
    ).otherwise(advertiser_id).cast(IntegerType)
    campaign_group_id = when(
      is_not_null(col("log_dw_bid_deal")).cast(BooleanType), {
        campaign_group_id = col("campaign_group_id").cast(IntegerType)
        campaign_group_id = when(
          imp_type === lit(6),
          when(
            is_not_null(
              col("log_dw_bid_deal.campaign_group_id").cast(IntegerType)
            ).and(
              col("log_dw_bid_deal.campaign_group_id").cast(
                IntegerType
              ) =!= lit(0)
            ),
            col("log_dw_bid_deal.campaign_group_id").cast(IntegerType)
          ).otherwise(campaign_group_id)
        ).otherwise(campaign_group_id).cast(IntegerType)
        campaign_group_id
      }
    ).otherwise(campaign_group_id).cast(IntegerType)
    insertion_order_id = when(
      is_not_null(col("log_dw_bid_deal")).cast(BooleanType), {
        insertion_order_id = col("insertion_order_id").cast(IntegerType)
        insertion_order_id = when(
          imp_type === lit(6),
          when(
            is_not_null(
              col("log_dw_bid_deal.insertion_order_id").cast(IntegerType)
            ).and(
              col("log_dw_bid_deal.insertion_order_id").cast(
                IntegerType
              ) =!= lit(0)
            ),
            col("log_dw_bid_deal.insertion_order_id").cast(IntegerType)
          ).otherwise(insertion_order_id)
        ).otherwise(insertion_order_id).cast(IntegerType)
        insertion_order_id
      }
    ).otherwise(insertion_order_id).cast(IntegerType)
    flight_id = when(
      is_not_null(col("log_dw_bid_deal"))
        .cast(BooleanType)
        .and(imp_type === lit(6))
        .and(
          is_not_null(
            col("log_dw_bid_deal.campaign_group_budget_interval_id")
              .cast(IntegerType)
          ).and(
            col("log_dw_bid_deal.campaign_group_budget_interval_id")
              .cast(IntegerType) =!= lit(0)
          )
        ),
      col("log_dw_bid_deal.campaign_group_budget_interval_id").cast(IntegerType)
    ).otherwise(flight_id).cast(IntegerType)
    billing_period_id = when(
      is_not_null(col("log_dw_bid_deal"))
        .cast(BooleanType)
        .and(imp_type === lit(6))
        .and(
          is_not_null(
            col("log_dw_bid_deal.insertion_order_budget_interval_id")
              .cast(IntegerType)
          ).and(
            col("log_dw_bid_deal.insertion_order_budget_interval_id")
              .cast(IntegerType) =!= lit(0)
          )
        ),
      col("log_dw_bid_deal.insertion_order_budget_interval_id").cast(
        IntegerType
      )
    ).otherwise(billing_period_id).cast(IntegerType)
    should_zero_seller_revenue = f_should_zero_seller_revenue(
      col("log_dw_bid"),
      col("imp_type").cast(IntegerType),
      revenue_type_normalized,
      col("seller_member_id").cast(IntegerType),
      col(
        "log_impbus_impressions_pricing.impression_event_pricing.seller_revenue_microcents"
      ).cast(LongType)
    ).cast(BooleanType)
    netflix_ppid = when(
      is_not_null(col("seller_member_id").cast(IntegerType)).and(
        (col("seller_member_id").cast(IntegerType) === lit(14007))
          .or(col("seller_member_id").cast(IntegerType) === lit(14120))
          .or(col("seller_member_id").cast(IntegerType) === lit(14136))
      ),
      when(
        is_not_null(_f_find_personal_identifier).and(
          is_not_null(_f_find_personal_identifier.getField("identity_value"))
        ),
        _f_find_personal_identifier.getField("identity_value")
      ).otherwise(netflix_ppid)
    ).otherwise(netflix_ppid)
    f_create_agg_dw_impressions_return_11668614(
      curator_member_id,
      discrepancy_allowance,
      should_process_views,
      buyer_member_id,
      imps_for_budget_caps_pacing,
      imp_type,
      is_curated,
      view_detection_enabled,
      buyer_trx_event_type_id,
      commission_revshare,
      creative_overage_fees,
      viewdef_definition_id,
      virtual_log_dw_bid,
      has_buyer_transacted,
      payment_type_normalized,
      seller_trx_event_id,
      seller_revenue_cpm,
      insertion_order_id,
      revenue_type_normalized,
      billing_period_id,
      viewdef_viewable,
      split_id,
      data_costs_deal,
      f_preempt_over_impression_94298,
      commission_cpm,
      v_transaction_event_pricing,
      buyer_trx_event_id,
      campaign_group_id,
      has_seller_transacted,
      revenue_value,
      netflix_ppid,
      campaign_group_type_id,
      should_zero_seller_revenue,
      view_measurable,
      advertiser_id,
      media_cost_dollars_cpm,
      creative_id,
      f_preempt_over_impression_95337,
      flight_id,
      viewable,
      auction_service_deduction,
      media_buy_cost,
      seller_deduction,
      buyer_spend_cpm,
      seller_trx_event_type_id,
      auction_service_fees,
      serving_fees_cpm,
      data_costs,
      sup_ip_range_lookup_count,
      ttl,
      view_result,
      is_dw_normalized,
      booked_revenue_adv_curr,
      booked_revenue_dollars,
      deal_id,
      _f_is_buy_side,
      deal_type,
      campaign_id,
      is_placeholder_bid,
      serving_fees_revshare,
      view_non_measurable_reason
    )
  }

  def f_router_is_curated(in1: Column): Column = {
    var has_imp:         org.apache.spark.sql.Column = lit(0)
    var has_preempt:     org.apache.spark.sql.Column = lit(0)
    var has_curator_bid: org.apache.spark.sql.Column = lit(0)
    has_imp = when(
      is_not_null(in1.getField("log_impbus_impressions")).cast(BooleanType),
      lit(1)
    ).otherwise(has_imp).cast(IntegerType)
    has_preempt =
      when(is_not_null(in1.getField("log_impbus_preempt")).cast(BooleanType),
           lit(1)
      ).otherwise(has_preempt).cast(IntegerType)
    has_curator_bid =
      when(is_not_null(in1.getField("log_dw_bid_curator")).cast(BooleanType),
           lit(1)
      ).otherwise(has_curator_bid).cast(IntegerType)
    (has_imp === lit(1))
      .and(has_preempt === lit(1))
      .and(has_curator_bid === lit(1))
  }

  def f_router_is_dw(in1: Column): Column =
    when(
      is_not_null(
        in1
          .getField("log_impbus_impressions_pricing")
          .getField("buyer_charges")
          .getField("is_dw")
      ).or(
          is_not_null(
            in1
              .getField("log_impbus_impressions_pricing")
              .getField("seller_charges")
              .getField("is_dw")
          )
        )
        .and(
          (in1
            .getField("log_impbus_impressions_pricing")
            .getField("buyer_charges")
            .getField("is_dw") === lit(1)).or(
            in1
              .getField("log_impbus_impressions_pricing")
              .getField("seller_charges")
              .getField("is_dw") === lit(1)
          )
        ),
      lit(1)
    ).otherwise(lit(0))

  def f_router_is_quarantined(
    in1:                                       Column,
    advertiser_id_by_campaign_group_id_lookup: Column,
    member_id_by_advertiser_id_lookup:         Column,
    member_id_by_publisher_id_lookup:          Column
  ): Column = {
    var imp_type: org.apache.spark.sql.Column =
      in1.getField("imp_type").cast(IntegerType)
    var seller_member_id: org.apache.spark.sql.Column = in1
      .getField("log_impbus_impressions")
      .getField("seller_member_id")
      .cast(IntegerType)
    var publisher_id: org.apache.spark.sql.Column =
      in1.getField("publisher_id").cast(IntegerType)
    var buyer_member_id: org.apache.spark.sql.Column = coalesce(
      in1.getField("log_dw_bid").getField("member_id"),
      in1.getField("log_impbus_preempt").getField("buyer_member_id"),
      in1.getField("log_impbus_impressions").getField("buyer_member_id"),
      lit(null)
    ).cast(IntegerType)
    var advertiser_id: org.apache.spark.sql.Column =
      in1.getField("advertiser_id").cast(IntegerType)
    var campaign_group_id: org.apache.spark.sql.Column =
      in1.getField("campaign_group_id").cast(IntegerType)
    var publisher_seller_member_id: org.apache.spark.sql.Column =
      lit(null).cast(IntegerType)
    var advertiser_buyer_member_id: org.apache.spark.sql.Column =
      lit(null).cast(IntegerType)
    var campaign_group_advertiser_id: org.apache.spark.sql.Column =
      lit(null).cast(IntegerType)
    var quarantine: org.apache.spark.sql.Column = lit(0)
    var log_impbus_impressions_pricing_count: org.apache.spark.sql.Column =
      coalesce(in1.getField("log_impbus_impressions_pricing_count"), lit(0))
        .cast(IntegerType)
    var gross_payment_value_microcents: org.apache.spark.sql.Column =
      coalesce(in1
                 .getField("log_impbus_impressions_pricing")
                 .getField("impression_event_pricing")
                 .getField("gross_payment_value_microcents"),
               lit(0)
      ).cast(LongType)
    var gross_payment_value_microcents_dup: org.apache.spark.sql.Column =
      coalesce(in1
                 .getField("log_impbus_impressions_pricing_dup")
                 .getField("impression_event_pricing")
                 .getField("gross_payment_value_microcents"),
               lit(0)
      ).cast(LongType)
    quarantine = when(
      is_not_null(in1.getField("log_dw_bid").getField("member_id"))
        .and(
          is_not_null(
            coalesce(
              in1.getField("log_impbus_preempt").getField("buyer_member_id"),
              in1
                .getField("log_impbus_impressions")
                .getField("buyer_member_id"),
              lit(null)
            )
          )
        )
        .and(
          coalesce(
            in1.getField("log_impbus_preempt").getField("buyer_member_id"),
            in1.getField("log_impbus_impressions").getField("buyer_member_id"),
            lit(null)
          ) =!= in1.getField("log_dw_bid").getField("member_id")
        ),
      lit(1)
    ).when(
        is_not_null(in1.getField("log_dw_bid_last").getField("member_id"))
          .and(is_not_null(in1.getField("log_dw_bid").getField("member_id")))
          .and(
            in1.getField("log_dw_bid_last").getField("member_id") =!= in1
              .getField("log_dw_bid")
              .getField("member_id")
          ),
        lit(1)
      )
      .when(
        is_not_null(in1.getField("log_dw_bid_last").getField("advertiser_id"))
          .and(
            is_not_null(in1.getField("log_dw_bid").getField("advertiser_id"))
          )
          .and(
            in1.getField("log_dw_bid_last").getField("advertiser_id") =!= in1
              .getField("log_dw_bid")
              .getField("advertiser_id")
          ),
        lit(1)
      )
      .when(
        is_not_null(
          in1.getField("log_dw_bid_last").getField("campaign_group_id")
        ).and(
            is_not_null(
              in1.getField("log_dw_bid").getField("campaign_group_id")
            )
          )
          .and(
            in1
              .getField("log_dw_bid_last")
              .getField("campaign_group_id") =!= in1
              .getField("log_dw_bid")
              .getField("campaign_group_id")
          ),
        lit(1)
      )
      .otherwise(quarantine)
      .cast(IntegerType)
    quarantine = when(
      quarantine === lit(0),
      when(isnull(member_id_by_publisher_id_lookup.getField("seller_member_id"))
             .and(is_not_null(publisher_id).and(publisher_id =!= lit(0))),
           lit(1)
      ).when(
          is_not_null(campaign_group_id)
            .and(campaign_group_id =!= lit(0))
            .and(is_not_null(advertiser_id))
            .and(advertiser_id =!= lit(0))
            .and(
              is_not_null(imp_type)
                .and((imp_type === lit(6)).and(imp_type === lit(2)))
                .and(is_not_null(seller_member_id))
                .and(
                  is_not_null(
                    member_id_by_publisher_id_lookup
                      .getField("seller_member_id")
                  )
                )
                .and(
                  seller_member_id =!= member_id_by_publisher_id_lookup
                    .getField("seller_member_id")
                )
                .or(
                  is_not_null(imp_type)
                    .and(imp_type =!= lit(6))
                    .and(imp_type =!= lit(2))
                    .and(is_not_null(buyer_member_id))
                    .and(
                      is_not_null(
                        member_id_by_advertiser_id_lookup
                          .getField("buyer_member_id")
                      )
                    )
                    .and(
                      buyer_member_id =!= member_id_by_advertiser_id_lookup
                        .getField("buyer_member_id")
                    )
                )
                .or(
                  is_not_null(buyer_member_id)
                    .and(
                      is_not_null(
                        member_id_by_advertiser_id_lookup
                          .getField("buyer_member_id")
                      )
                    )
                    .and(
                      is_not_null(
                        advertiser_id_by_campaign_group_id_lookup
                          .getField("advertiser_id")
                      )
                    )
                    .and(
                      buyer_member_id =!= member_id_by_advertiser_id_lookup
                        .getField("buyer_member_id")
                    )
                    .and(
                      advertiser_id =!= advertiser_id_by_campaign_group_id_lookup
                        .getField("advertiser_id")
                    )
                )
            ),
          lit(1)
        )
        .when(
          is_not_null(imp_type)
            .and((imp_type === lit(6)).and(imp_type === lit(2)))
            .and(is_not_null(seller_member_id))
            .and(
              is_not_null(
                member_id_by_publisher_id_lookup.getField("seller_member_id")
              )
            )
            .and(
              seller_member_id =!= member_id_by_publisher_id_lookup
                .getField("seller_member_id")
            ),
          lit(1)
        )
        .when(
          is_not_null(publisher_id)
            .and(
              is_not_null(
                member_id_by_publisher_id_lookup.getField("seller_member_id")
              )
            )
            .and(is_not_null(seller_member_id))
            .and(publisher_id =!= lit(0))
            .and(
              member_id_by_publisher_id_lookup
                .getField("seller_member_id") =!= seller_member_id
            ),
          lit(1)
        )
        .when(
          is_not_null(imp_type)
            .and(imp_type === lit(5))
            .and(is_not_null(seller_member_id))
            .and(is_not_null(buyer_member_id))
            .and(buyer_member_id =!= lit(0))
            .and(seller_member_id =!= buyer_member_id),
          lit(1)
        )
        .otherwise(quarantine)
    ).otherwise(quarantine).cast(IntegerType)
    quarantine = when(
      quarantine === lit(0),
      when(
        (log_impbus_impressions_pricing_count > lit(1)).and(
          gross_payment_value_microcents =!= gross_payment_value_microcents_dup
        ),
        lit(1)
      ).otherwise(quarantine)
    ).otherwise(quarantine).cast(IntegerType)
    quarantine
  }

  def f_router_is_transacted(in1: Column): Column = {
    var has_imp:      org.apache.spark.sql.Column = lit(0)
    var is_delivered: org.apache.spark.sql.Column = lit(0)
    var has_preempt:  org.apache.spark.sql.Column = lit(0)
    has_imp = when(
      is_not_null(in1.getField("log_impbus_impressions")).cast(BooleanType),
      lit(1)
    ).otherwise(has_imp).cast(IntegerType)
    is_delivered = when(
      is_not_null(
        in1.getField("log_impbus_impressions").getField("is_delivered")
      ).and(
        in1.getField("log_impbus_impressions").getField("is_delivered") === lit(
          1
        )
      ),
      lit(1)
    ).otherwise(is_delivered).cast(IntegerType)
    has_preempt =
      when(is_not_null(in1.getField("log_impbus_preempt")).cast(BooleanType),
           lit(1)
      ).otherwise(has_preempt).cast(IntegerType)
    (has_imp === lit(1))
      .and((has_preempt === lit(1)).or(is_delivered === lit(1)))
  }

  def v_impression_transaction_event_pricing(): Column =
    f_get_impression_transaction_event_pricing(
      col("log_impbus_impressions_pricing.impression_event_pricing"),
      col("log_impbus_impressions_pricing.buyer_charges"),
      col("log_impbus_impressions_pricing.seller_charges")
    )

  def impression_buyer_charges_pricing_terms(): Column =
    when(
      is_not_null(
        f_get_impression_transaction_event_pricing(
          col("log_impbus_impressions_pricing.impression_event_pricing"),
          col("log_impbus_impressions_pricing.buyer_charges"),
          col("log_impbus_impressions_pricing.seller_charges")
        ).getField("buyer_charges").getField("pricing_terms")
      ).cast(BooleanType),
      v_impression_transaction_event_pricing()
        .getField("buyer_charges")
        .getField("pricing_terms")
    ).otherwise(
      lit(null).cast(
        ArrayType(
          StructType(
            List(
              StructField("term_id",                 IntegerType, true),
              StructField("amount",                  DoubleType,  true),
              StructField("rate",                    DoubleType,  true),
              StructField("is_deduction",            BooleanType, true),
              StructField("is_media_cost_dependent", BooleanType, true),
              StructField("data_member_id",          IntegerType, true)
            )
          ),
          true
        )
      )
    )

  def v_transaction_event_pricing(): Column =
    f_get_transaction_event_pricing(
      col("log_impbus_impressions_pricing.impression_event_pricing"),
      col("log_impbus_auction_event.auction_event_pricing"),
      col("log_impbus_impressions_pricing.buyer_charges"),
      col("log_impbus_impressions_pricing.seller_charges"),
      f_should_process_views(
        col("log_dw_view"),
        f_transaction_event(
          col("log_impbus_impressions.seller_transaction_def"),
          col("log_impbus_preempt.seller_transaction_def")
        ).cast(IntegerType),
        f_transaction_event(col("log_impbus_impressions.buyer_transaction_def"),
                            col("log_impbus_preempt.buyer_transaction_def")
        ).cast(IntegerType)
      ).cast(IntegerType)
    )

  def f_forward_for_next_stage(
    agg_type:     Column,
    payment_type: Column
  ): Column = {
    var l_payment_type: org.apache.spark.sql.Column =
      coalesce(payment_type, lit(0)).cast(IntegerType)
    var l_forward_for_next_stage: org.apache.spark.sql.Column = lit(0)
    l_forward_for_next_stage =
      when(coalesce(agg_type, lit(0)) === lit(1),
           when(l_payment_type === lit(2), lit(1)).otherwise(lit(0))
      ).when(coalesce(agg_type, lit(0)) === lit(3), lit(0))
        .when(coalesce(agg_type, lit(0)) === lit(5), lit(0))
        .otherwise(lit(1))
        .cast(IntegerType)
    l_forward_for_next_stage
  }

  def f_is_matching_payment_and_agg_type(
    agg_type:     Column,
    payment_type: Column
  ): Column = {
    var l_agg_type: org.apache.spark.sql.Column =
      coalesce(agg_type, lit(0)).cast(IntegerType)
    var l_payment_type: org.apache.spark.sql.Column =
      coalesce(payment_type, lit(0)).cast(IntegerType)
    var l_is_matching_payment_and_agg_type: org.apache.spark.sql.Column = lit(0)
    l_is_matching_payment_and_agg_type = when(
      (coalesce(agg_type, lit(0)) =!= lit(1)).and(
        coalesce(agg_type, lit(0)) =!= lit(2)
      ),
      when(l_agg_type === lit(3),
           when(l_payment_type === lit(2), lit(1)).otherwise(
             l_is_matching_payment_and_agg_type
           )
      ).when(l_agg_type === lit(4),
              when(l_payment_type === lit(5), lit(1))
                .otherwise(l_is_matching_payment_and_agg_type)
        )
        .otherwise(l_is_matching_payment_and_agg_type)
    ).when(coalesce(payment_type, lit(0)) === lit(1), lit(1))
      .otherwise(l_is_matching_payment_and_agg_type)
      .cast(IntegerType)
    l_is_matching_payment_and_agg_type
  }

  def f_payment_type_matches(agg_type: Column, payment_type: Column): Column = {
    var l_payment_type_matches: org.apache.spark.sql.Column = lit(0)
    l_payment_type_matches = when(
      (f_is_matching_payment_and_agg_type(coalesce(agg_type,     lit(0)),
                                          coalesce(payment_type, lit(0))
      ) === lit(1))
        .or(
          (coalesce(agg_type, lit(0)) === lit(5))
            .and(coalesce(payment_type, lit(0)) === lit(6))
        )
        .or(
          (coalesce(agg_type, lit(0)) === lit(0)).and(
            (coalesce(payment_type, lit(0)) =!= lit(1))
              .and(coalesce(payment_type, lit(0)) =!= lit(2))
              .and(coalesce(payment_type, lit(0)) =!= lit(5))
              .and(coalesce(payment_type, lit(0)) =!= lit(6))
          )
        ),
      lit(1)
    ).otherwise(l_payment_type_matches).cast(IntegerType)
    l_payment_type_matches
  }

  def f_keep_data_charge(
    cost_pct:     Column,
    agg_type:     Column,
    payment_type: Column
  ): Column = {
    var l_agg_type: org.apache.spark.sql.Column =
      coalesce(agg_type, lit(0)).cast(IntegerType)
    var l_payment_type: org.apache.spark.sql.Column =
      coalesce(payment_type, lit(0)).cast(IntegerType)
    var l_keep_data_charge: org.apache.spark.sql.Column = lit(0)
    l_keep_data_charge = when(
      coalesce(cost_pct, lit(0)) > lit(0),
      when((f_payment_type_matches(l_agg_type, l_payment_type) === lit(1)).or(
             f_forward_for_next_stage(l_agg_type, l_payment_type) === lit(1)
           ),
           lit(1)
      ).otherwise(l_keep_data_charge)
    ).when((coalesce(agg_type, lit(0)) === lit(0))
              .or(coalesce(agg_type, lit(0)) === lit(4)),
            lit(1)
      )
      .otherwise(l_keep_data_charge)
      .cast(IntegerType)
    l_keep_data_charge
  }

  def f_create_agg_dw_impressions_return_11668529(
    curator_member_id:               Column,
    discrepancy_allowance:           Column,
    should_process_views:            Column,
    buyer_member_id:                 Column,
    imps_for_budget_caps_pacing:     Column,
    imp_type:                        Column,
    is_curated:                      Column,
    view_detection_enabled:          Column,
    buyer_trx_event_type_id:         Column,
    commission_revshare:             Column,
    creative_overage_fees:           Column,
    viewdef_definition_id:           Column,
    virtual_log_dw_bid:              Column,
    has_buyer_transacted:            Column,
    payment_type_normalized:         Column,
    seller_trx_event_id:             Column,
    seller_revenue_cpm:              Column,
    insertion_order_id:              Column,
    revenue_type_normalized:         Column,
    billing_period_id:               Column,
    viewdef_viewable:                Column,
    split_id:                        Column,
    data_costs_deal:                 Column,
    f_preempt_over_impression_94298: Column,
    commission_cpm:                  Column,
    v_transaction_event_pricing:     Column,
    buyer_trx_event_id:              Column,
    campaign_group_id:               Column,
    has_seller_transacted:           Column,
    revenue_value:                   Column,
    netflix_ppid:                    Column,
    campaign_group_type_id:          Column,
    should_zero_seller_revenue:      Column,
    view_measurable:                 Column,
    advertiser_id:                   Column,
    media_cost_dollars_cpm:          Column,
    creative_id:                     Column,
    f_preempt_over_impression_95337: Column,
    flight_id:                       Column,
    viewable:                        Column,
    auction_service_deduction:       Column,
    media_buy_cost:                  Column,
    seller_deduction:                Column,
    buyer_spend_cpm:                 Column,
    seller_trx_event_type_id:        Column,
    auction_service_fees:            Column,
    serving_fees_cpm:                Column,
    data_costs:                      Column,
    sup_ip_range_lookup_count:       Column,
    ttl:                             Column,
    view_result:                     Column,
    is_dw_normalized:                Column,
    booked_revenue_adv_curr:         Column,
    booked_revenue_dollars:          Column,
    deal_id:                         Column,
    _f_is_buy_side:                  Column,
    deal_type:                       Column,
    campaign_id:                     Column,
    is_placeholder_bid:              Column,
    serving_fees_revshare:           Column,
    view_non_measurable_reason:      Column
  ): Column =
    struct(
      coalesce(
        col("log_dw_view.date_time").cast(LongType),
        when((ttl > lit(3600)).and(
               is_not_null(col("log_impbus_preempt.date_time").cast(LongType))
             ),
             col("log_impbus_preempt.date_time").cast(LongType)
        ),
        col("date_time").cast(LongType)
      ).as("date_time"),
      col("auction_id_64").cast(LongType).as("auction_id_64"),
      coalesce(
        when(col("log_dw_view.user_id_64").cast(LongType) =!= lit(0),
             col("log_dw_view.user_id_64").cast(LongType)
        ),
        when(col("log_impbus_impressions.user_id_64").cast(LongType) =!= lit(0),
             col("log_impbus_impressions.user_id_64").cast(LongType)
        )
      ).as("user_id_64"),
      when(col("log_impbus_impressions.tag_id").cast(IntegerType) =!= lit(0),
           col("log_impbus_impressions.tag_id").cast(IntegerType)
      ).as("tag_id"),
      when(col("log_impbus_impressions.venue_id").cast(IntegerType) =!= lit(0),
           col("log_impbus_impressions.venue_id").cast(IntegerType)
      ).as("venue_id"),
      when(col("log_impbus_impressions.inventory_source_id").cast(
             IntegerType
           ) =!= lit(0),
           col("log_impbus_impressions.inventory_source_id").cast(IntegerType)
      ).as("inventory_source_id"),
      lit(null).as("session_frequency"),
      when(string_compare(col("log_impbus_impressions.site_domain"),
                          lit("---")
           ) =!= lit(0),
           col("log_impbus_impressions.site_domain")
      ).as("site_domain"),
      f_preempt_over_impression_non_zero_explicit(
        is_not_null(col("log_impbus_preempt")),
        col("log_impbus_impressions.width").cast(IntegerType),
        col("log_impbus_preempt.width").cast(IntegerType)
      ).as("width"),
      f_preempt_over_impression_non_zero_explicit(
        is_not_null(col("log_impbus_preempt")),
        col("log_impbus_impressions.height").cast(IntegerType),
        col("log_impbus_preempt.height").cast(IntegerType)
      ).as("height"),
      when(string_compare(col("log_impbus_impressions.geo_country"),
                          lit("--")
           ) =!= lit(0),
           col("log_impbus_impressions.geo_country")
      ).as("geo_country"),
      when(string_compare(col("log_impbus_impressions.geo_region"),
                          lit("--")
           ) =!= lit(0),
           col("log_impbus_impressions.geo_region")
      ).as("geo_region"),
      when(string_compare(col("log_impbus_impressions.gender"),
                          lit("u")
           ) =!= lit(0),
           col("log_impbus_impressions.gender")
      ).as("gender"),
      when(col("log_impbus_impressions.age").cast(IntegerType) =!= lit(0),
           col("log_impbus_impressions.age").cast(IntegerType)
      ).as("age"),
      col("seller_member_id").cast(IntegerType).as("seller_member_id"),
      when(col("imp_type").cast(IntegerType) =!= lit(1), buyer_member_id)
        .otherwise(lit(0))
        .as("buyer_member_id"),
      creative_id.as("creative_id"),
      when(string_compare(col("log_impbus_impressions.seller_currency"),
                          lit("")
           ) =!= lit(0),
           col("log_impbus_impressions.seller_currency")
      ).as("seller_currency"),
      when(string_compare(col("log_impbus_impressions.buyer_currency"),
                          lit("")
           ) =!= lit(0),
           col("log_impbus_impressions.buyer_currency")
      ).as("buyer_currency"),
      f_preempt_over_impression_non_zero_explicit(
        is_not_null(col("log_impbus_preempt")),
        col("log_impbus_impressions.buyer_bid"),
        col("log_impbus_preempt.buyer_bid")
      ).as("buyer_bid"),
      coalesce(when(imp_type.isin(lit(8), lit(1), lit(3)), lit(0)),
               when(imp_type === lit(5), col("log_dw_bid.price")),
               buyer_spend_cpm
      ).as("buyer_spend"),
      when(col("log_impbus_impressions.ecp") =!= lit(0),
           col("log_impbus_impressions.ecp")
      ).as("ecp"),
      when(col("log_impbus_impressions.reserve_price") =!= lit(0.0d),
           col("log_impbus_impressions.reserve_price")
      ).as("reserve_price"),
      coalesce(when((col("imp_type").cast(IntegerType) === lit(1))
                      .or(buyer_member_id === lit(0))
                      .and(is_not_null(advertiser_id)),
                    lit(0)
               ),
               advertiser_id
      ).as("advertiser_id"),
      coalesce(when((col("imp_type").cast(IntegerType) === lit(1))
                      .or(buyer_member_id === lit(0))
                      .and(is_not_null(campaign_group_id)),
                    lit(0)
               ),
               campaign_group_id
      ).as("campaign_group_id"),
      coalesce(
        when((col("imp_type").cast(IntegerType) === lit(1))
               .or(buyer_member_id === lit(0))
               .and(is_not_null(campaign_id)),
             lit(0)
        ),
        when((imp_type =!= lit(6)).and(campaign_id =!= lit(0)), campaign_id)
      ).as("campaign_id"),
      coalesce(
        when((_f_is_buy_side === lit(1)).and(
               col("log_dw_bid.creative_freq").cast(IntegerType) =!= lit(0)
             ),
             col("log_dw_bid.creative_freq").cast(IntegerType)
        ),
        when(_f_is_buy_side === lit(0),
             virtual_log_dw_bid.getField("creative_freq")
        )
      ).as("creative_freq"),
      coalesce(
        when((_f_is_buy_side === lit(1)).and(
               col("log_dw_bid.creative_rec").cast(IntegerType) =!= lit(0)
             ),
             col("log_dw_bid.creative_rec").cast(IntegerType)
        ),
        when(_f_is_buy_side === lit(0),
             virtual_log_dw_bid.getField("creative_rec")
        )
      ).as("creative_rec"),
      coalesce(
        when((_f_is_buy_side === lit(1)).and(
               col("log_dw_bid.predict_type").cast(IntegerType) === lit(1)
             ),
             lit(1)
        ),
        when(
          (_f_is_buy_side === lit(1)).and(
            (col("log_dw_bid.predict_type").cast(IntegerType) === lit(0)).or(
              (col("log_dw_bid.predict_type").cast(IntegerType) >= lit(2)).and(
                col("log_dw_bid.predict_type").cast(IntegerType) =!= lit(9)
              )
            )
          ),
          lit(2)
        ),
        lit(0)
      ).as("is_learn"),
      when((_f_is_buy_side === lit(1)).and(
             col("log_dw_bid.is_remarketing").cast(IntegerType) =!= lit(0)
           ),
           col("log_dw_bid.is_remarketing").cast(IntegerType)
      ).as("is_remarketing"),
      coalesce(
        when((_f_is_buy_side === lit(1)).and(
               col("log_dw_bid.advertiser_freq").cast(IntegerType) =!= lit(0)
             ),
             col("log_dw_bid.advertiser_freq").cast(IntegerType)
        ),
        when((_f_is_buy_side === lit(1)).and(imp_type === lit(9)), lit(0)),
        when(_f_is_buy_side === lit(0),
             virtual_log_dw_bid.getField("advertiser_freq")
        )
      ).as("advertiser_frequency"),
      coalesce(
        when((_f_is_buy_side === lit(1)).and(
               col("log_dw_bid.advertiser_rec").cast(IntegerType) =!= lit(0)
             ),
             col("log_dw_bid.advertiser_rec").cast(IntegerType)
        ),
        when((_f_is_buy_side === lit(1)).and(imp_type === lit(9)), lit(0)),
        when(_f_is_buy_side === lit(0),
             virtual_log_dw_bid.getField("advertiser_rec")
        )
      ).as("advertiser_recency"),
      coalesce(
        when(col("log_impbus_impressions.user_id_64").cast(LongType) > lit(
               9223372036854775807L
             ).cast(LongType),
             lit(-1)
        ),
        when(
          (is_placeholder_bid === lit(1))
            .or(col("imp_type").cast(IntegerType) === lit(1))
            .or(col("imp_type").cast(IntegerType) === lit(3))
            .or(imp_type === lit(8))
            .and(
              is_not_null(
                col("log_impbus_impressions.user_id_64").cast(LongType)
              )
            ),
          col("log_impbus_impressions.user_id_64").cast(LongType) % lit(1000)
        ),
        when(
          (virtual_log_dw_bid.getField("log_type") === lit(2)).or(
            is_not_null(col("log_dw_bid.log_type").cast(IntegerType))
              .and(col("log_dw_bid.log_type").cast(IntegerType) === lit(2))
          ),
          col("log_impbus_impressions.user_group_id").cast(IntegerType)
        ),
        col("log_dw_bid.user_group_id").cast(IntegerType)
      ).as("user_group_id"),
      lit(null).as("camp_dp_id"),
      coalesce(when(imp_type.isin(lit(8), lit(1), lit(3)), lit(0)),
               col("publisher_id").cast(IntegerType)
      ).as("media_buy_id"),
      media_buy_cost.as("media_buy_cost"),
      f_preempt_over_impression_non_zero_explicit(
        is_not_null(col("log_impbus_preempt")),
        col("log_impbus_impressions.brand_id").cast(IntegerType),
        col("log_impbus_preempt.brand_id").cast(IntegerType)
      ).as("brand_id"),
      f_preempt_over_impression_non_zero_explicit(
        is_not_null(col("log_impbus_preempt")),
        col("log_impbus_impressions.cleared_direct").cast(IntegerType),
        col("log_impbus_preempt.cleared_direct").cast(IntegerType)
      ).as("cleared_direct"),
      lit(null).as("clear_fees"),
      when(imp_type =!= lit(9),
           col("log_impbus_impressions.media_buy_rev_share_pct")
      ).otherwise(col("log_dw_bid.media_buy_rev_share_pct"))
        .as("media_buy_rev_share_pct"),
      revenue_value.as("revenue_value"),
      coalesce(
        when(
          (_f_is_buy_side === lit(1)).and(
            string_compare(col("log_dw_bid.pricing_type"), lit("--")) =!= lit(0)
          ),
          col("log_dw_bid.pricing_type")
        ),
        when(_f_is_buy_side === lit(0),
             virtual_log_dw_bid.getField("pricing_type")
        )
      ).as("pricing_type"),
      when((_f_is_buy_side === lit(1))
             .and(col("log_dw_bid.can_convert").cast(IntegerType) =!= lit(0)),
           col("log_dw_bid.can_convert").cast(IntegerType)
      ).as("can_convert"),
      coalesce(
        when(
          (is_placeholder_bid === lit(1))
            .or(col("imp_type").cast(IntegerType) === lit(1))
            .or(col("imp_type").cast(IntegerType) === lit(3))
            .or(imp_type === lit(8)),
          lit(0)
        ),
        col("log_impbus_impressions.pub_rule_id").cast(IntegerType)
      ).as("pub_rule_id"),
      coalesce(
        when((_f_is_buy_side === lit(1))
               .and(col("log_dw_bid.is_control").cast(IntegerType) =!= lit(0)),
             col("log_dw_bid.is_control").cast(IntegerType)
        ),
        when((_f_is_buy_side === lit(0))
               .and(virtual_log_dw_bid.getField("is_control") =!= lit(0)),
             virtual_log_dw_bid.getField("is_control")
        )
      ).as("is_control"),
      when(col("log_dw_bid.control_pct") =!= lit(0),
           col("log_dw_bid.control_pct")
      ).as("control_pct"),
      when(col("log_dw_bid.control_creative_id").cast(IntegerType) =!= lit(0),
           col("log_dw_bid.control_creative_id").cast(IntegerType)
      ).as("control_creative_id"),
      lit(null).as("predicted_cpm"),
      coalesce(
        when(
          (_f_is_buy_side === lit(1)).and(col("log_dw_bid.price") > lit(999)),
          lit(999)
        ),
        when(
          (_f_is_buy_side === lit(1)).and(col("log_dw_bid.price") <= lit(999)),
          col("log_dw_bid.price")
        ),
        lit(0)
      ).as("actual_bid"),
      when(col("log_impbus_impressions.site_id").cast(IntegerType) =!= lit(0),
           col("log_impbus_impressions.site_id").cast(IntegerType)
      ).as("site_id"),
      when(col("log_impbus_impressions.content_category_id").cast(
             IntegerType
           ) =!= lit(0),
           col("log_impbus_impressions.content_category_id").cast(IntegerType)
      ).as("content_category_id"),
      coalesce(
        when((imp_type === lit(6))
               .and(is_dw_normalized === lit(1))
               .and(should_process_views === lit(1)),
             lit(0)
        ),
        when((imp_type === lit(7))
               .and(is_dw_normalized === lit(1))
               .and(should_process_views === lit(1)),
             auction_service_fees
        ),
        when((imp_type === lit(7))
               .and(is_dw_normalized === lit(0))
               .and(should_process_views === lit(1)),
             auction_service_fees
        ),
        when(f_is_non_cpm_payment_or_payment(imp_type,
                                             payment_type_normalized,
                                             revenue_type_normalized
             ) === lit(1),
             lit(0)
        ).otherwise(auction_service_fees)
      ).as("auction_service_fees"),
      coalesce(
        when((imp_type === lit(6))
               .and(is_dw_normalized === lit(1))
               .and(should_process_views === lit(1)),
             lit(0)
        ),
        when((imp_type === lit(7))
               .and(is_dw_normalized === lit(1))
               .and(should_process_views === lit(1)),
             discrepancy_allowance
        ),
        when((imp_type === lit(7))
               .and(is_dw_normalized === lit(0))
               .and(should_process_views === lit(1)),
             discrepancy_allowance
        ),
        when(f_is_non_cpm_payment_or_payment(imp_type,
                                             payment_type_normalized,
                                             revenue_type_normalized
             ) === lit(1),
             lit(0)
        ).otherwise(discrepancy_allowance)
      ).as("discrepancy_allowance"),
      lit(null).as("forex_allowance"),
      coalesce(
        when((imp_type === lit(6))
               .and(is_dw_normalized === lit(1))
               .and(should_process_views === lit(1)),
             lit(0)
        ),
        when((imp_type === lit(7))
               .and(is_dw_normalized === lit(1))
               .and(should_process_views === lit(1)),
             creative_overage_fees
        ),
        when((imp_type === lit(7))
               .and(is_dw_normalized === lit(0))
               .and(should_process_views === lit(1)),
             creative_overage_fees
        ),
        when(f_is_non_cpm_payment_or_payment(imp_type,
                                             payment_type_normalized,
                                             revenue_type_normalized
             ) === lit(1),
             lit(0)
        ).otherwise(creative_overage_fees)
      ).as("creative_overage_fees"),
      coalesce(col("log_impbus_impressions.fold_position").cast(IntegerType),
               lit(0)
      ).as("fold_position"),
      when(col("log_impbus_impressions.external_inv_id").cast(
             IntegerType
           ) =!= lit(0),
           col("log_impbus_impressions.external_inv_id").cast(IntegerType)
      ).as("external_inv_id"),
      coalesce(
        when((_f_is_buy_side === lit(1))
               .and(col("log_dw_bid.cadence_modifier") =!= lit(0)),
             col("log_dw_bid.cadence_modifier")
        ),
        when((_f_is_buy_side === lit(0))
               .and(virtual_log_dw_bid.getField("cadence_modifier") =!= lit(0)),
             virtual_log_dw_bid.getField("cadence_modifier")
        )
      ).as("cadence_modifier"),
      col("imp_type").cast(IntegerType).as("imp_type"),
      coalesce(
        when(_f_is_buy_side === lit(1), col("log_dw_bid.advertiser_currency")),
        lit("USD")
      ).as("advertiser_currency"),
      coalesce(
        when(
          (_f_is_buy_side === lit(1))
            .and(is_not_null(col("log_dw_bid.advertiser_exchange_rate")))
            .and(col("log_dw_bid.advertiser_exchange_rate") =!= lit(1.0d))
            .and(col("log_dw_bid.advertiser_exchange_rate") =!= lit(0.0d)),
          col("log_dw_bid.advertiser_exchange_rate")
        ),
        lit(1.0d)
      ).as("advertiser_exchange_rate"),
      col("log_impbus_impressions.ip_address").as("ip_address"),
      when(
        col("log_impbus_impressions.publisher_id").cast(IntegerType) =!= lit(0),
        col("log_impbus_impressions.publisher_id").cast(IntegerType)
      ).as("publisher_id"),
      coalesce(
        when((imp_type === lit(6))
               .and(is_dw_normalized === lit(1))
               .and(should_process_views === lit(1)),
             lit(0)
        ),
        when((imp_type === lit(7))
               .and(is_dw_normalized === lit(1))
               .and(should_process_views === lit(1)),
             auction_service_deduction
        ),
        when((imp_type === lit(7))
               .and(is_dw_normalized === lit(0))
               .and(should_process_views === lit(1)),
             auction_service_deduction
        ),
        when(f_is_non_cpm_payment_or_payment(imp_type,
                                             payment_type_normalized,
                                             revenue_type_normalized
             ) === lit(1),
             lit(0)
        ).otherwise(auction_service_deduction)
      ).as("auction_service_deduction"),
      coalesce(
        when((col("imp_type").cast(IntegerType) === lit(1))
               .or(buyer_member_id === lit(0))
               .and(is_not_null(insertion_order_id)),
             lit(0)
        ),
        when(insertion_order_id =!= lit(0), insertion_order_id)
      ).as("insertion_order_id"),
      coalesce(
        when(_f_is_buy_side === lit(1),
             col("log_dw_bid.predict_type").cast(IntegerType)
        ),
        when(_f_is_buy_side === lit(0),
             virtual_log_dw_bid.getField("predict_type")
        )
      ).as("predict_type_rev"),
      coalesce(
        when((_f_is_buy_side === lit(1)).and(
               col("log_dw_bid.predict_type_goal").cast(IntegerType) =!= lit(0)
             ),
             col("log_dw_bid.predict_type_goal").cast(IntegerType)
        ),
        when(_f_is_buy_side === lit(0),
             virtual_log_dw_bid.getField("predict_type_goal")
        )
      ).as("predict_type_goal"),
      coalesce(
        when(_f_is_buy_side === lit(1),
             col("log_dw_bid.predict_type").cast(IntegerType)
        ),
        when(_f_is_buy_side === lit(0),
             virtual_log_dw_bid.getField("predict_type")
        )
      ).as("predict_type_cost"),
      booked_revenue_dollars.as("booked_revenue_dollars"),
      booked_revenue_adv_curr.as("booked_revenue_adv_curr"),
      when(commission_cpm =!= lit(0),      commission_cpm).as("commission_cpm"),
      when(commission_revshare =!= lit(0), commission_revshare)
        .as("commission_revshare"),
      when(serving_fees_cpm =!= lit(0), serving_fees_cpm)
        .as("serving_fees_cpm"),
      when(serving_fees_revshare =!= lit(0), serving_fees_revshare)
        .as("serving_fees_revshare"),
      when(col("log_impbus_impressions.user_tz_offset").cast(
             IntegerType
           ) =!= lit(0),
           col("log_impbus_impressions.user_tz_offset").cast(IntegerType)
      ).as("user_tz_offset"),
      f_preempt_over_impression_non_zero_explicit(
        is_not_null(col("log_impbus_preempt.media_type").cast(IntegerType)),
        col("log_impbus_impressions.media_type").cast(IntegerType),
        col("log_impbus_preempt.media_type").cast(IntegerType)
      ).as("media_type"),
      when(col("log_impbus_impressions.operating_system").cast(
             IntegerType
           ) =!= lit(0),
           col("log_impbus_impressions.operating_system").cast(IntegerType)
      ).as("operating_system"),
      when(col("log_impbus_impressions.browser").cast(IntegerType) =!= lit(0),
           col("log_impbus_impressions.browser").cast(IntegerType)
      ).as("browser"),
      when(col("log_impbus_impressions.language").cast(IntegerType) =!= lit(0),
           col("log_impbus_impressions.language").cast(IntegerType)
      ).as("language"),
      col("log_impbus_impressions.seller_currency").as("publisher_currency"),
      coalesce(
        when(
          is_not_null(col("log_impbus_impressions.seller_exchange_rate")).and(
            col("log_impbus_impressions.seller_exchange_rate") =!= lit(1.0d)
          ),
          col("log_impbus_impressions.seller_exchange_rate")
        ),
        lit(1.0d)
      ).as("publisher_exchange_rate"),
      media_cost_dollars_cpm.as("media_cost_dollars_cpm"),
      coalesce(
        when(
          (is_placeholder_bid === lit(1)).or(
            (col("imp_type").cast(IntegerType) === lit(1))
              .or(col("imp_type").cast(IntegerType) === lit(3))
              .or(imp_type === lit(8))
          ),
          lit(0)
        ),
        payment_type_normalized
      ).as("payment_type"),
      coalesce(
        when(
          (is_placeholder_bid === lit(1)).or(
            (col("imp_type").cast(IntegerType) === lit(1))
              .or(imp_type === lit(2))
              .or(col("imp_type").cast(IntegerType) === lit(3))
              .or(imp_type === lit(8))
          ),
          lit(-1)
        ),
        revenue_type_normalized
      ).as("revenue_type"),
      coalesce(
        when(should_zero_seller_revenue.cast(BooleanType), lit(0)),
        when(
          (v_transaction_event_pricing
            .getField("buyer_charges")
            .getField("is_dw") === lit(1)).and(
            col("seller_member_id").cast(IntegerType) === col("buyer_member_id")
              .cast(IntegerType)
          ),
          seller_revenue_cpm
        ),
        when(_f_is_buy_side === lit(0), seller_revenue_cpm),
        when(
          not(
            (v_transaction_event_pricing
              .getField("buyer_charges")
              .getField("is_dw") === lit(1)).and(
              col("seller_member_id")
                .cast(IntegerType) === col("buyer_member_id").cast(IntegerType)
            )
          ),
          lit(0)
        ),
        when(imp_type === lit(9), lit(0))
      ).as("seller_revenue_cpm"),
      coalesce(
        col("log_impbus_preempt.bidder_id").cast(IntegerType),
        when(is_not_null(col("log_impbus_preempt")).cast(BooleanType), lit(0)),
        col("log_impbus_impressions.bidder_id").cast(IntegerType)
      ).as("bidder_id"),
      when(string_compare(col("log_impbus_impressions.inv_code"),
                          lit("---")
           ) =!= lit(0),
           col("log_impbus_impressions.inv_code")
      ).as("inv_code"),
      when(string_compare(col("log_impbus_impressions.application_id"),
                          lit("---")
           ) =!= lit(0),
           col("log_impbus_impressions.application_id")
      ).as("application_id"),
      when(col("log_impbus_impressions.shadow_price") =!= lit(0.0d),
           col("log_impbus_impressions.shadow_price")
      ).as("shadow_price"),
      when(col("log_impbus_impressions.eap") =!= lit(0),
           col("log_impbus_impressions.eap")
      ).as("eap"),
      when(col("log_impbus_impressions.datacenter_id").cast(
             IntegerType
           ) =!= lit(0),
           col("log_impbus_impressions.datacenter_id").cast(IntegerType)
      ).as("datacenter_id"),
      when(
        col("log_impbus_impressions.imp_blacklist_or_fraud").cast(
          IntegerType
        ) =!= lit(0),
        col("log_impbus_impressions.imp_blacklist_or_fraud").cast(IntegerType)
      ).as("imp_blacklist_or_fraud"),
      f_preempt_over_impression_non_zero_explicit(
        is_not_null(col("log_impbus_preempt")),
        col("log_impbus_impressions.vp_expose_domains").cast(IntegerType),
        col("log_impbus_preempt.vp_expose_domains").cast(IntegerType)
      ).as("vp_expose_domains"),
      f_preempt_over_impression_non_zero_explicit(
        is_not_null(col("log_impbus_preempt")),
        col("log_impbus_impressions.vp_expose_categories").cast(IntegerType),
        col("log_impbus_preempt.vp_expose_categories").cast(IntegerType)
      ).as("vp_expose_categories"),
      f_preempt_over_impression_non_zero_explicit(
        is_not_null(col("log_impbus_preempt")),
        col("log_impbus_impressions.vp_expose_pubs").cast(IntegerType),
        col("log_impbus_preempt.vp_expose_pubs").cast(IntegerType)
      ).as("vp_expose_pubs"),
      f_preempt_over_impression_non_zero_explicit(
        is_not_null(col("log_impbus_preempt")),
        col("log_impbus_impressions.vp_expose_tag").cast(IntegerType),
        col("log_impbus_preempt.vp_expose_tag").cast(IntegerType)
      ).as("vp_expose_tag"),
      when(col("log_dw_bid.vp_expose_age").cast(IntegerType) =!= lit(0),
           col("log_dw_bid.vp_expose_age").cast(IntegerType)
      ).as("vp_expose_age"),
      when(col("log_dw_bid.vp_expose_gender").cast(IntegerType) =!= lit(0),
           col("log_dw_bid.vp_expose_gender").cast(IntegerType)
      ).as("vp_expose_gender"),
      when(col("log_impbus_impressions.inventory_url_id").cast(
             IntegerType
           ) =!= lit(0),
           col("log_impbus_impressions.inventory_url_id").cast(IntegerType)
      ).as("inventory_url_id"),
      when(
        col("log_impbus_impressions.audit_type").cast(IntegerType) =!= lit(0),
        col("log_impbus_impressions.audit_type").cast(IntegerType)
      ).as("audit_type"),
      when(
        col("log_impbus_impressions.is_exclusive").cast(IntegerType) =!= lit(0),
        col("log_impbus_impressions.is_exclusive").cast(IntegerType)
      ).as("is_exclusive"),
      when(
        col("log_impbus_impressions.truncate_ip").cast(IntegerType) =!= lit(0),
        col("log_impbus_impressions.truncate_ip").cast(IntegerType)
      ).as("truncate_ip"),
      when(col("log_impbus_impressions.device_id").cast(IntegerType) =!= lit(0),
           col("log_impbus_impressions.device_id").cast(IntegerType)
      ).as("device_id"),
      when(
        col("log_impbus_impressions.carrier_id").cast(IntegerType) =!= lit(0),
        col("log_impbus_impressions.carrier_id").cast(IntegerType)
      ).as("carrier_id"),
      f_preempt_over_impression_non_zero_explicit(
        is_not_null(col("log_impbus_preempt")),
        col("log_impbus_impressions.creative_audit_status").cast(IntegerType),
        col("log_impbus_preempt.creative_audit_status").cast(IntegerType)
      ).as("creative_audit_status"),
      f_preempt_over_impression_non_zero_explicit(
        is_not_null(col("log_impbus_preempt")),
        col("log_impbus_impressions.is_creative_hosted").cast(IntegerType),
        col("log_impbus_preempt.is_creative_hosted").cast(IntegerType)
      ).as("is_creative_hosted"),
      coalesce(when(should_zero_seller_revenue.cast(BooleanType), lit(0)),
               seller_deduction
      ).as("seller_deduction"),
      when(col("log_impbus_impressions.city").cast(IntegerType) =!= lit(0),
           col("log_impbus_impressions.city").cast(IntegerType)
      ).as("city"),
      when(string_compare(col("log_impbus_impressions.latitude"),
                          lit("")
           ) =!= lit(0),
           col("log_impbus_impressions.latitude")
      ).as("latitude"),
      when(string_compare(col("log_impbus_impressions.longitude"),
                          lit("")
           ) =!= lit(0),
           col("log_impbus_impressions.longitude")
      ).as("longitude"),
      when(string_compare(col("log_impbus_impressions.device_unique_id"),
                          lit("")
           ) =!= lit(0),
           col("log_impbus_impressions.device_unique_id")
      ).as("device_unique_id"),
      when(
        col("log_impbus_impressions.package_id").cast(IntegerType) =!= lit(0),
        col("log_impbus_impressions.package_id").cast(IntegerType)
      ).as("package_id"),
      when((_f_is_buy_side === lit(1)).and(
             string_compare(col("log_dw_bid.targeted_segments"),
                            lit("")
             ) =!= lit(0)
           ),
           col("log_dw_bid.targeted_segments")
      ).as("targeted_segments"),
      when(
        col("log_impbus_impressions.supply_type").cast(IntegerType) =!= lit(0),
        col("log_impbus_impressions.supply_type").cast(IntegerType)
      ).as("supply_type"),
      when(
        col("log_impbus_impressions.is_toolbar").cast(IntegerType) =!= lit(0),
        col("log_impbus_impressions.is_toolbar").cast(IntegerType)
      ).as("is_toolbar"),
      deal_id.as("deal_id"),
      f_preempt_over_impression_non_zero_explicit(
        is_not_null(col("log_impbus_preempt")),
        col("log_impbus_impressions.vp_bitmap").cast(LongType),
        col("log_impbus_preempt.vp_bitmap").cast(LongType)
      ).as("vp_bitmap"),
      view_detection_enabled.as("view_detection_enabled"),
      view_result.as("view_result"),
      when(col("log_impbus_impressions.ozone_id").cast(IntegerType) =!= lit(0),
           col("log_impbus_impressions.ozone_id").cast(IntegerType)
      ).as("ozone_id"),
      when(col("log_impbus_impressions.is_performance").cast(
             IntegerType
           ) =!= lit(0),
           col("log_impbus_impressions.is_performance").cast(IntegerType)
      ).as("is_performance"),
      when(string_compare(col("log_impbus_impressions.sdk_version"),
                          lit("---")
           ) =!= lit(0),
           col("log_impbus_impressions.sdk_version")
      ).as("sdk_version"),
      when(
        col("log_impbus_impressions.inventory_session_frequency").cast(
          IntegerType
        ) =!= lit(0),
        col("log_impbus_impressions.inventory_session_frequency").cast(
          IntegerType
        )
      ).as("inventory_session_frequency"),
      when(
        col("log_impbus_impressions.device_type").cast(IntegerType) =!= lit(0),
        col("log_impbus_impressions.device_type").cast(IntegerType)
      ).as("device_type"),
      when(col("log_impbus_impressions.dma").cast(IntegerType) =!= lit(0),
           col("log_impbus_impressions.dma").cast(IntegerType)
      ).as("dma"),
      when(string_compare(col("log_impbus_impressions.postal"),
                          lit("---")
           ) =!= lit(0),
           col("log_impbus_impressions.postal")
      ).as("postal"),
      viewdef_definition_id.as("viewdef_definition_id"),
      viewdef_viewable.as("viewdef_viewable"),
      view_measurable.as("view_measurable"),
      viewable.as("viewable"),
      when(col("log_impbus_impressions.is_secure").cast(IntegerType) =!= lit(0),
           col("log_impbus_impressions.is_secure").cast(IntegerType)
      ).as("is_secure"),
      view_non_measurable_reason.as("view_non_measurable_reason"),
      coalesce(when((buyer_trx_event_id === lit(1)).and(imp_type === lit(6)),
                    data_costs_deal
               ),
               when(_f_is_buy_side === lit(1), data_costs)
      ).as("data_costs"),
      when(col("log_impbus_impressions.bidder_instance_id").cast(
             IntegerType
           ) =!= lit(0),
           col("log_impbus_impressions.bidder_instance_id").cast(IntegerType)
      ).as("bidder_instance_id"),
      coalesce(
        when(
          (_f_is_buy_side === lit(1)).and(
            col("log_dw_bid.campaign_group_freq").cast(IntegerType) =!= lit(-2)
          ),
          col("log_dw_bid.campaign_group_freq").cast(IntegerType)
        ),
        when((_f_is_buy_side === lit(0)).and(
               virtual_log_dw_bid.getField("campaign_group_freq") =!= lit(-2)
             ),
             virtual_log_dw_bid.getField("campaign_group_freq")
        )
      ).as("campaign_group_freq"),
      coalesce(
        when((_f_is_buy_side === lit(1)).and(
               col("log_dw_bid.campaign_group_rec").cast(IntegerType) =!= lit(0)
             ),
             col("log_dw_bid.campaign_group_rec").cast(IntegerType)
        ),
        when(_f_is_buy_side === lit(0),
             virtual_log_dw_bid.getField("campaign_group_rec")
        )
      ).as("campaign_group_rec"),
      coalesce(
        when(
          (_f_is_buy_side === lit(1)).and(
            col("log_dw_bid.insertion_order_freq").cast(IntegerType) =!= lit(-2)
          ),
          col("log_dw_bid.insertion_order_freq").cast(IntegerType)
        ),
        when((_f_is_buy_side === lit(0)).and(
               virtual_log_dw_bid.getField("insertion_order_freq") =!= lit(-2)
             ),
             virtual_log_dw_bid.getField("insertion_order_freq")
        )
      ).as("insertion_order_freq"),
      coalesce(
        when(
          (_f_is_buy_side === lit(1)).and(
            col("log_dw_bid.insertion_order_rec").cast(IntegerType) =!= lit(0)
          ),
          col("log_dw_bid.insertion_order_rec").cast(IntegerType)
        ),
        when(_f_is_buy_side === lit(0),
             virtual_log_dw_bid.getField("insertion_order_rec")
        )
      ).as("insertion_order_rec"),
      when((_f_is_buy_side === lit(1)).and(
             string_compare(col("log_dw_bid.buyer_gender"), lit("u")) =!= lit(0)
           ),
           col("log_dw_bid.buyer_gender")
      ).as("buyer_gender"),
      coalesce(
        when(
          (_f_is_buy_side === lit(1))
            .and(is_not_null(col("log_dw_bid.buyer_age").cast(IntegerType)))
            .and(col("log_dw_bid.buyer_age").cast(IntegerType) =!= lit(-1)),
          col("log_dw_bid.buyer_age").cast(IntegerType)
        ),
        lit(0)
      ).as("buyer_age"),
      when((_f_is_buy_side === lit(1))
             .and(is_not_null(col("log_dw_bid.targeted_segment_list"))),
           col("log_dw_bid.targeted_segment_list")
      ).as("targeted_segment_list"),
      when((_f_is_buy_side === lit(1)).and(
             col("log_dw_bid.custom_model_id").cast(IntegerType) =!= lit(0)
           ),
           col("log_dw_bid.custom_model_id").cast(IntegerType)
      ).as("custom_model_id"),
      when(
        (_f_is_buy_side === lit(1)).and(
          col("log_dw_bid.custom_model_last_modified").cast(LongType) =!= lit(0)
        ),
        col("log_dw_bid.custom_model_last_modified").cast(LongType)
      ).as("custom_model_last_modified"),
      coalesce(
        when((_f_is_buy_side === lit(1)).and(
               string_compare(col("log_dw_bid.custom_model_output_code"),
                              lit("")
               ) =!= lit(0)
             ),
             col("log_dw_bid.custom_model_output_code")
        ),
        when(_f_is_buy_side === lit(0),
             virtual_log_dw_bid.getField("custom_model_output_code")
        )
      ).as("custom_model_output_code"),
      when(string_compare(col("log_impbus_impressions.external_uid"),
                          lit("---")
           ) =!= lit(0),
           col("log_impbus_impressions.external_uid")
      ).as("external_uid"),
      when(string_compare(col("log_impbus_impressions.request_uuid"),
                          lit("---")
           ) =!= lit(0),
           col("log_impbus_impressions.request_uuid")
      ).as("request_uuid"),
      when(
        col("log_impbus_impressions.mobile_app_instance_id").cast(
          IntegerType
        ) =!= lit(0),
        col("log_impbus_impressions.mobile_app_instance_id").cast(IntegerType)
      ).as("mobile_app_instance_id"),
      when(string_compare(col("log_impbus_impressions.traffic_source_code"),
                          lit("---")
           ) =!= lit(0),
           col("log_impbus_impressions.traffic_source_code")
      ).as("traffic_source_code"),
      when(string_compare(col("log_impbus_impressions.external_request_id"),
                          lit("---")
           ) =!= lit(0),
           col("log_impbus_impressions.external_request_id")
      ).as("external_request_id"),
      when(string_compare(col("log_impbus_impressions.stitch_group_id"),
                          lit("---")
           ) =!= lit(0),
           col("log_impbus_impressions.stitch_group_id")
      ).as("stitch_group_id"),
      deal_type.as("deal_type"),
      f_preempt_over_impression_non_zero_explicit(
        is_not_null(col("log_impbus_preempt")),
        col("log_impbus_impressions.ym_floor_id").cast(IntegerType),
        col("log_impbus_preempt.ym_floor_id").cast(IntegerType)
      ).as("ym_floor_id"),
      f_preempt_over_impression_non_zero_explicit(
        is_not_null(col("log_impbus_preempt")),
        col("log_impbus_impressions.ym_bias_id").cast(IntegerType),
        col("log_impbus_preempt.ym_bias_id").cast(IntegerType)
      ).as("ym_bias_id"),
      when(col("log_dw_bid.bid_priority").cast(IntegerType) =!= lit(0),
           col("log_dw_bid.bid_priority").cast(IntegerType)
      ).as("bid_priority"),
      lit(null)
        .cast(
          StructType(
            List(
              StructField("rate_card_id", IntegerType, true),
              StructField("member_id",    IntegerType, true),
              StructField("is_dw",        BooleanType, true),
              StructField(
                "pricing_terms",
                ArrayType(
                  StructType(
                    List(
                      StructField("term_id",                 IntegerType, true),
                      StructField("amount",                  DoubleType,  true),
                      StructField("rate",                    DoubleType,  true),
                      StructField("is_deduction",            BooleanType, true),
                      StructField("is_media_cost_dependent", BooleanType, true),
                      StructField("data_member_id",          IntegerType, true)
                    )
                  ),
                  true
                ),
                true
              ),
              StructField("fx_margin_rate_id",      IntegerType, true),
              StructField("marketplace_owner_id",   IntegerType, true),
              StructField("virtual_marketplace_id", IntegerType, true),
              StructField("amino_enabled",          BooleanType, true)
            )
          )
        )
        .as("buyer_charges"),
      lit(null)
        .cast(
          StructType(
            List(
              StructField("rate_card_id", IntegerType, true),
              StructField("member_id",    IntegerType, true),
              StructField("is_dw",        BooleanType, true),
              StructField(
                "pricing_terms",
                ArrayType(
                  StructType(
                    List(
                      StructField("term_id",                 IntegerType, true),
                      StructField("amount",                  DoubleType,  true),
                      StructField("rate",                    DoubleType,  true),
                      StructField("is_deduction",            BooleanType, true),
                      StructField("is_media_cost_dependent", BooleanType, true),
                      StructField("data_member_id",          IntegerType, true)
                    )
                  ),
                  true
                ),
                true
              ),
              StructField("fx_margin_rate_id",      IntegerType, true),
              StructField("marketplace_owner_id",   IntegerType, true),
              StructField("virtual_marketplace_id", IntegerType, true),
              StructField("amino_enabled",          BooleanType, true)
            )
          )
        )
        .as("seller_charges"),
      when(col("log_dw_bid.explore_disposition").cast(IntegerType) =!= lit(0),
           col("log_dw_bid.explore_disposition").cast(IntegerType)
      ).as("explore_disposition"),
      when(col("log_impbus_impressions.device_make_id").cast(
             IntegerType
           ) =!= lit(0),
           col("log_impbus_impressions.device_make_id").cast(IntegerType)
      ).as("device_make_id"),
      when(
        col("log_impbus_impressions.operating_system_family_id").cast(
          IntegerType
        ) =!= lit(1),
        col("log_impbus_impressions.operating_system_family_id").cast(
          IntegerType
        )
      ).as("operating_system_family_id"),
      col("log_impbus_impressions.tag_sizes").as("tag_sizes"),
      lit(null).as("campaign_group_models"),
      coalesce(col("log_impbus_impressions_pricing.rate_card_media_type").cast(
                 IntegerType
               ),
               lit(0)
      ).as("pricing_media_type"),
      buyer_trx_event_id.as("buyer_trx_event_id"),
      seller_trx_event_id.as("seller_trx_event_id"),
      coalesce(
        when(
          (_f_is_buy_side === lit(0)).and(
            virtual_log_dw_bid.getField("revenue_auction_event_type") =!= lit(0)
          ),
          virtual_log_dw_bid.getField("revenue_auction_event_type")
        ),
        when(
          (_f_is_buy_side === lit(1)).and(
            col("log_dw_bid.revenue_auction_event_type")
              .cast(IntegerType) =!= lit(0)
          ),
          col("log_dw_bid.revenue_auction_event_type").cast(IntegerType)
        )
      ).as("revenue_auction_event_type"),
      when(col("log_impbus_impressions.is_prebid").cast(ByteType) =!= lit(0),
           col("log_impbus_impressions.is_prebid").cast(BooleanType)
      ).as("is_prebid"),
      coalesce(
        when(imp_type === lit(6), has_seller_transacted.cast(BooleanType)),
        when(imp_type.isin(lit(7), lit(11)),
             has_buyer_transacted.cast(BooleanType)
        ),
        lit(1).cast(BooleanType)
      ).as("is_unit_of_trx"),
      imps_for_budget_caps_pacing.as("imps_for_budget_caps_pacing"),
      col("log_impbus_impressions.date_time")
        .cast(LongType)
        .as("auction_timestamp"),
      when(
        col("log_impbus_impressions_pricing.two_phase_reduction_applied").cast(
          ByteType
        ) =!= lit(0),
        col("log_impbus_impressions_pricing.two_phase_reduction_applied").cast(
          BooleanType
        )
      ).as("two_phase_reduction_applied"),
      when(col("log_impbus_impressions.region_id").cast(IntegerType) =!= lit(0),
           col("log_impbus_impressions.region_id").cast(IntegerType)
      ).as("region_id"),
      when(col("log_impbus_impressions.media_company_id").cast(
             IntegerType
           ) =!= lit(0),
           col("log_impbus_impressions.media_company_id").cast(IntegerType)
      ).as("media_company_id"),
      when(
        col("log_impbus_impressions_pricing.trade_agreement_id").cast(
          IntegerType
        ) =!= lit(0),
        col("log_impbus_impressions_pricing.trade_agreement_id").cast(
          IntegerType
        )
      ).as("trade_agreement_id"),
      lit(null).as("personal_data"),
      coalesce(when(is_not_null(netflix_ppid).cast(BooleanType),
                    f_string_to_anon_user_info(netflix_ppid)
               ),
               col("log_impbus_impressions.anonymized_user_info")
      ).as("anonymized_user_info"),
      when(string_compare(col("log_impbus_impressions.gdpr_consent_cookie"),
                          lit("")
           ) =!= lit(0),
           col("log_impbus_impressions.gdpr_consent_cookie")
      ).as("gdpr_consent_cookie"),
      col("additional_clearing_events").as("additional_clearing_events"),
      col("log_impbus_impressions.fx_rate_snapshot_id")
        .cast(IntegerType)
        .as("fx_rate_snapshot_id"),
      coalesce(
        when(_f_is_buy_side === lit(1),
             col("log_dw_bid.crossdevice_group_anon")
        ),
        when(imp_type === lit(6), col("log_dw_bid_deal.crossdevice_group_anon"))
      ).as("crossdevice_group_anon"),
      coalesce(
        when((buyer_trx_event_id === lit(1)).and(imp_type === lit(6)),
             col("log_dw_bid_deal.crossdevice_graph_cost")
        ),
        when((_f_is_buy_side === lit(1)).and(imp_type =!= lit(6)),
             col("log_dw_bid.crossdevice_graph_cost")
        )
      ).as("crossdevice_graph_cost"),
      when(
        (_f_is_buy_side === lit(1)).and(
          col("log_dw_bid.revenue_event_type_id").cast(IntegerType) =!= lit(0)
        ),
        col("log_dw_bid.revenue_event_type_id").cast(IntegerType)
      ).as("revenue_event_type_id"),
      buyer_trx_event_type_id.as("buyer_trx_event_type_id"),
      seller_trx_event_type_id.as("seller_trx_event_type_id"),
      coalesce(col("log_impbus_preempt.external_creative_id"),
               when(is_not_null(col("log_impbus_preempt")).cast(BooleanType),
                    lit("---")
               )
      ).as("external_creative_id"),
      coalesce(
        when(is_not_null(col("log_dw_bid_deal")).and(imp_type === lit(6)),
             col("log_dw_bid_deal.targeted_segment_details")
        ),
        when(_f_is_buy_side === lit(1),
             col("log_dw_bid.targeted_segment_details")
        )
      ).as("targeted_segment_details"),
      coalesce(
        col("log_impbus_preempt.seat_id").cast(IntegerType),
        when(is_not_null(col("log_impbus_preempt")).cast(BooleanType), lit(0)),
        lit(0)
      ).as("bidder_seat_id"),
      when(col("log_impbus_impressions.is_whiteops_scanned").cast(
             ByteType
           ) =!= lit(0),
           col("log_impbus_impressions.is_whiteops_scanned").cast(BooleanType)
      ).as("is_whiteops_scanned"),
      lit(null).as("default_referrer_url"),
      when(is_curated === lit(1), lit(1).cast(BooleanType)).as("is_curated"),
      curator_member_id.as("curator_member_id"),
      coalesce(
        when(
          (_f_is_buy_side === lit(1))
            .and(
              is_not_null(
                col("log_dw_view.revenue_info.total_partner_fees_microcents")
                  .cast(LongType)
              )
            )
            .and(
              is_not_null(
                col("log_dw_bid.revenue_info.total_partner_fees_microcents")
                  .cast(LongType)
              )
            ),
          col("log_dw_view.revenue_info.total_partner_fees_microcents")
            .cast(LongType) + col(
            "log_dw_bid.revenue_info.total_partner_fees_microcents"
          ).cast(LongType)
        ),
        when(
          (_f_is_buy_side === lit(1)).and(
            is_not_null(
              col("log_dw_bid.revenue_info.total_partner_fees_microcents")
                .cast(LongType)
            )
          ),
          col("log_dw_bid.revenue_info.total_partner_fees_microcents").cast(
            LongType
          )
        ),
        when(_f_is_buy_side === lit(1), lit(0))
      ).as("total_partner_fees_microcents"),
      lit(null).as("net_buyer_spend"),
      when(col("log_impbus_preempt.is_prebid_server").cast(ByteType) =!= lit(0),
           col("log_impbus_preempt.is_prebid_server").cast(BooleanType)
      ).as("is_prebid_server"),
      when(
        (_f_is_buy_side === lit(1)).and(
          col("log_dw_bid.cold_start_price_type").cast(IntegerType) =!= lit(-1)
        ),
        col("log_dw_bid.cold_start_price_type").cast(IntegerType)
      ).as("cold_start_price_type"),
      when((_f_is_buy_side === lit(1)).and(
             col("log_dw_bid.discovery_state").cast(IntegerType) =!= lit(-1)
           ),
           col("log_dw_bid.discovery_state").cast(IntegerType)
      ).as("discovery_state"),
      coalesce(
        when(
          _f_is_buy_side === lit(1),
          col("log_dw_bid.insertion_order_budget_interval_id").cast(IntegerType)
        ),
        billing_period_id,
        lit(0)
      ).as("billing_period_id"),
      coalesce(
        when(
          _f_is_buy_side === lit(1),
          col("log_dw_bid.campaign_group_budget_interval_id").cast(IntegerType)
        ),
        flight_id,
        lit(0)
      ).as("flight_id"),
      split_id.as("split_id"),
      when(
        (imp_type === lit(7)).and(
          v_transaction_event_pricing.getField("buyer_transacted") === lit(1)
        ),
        v_transaction_event_pricing
          .getField("net_payment_value_microcents")
          .cast(DoubleType) / lit(100000.0d)
      ).as("net_media_cost_dollars_cpm"),
      coalesce(
        when(
          (_f_is_buy_side === lit(1))
            .and(
              is_not_null(
                col("log_dw_view.revenue_info.total_data_costs_microcents")
                  .cast(LongType)
              )
            )
            .and(
              is_not_null(
                col("log_dw_bid.revenue_info.total_data_costs_microcents")
                  .cast(LongType)
              )
            ),
          col("log_dw_view.revenue_info.total_data_costs_microcents")
            .cast(LongType) + col(
            "log_dw_bid.revenue_info.total_data_costs_microcents"
          ).cast(LongType)
        ),
        when(
          (_f_is_buy_side === lit(1)).and(
            is_not_null(
              col("log_dw_bid.revenue_info.total_data_costs_microcents")
                .cast(LongType)
            )
          ),
          col("log_dw_bid.revenue_info.total_data_costs_microcents").cast(
            LongType
          )
        ),
        when(_f_is_buy_side === lit(1), lit(0))
      ).as("total_data_costs_microcents"),
      coalesce(
        when(
          (_f_is_buy_side === lit(1))
            .and(
              is_not_null(
                col("log_dw_view.revenue_info.total_profit_microcents")
                  .cast(LongType)
              )
            )
            .and(
              is_not_null(
                col("log_dw_bid.revenue_info.total_profit_microcents")
                  .cast(LongType)
              )
            ),
          col("log_dw_view.revenue_info.total_profit_microcents")
            .cast(LongType) + col(
            "log_dw_bid.revenue_info.total_profit_microcents"
          ).cast(LongType)
        ),
        when(
          (_f_is_buy_side === lit(1)).and(
            is_not_null(
              col("log_dw_bid.revenue_info.total_profit_microcents")
                .cast(LongType)
            )
          ),
          col("log_dw_bid.revenue_info.total_profit_microcents").cast(LongType)
        ),
        when(_f_is_buy_side === lit(1), lit(0))
      ).as("total_profit_microcents"),
      when(
        (_f_is_buy_side === lit(1)).and(
          col("log_dw_bid.targeted_crossdevice_graph_id")
            .cast(IntegerType) =!= lit(0)
        ),
        col("log_dw_bid.targeted_crossdevice_graph_id").cast(IntegerType)
      ).as("targeted_crossdevice_graph_id"),
      when((_f_is_buy_side === lit(1))
             .and(col("log_dw_bid.discovery_prediction") =!= lit(0.0d)),
           col("log_dw_bid.discovery_prediction")
      ).as("discovery_prediction"),
      campaign_group_type_id.as("campaign_group_type_id"),
      when(col("log_impbus_impressions.hb_source").cast(IntegerType) =!= lit(0),
           col("log_impbus_impressions.hb_source").cast(IntegerType)
      ).as("hb_source"),
      f_preempt_over_impression_94298.as("external_campaign_id"),
      when(_f_is_buy_side === lit(1),
           col("log_dw_bid.excluded_targeted_segment_details")
      ).as("excluded_targeted_segment_details"),
      lit(null).cast(StringType).as("trust_id"),
      when((_f_is_buy_side === lit(1))
             .and(col("log_dw_bid.predicted_kpi_event_rate") =!= lit(0.0d)),
           col("log_dw_bid.predicted_kpi_event_rate")
      ).as("predicted_kpi_event_rate"),
      coalesce(
        when(_f_is_buy_side === lit(1),
             col("log_dw_bid.has_crossdevice_reach_extension").cast(BooleanType)
        ),
        when(imp_type === lit(6),
             col("log_dw_bid_deal.has_crossdevice_reach_extension").cast(
               BooleanType
             )
        )
      ).as("has_crossdevice_reach_extension"),
      coalesce(
        when(_f_is_buy_side === lit(1),
             col("log_dw_bid.crossdevice_graph_membership")
        ),
        when(imp_type === lit(6),
             col("log_dw_bid_deal.crossdevice_graph_membership")
        )
      ).as("crossdevice_graph_membership"),
      when(
        _f_is_buy_side === lit(1),
        coalesce(col(
                   "log_dw_bid.revenue_info.total_segment_data_costs_microcents"
                 ).cast(LongType),
                 lit(0)
        ) + coalesce(
          col("log_dw_view.revenue_info.total_segment_data_costs_microcents")
            .cast(LongType),
          lit(0)
        )
      ).otherwise(lit(0)).as("total_segment_data_costs_microcents"),
      when(
        _f_is_buy_side === lit(1),
        coalesce(col("log_dw_bid.revenue_info.total_feature_costs_microcents")
                   .cast(LongType),
                 lit(0)
        ) + coalesce(col(
                       "log_dw_view.revenue_info.total_feature_costs_microcents"
                     ).cast(LongType),
                     lit(0)
        )
      ).otherwise(lit(0)).as("total_feature_costs_microcents"),
      when(
        col("log_impbus_impressions_pricing.counterparty_ruleset_type").cast(
          IntegerType
        ) =!= lit(0),
        col("log_impbus_impressions_pricing.counterparty_ruleset_type").cast(
          IntegerType
        )
      ).as("counterparty_ruleset_type"),
      coalesce(col("log_impbus_preempt.log_product_ads"),
               col("log_impbus_impressions.log_product_ads")
      ).as("log_product_ads"),
      coalesce(when(imp_type.isin(lit(7),                          lit(5)),
                    coalesce(col("log_dw_bid.line_item_currency"), lit("---"))
               ),
               lit("---")
      ).as("buyer_line_item_currency"),
      coalesce(
        when(imp_type === lit(6),
             coalesce(col("log_dw_bid_deal.line_item_currency"), lit("---"))
        ),
        lit("---")
      ).as("deal_line_item_currency"),
      coalesce(when(_f_is_buy_side === lit(1),
                    col("log_dw_bid.measurement_fee_cpm_usd")
               ),
               lit(0)
      ).as("measurement_fee_usd"),
      coalesce(
        when(_f_is_buy_side === lit(1),
             col("log_dw_bid.measurement_provider_member_id").cast(IntegerType)
        ),
        lit(0)
      ).as("measurement_provider_member_id"),
      when(_f_is_buy_side === lit(1),
           col("log_dw_bid.offline_attribution_provider_member_id").cast(
             IntegerType
           )
      ).as("offline_attribution_provider_member_id"),
      when(_f_is_buy_side === lit(1),
           col("log_dw_bid.offline_attribution_cost_usd_cpm")
      ).as("offline_attribution_cost_usd_cpm"),
      when(col("log_impbus_impressions.pred_info").cast(IntegerType) =!= lit(0),
           col("log_impbus_impressions.pred_info").cast(IntegerType)
      ).as("pred_info"),
      col("log_impbus_impressions.imp_rejecter_do_auction")
        .cast(BooleanType)
        .as("imp_rejecter_do_auction"),
      col("log_impbus_impressions.personal_identifiers")
        .as("personal_identifiers"),
      col("log_impbus_impressions.is_imp_rejecter_applied")
        .cast(BooleanType)
        .as("imp_rejecter_applied"),
      lit(null).cast(FloatType).as("ip_derived_latitude"),
      lit(null).cast(FloatType).as("ip_derived_longitude"),
      col("log_impbus_impressions.personal_identifiers_experimental")
        .as("personal_identifiers_experimental"),
      col("log_impbus_impressions.postal_code_ext_id")
        .cast(IntegerType)
        .as("postal_code_ext_id"),
      coalesce(when(buyer_trx_event_id === lit(2),
                    col("log_dw_view.ecpm_conversion_rate")
               ),
               lit(1.0d)
      ).as("ecpm_conversion_rate"),
      when(sup_ip_range_lookup_count > lit(0), lit(1).cast(BooleanType))
        .otherwise(lit(0).cast(BooleanType))
        .as("is_residential_ip"),
      col("log_impbus_impressions.hashed_ip").as("hashed_ip"),
      coalesce(
        when(is_not_null(col("log_dw_bid_deal")).and(imp_type === lit(6)),
             col("log_dw_bid_deal.targeted_segment_details_by_id_type")
        ),
        when(_f_is_buy_side === lit(1),
             col("log_dw_bid.targeted_segment_details_by_id_type")
        )
      ).as("targeted_segment_details_by_id_type"),
      when(_f_is_buy_side === lit(1), col("log_dw_bid.offline_attribution"))
        .as("offline_attribution"),
      col("log_dw_bid.frequency_cap_type_internal")
        .cast(IntegerType)
        .as("frequency_cap_type_internal"),
      col("log_dw_bid.modeled_cap_did_override_line_item_daily_cap")
        .cast(BooleanType)
        .as("modeled_cap_did_override_line_item_daily_cap"),
      col("log_dw_bid.modeled_cap_user_sample_rate")
        .as("modeled_cap_user_sample_rate"),
      when(
        coalesce(col("log_impbus_impressions.device_type").cast(IntegerType),
                 lit(99)
        ) === lit(8),
        coalesce(
          col("log_impbus_impressions_pricing_dup.estimated_audience_imps"),
          lit(1.0d)
        )
      ).otherwise(lit(1.0d)).as("estimated_audience_imps"),
      when(
        coalesce(col("log_impbus_impressions.device_type").cast(IntegerType),
                 lit(99)
        ) === lit(8),
        coalesce(col("log_impbus_impressions_pricing_dup.audience_imps"),
                 lit(1.0d)
        )
      ).otherwise(lit(1.0d)).as("audience_imps"),
      col("log_dw_bid.district_postal_code_lists")
        .as("district_postal_code_lists"),
      coalesce(
        when(imp_type.isin(lit(4), lit(7), lit(5)),
             coalesce(col("log_dw_bid.bidding_host_id").cast(IntegerType),
                      lit(0)
             )
        ),
        when((imp_type === lit(6)).and(is_not_null(col("log_dw_bid_deal"))),
             coalesce(col("log_dw_bid_deal.bidding_host_id").cast(IntegerType),
                      lit(0)
             )
        )
      ).as("bidding_host_id"),
      lit(null).cast(LongType).as("buyer_dpvp_bitmap"),
      lit(null).cast(LongType).as("seller_dpvp_bitmap"),
      col("log_impbus_impressions.browser_code_id")
        .cast(IntegerType)
        .as("browser_code_id"),
      col("log_impbus_impressions.is_prebid_server_included")
        .cast(IntegerType)
        .as("is_prebid_server_included"),
      coalesce(col("log_dw_bid.feature_tests_bitmap").cast(IntegerType), lit(0))
        .as("feature_tests_bitmap"),
      coalesce(col("log_impbus_impressions.private_auction_eligible").cast(
                 BooleanType
               ),
               lit(0).cast(BooleanType)
      ).as("private_auction_eligible"),
      coalesce(
        col("log_impbus_impressions.chrome_traffic_label").cast(IntegerType),
        lit(0)
      ).as("chrome_traffic_label"),
      coalesce(
        col("log_impbus_impressions.is_private_auction").cast(BooleanType),
        lit(0).cast(BooleanType)
      ).as("is_private_auction"),
      f_preempt_over_impression_95337.as("creative_media_subtype_id"),
      col("log_impbus_impressions.allowed_media_types").as(
        "allowed_media_types"
      )
    )

  def f_create_agg_dw_impressions_return_11668614(
    curator_member_id:               Column,
    discrepancy_allowance:           Column,
    should_process_views:            Column,
    buyer_member_id:                 Column,
    imps_for_budget_caps_pacing:     Column,
    imp_type:                        Column,
    is_curated:                      Column,
    view_detection_enabled:          Column,
    buyer_trx_event_type_id:         Column,
    commission_revshare:             Column,
    creative_overage_fees:           Column,
    viewdef_definition_id:           Column,
    virtual_log_dw_bid:              Column,
    has_buyer_transacted:            Column,
    payment_type_normalized:         Column,
    seller_trx_event_id:             Column,
    seller_revenue_cpm:              Column,
    insertion_order_id:              Column,
    revenue_type_normalized:         Column,
    billing_period_id:               Column,
    viewdef_viewable:                Column,
    split_id:                        Column,
    data_costs_deal:                 Column,
    f_preempt_over_impression_94298: Column,
    commission_cpm:                  Column,
    v_transaction_event_pricing:     Column,
    buyer_trx_event_id:              Column,
    campaign_group_id:               Column,
    has_seller_transacted:           Column,
    revenue_value:                   Column,
    netflix_ppid:                    Column,
    campaign_group_type_id:          Column,
    should_zero_seller_revenue:      Column,
    view_measurable:                 Column,
    advertiser_id:                   Column,
    media_cost_dollars_cpm:          Column,
    creative_id:                     Column,
    f_preempt_over_impression_95337: Column,
    flight_id:                       Column,
    viewable:                        Column,
    auction_service_deduction:       Column,
    media_buy_cost:                  Column,
    seller_deduction:                Column,
    buyer_spend_cpm:                 Column,
    seller_trx_event_type_id:        Column,
    auction_service_fees:            Column,
    serving_fees_cpm:                Column,
    data_costs:                      Column,
    sup_ip_range_lookup_count:       Column,
    ttl:                             Column,
    view_result:                     Column,
    is_dw_normalized:                Column,
    booked_revenue_adv_curr:         Column,
    booked_revenue_dollars:          Column,
    deal_id:                         Column,
    _f_is_buy_side:                  Column,
    deal_type:                       Column,
    campaign_id:                     Column,
    is_placeholder_bid:              Column,
    serving_fees_revshare:           Column,
    view_non_measurable_reason:      Column
  ): Column =
    struct(
      coalesce(
        col("log_dw_view.date_time").cast(LongType),
        when((ttl > lit(3600)).and(
               is_not_null(col("log_impbus_preempt.date_time").cast(LongType))
             ),
             col("log_impbus_preempt.date_time").cast(LongType)
        ),
        col("date_time").cast(LongType)
      ).as("date_time"),
      col("auction_id_64").cast(LongType).as("auction_id_64"),
      coalesce(
        when(col("log_dw_view.user_id_64").cast(LongType) =!= lit(0),
             col("log_dw_view.user_id_64").cast(LongType)
        ),
        when(col("log_impbus_impressions.user_id_64").cast(LongType) =!= lit(0),
             col("log_impbus_impressions.user_id_64").cast(LongType)
        )
      ).as("user_id_64"),
      when(col("log_impbus_impressions.tag_id").cast(IntegerType) =!= lit(0),
           col("log_impbus_impressions.tag_id").cast(IntegerType)
      ).as("tag_id"),
      when(col("log_impbus_impressions.venue_id").cast(IntegerType) =!= lit(0),
           col("log_impbus_impressions.venue_id").cast(IntegerType)
      ).as("venue_id"),
      when(col("log_impbus_impressions.inventory_source_id").cast(
             IntegerType
           ) =!= lit(0),
           col("log_impbus_impressions.inventory_source_id").cast(IntegerType)
      ).as("inventory_source_id"),
      lit(null).cast(IntegerType).as("session_frequency"),
      when(string_compare(col("log_impbus_impressions.site_domain"),
                          lit("---")
           ) =!= lit(0),
           col("log_impbus_impressions.site_domain")
      ).as("site_domain"),
      f_preempt_over_impression_non_zero_explicit(
        is_not_null(col("log_impbus_preempt")),
        col("log_impbus_impressions.width").cast(IntegerType),
        col("log_impbus_preempt.width").cast(IntegerType)
      ).as("width"),
      f_preempt_over_impression_non_zero_explicit(
        is_not_null(col("log_impbus_preempt")),
        col("log_impbus_impressions.height").cast(IntegerType),
        col("log_impbus_preempt.height").cast(IntegerType)
      ).as("height"),
      when(string_compare(col("log_impbus_impressions.geo_country"),
                          lit("--")
           ) =!= lit(0),
           col("log_impbus_impressions.geo_country")
      ).as("geo_country"),
      when(string_compare(col("log_impbus_impressions.geo_region"),
                          lit("--")
           ) =!= lit(0),
           col("log_impbus_impressions.geo_region")
      ).as("geo_region"),
      when(string_compare(col("log_impbus_impressions.gender"),
                          lit("u")
           ) =!= lit(0),
           col("log_impbus_impressions.gender")
      ).as("gender"),
      when(col("log_impbus_impressions.age").cast(IntegerType) =!= lit(0),
           col("log_impbus_impressions.age").cast(IntegerType)
      ).as("age"),
      col("seller_member_id").cast(IntegerType).as("seller_member_id"),
      when(col("imp_type").cast(IntegerType) =!= lit(1), buyer_member_id)
        .otherwise(lit(0))
        .as("buyer_member_id"),
      creative_id.as("creative_id"),
      when(string_compare(col("log_impbus_impressions.seller_currency"),
                          lit("")
           ) =!= lit(0),
           col("log_impbus_impressions.seller_currency")
      ).as("seller_currency"),
      when(string_compare(col("log_impbus_impressions.buyer_currency"),
                          lit("")
           ) =!= lit(0),
           col("log_impbus_impressions.buyer_currency")
      ).as("buyer_currency"),
      f_preempt_over_impression_non_zero_explicit(
        is_not_null(col("log_impbus_preempt")),
        col("log_impbus_impressions.buyer_bid"),
        col("log_impbus_preempt.buyer_bid")
      ).as("buyer_bid"),
      coalesce(when(imp_type.isin(lit(8), lit(1), lit(3)), lit(0)),
               when(imp_type === lit(5), col("log_dw_bid.price")),
               buyer_spend_cpm
      ).as("buyer_spend"),
      when(col("log_impbus_impressions.ecp") =!= lit(0),
           col("log_impbus_impressions.ecp")
      ).as("ecp"),
      when(col("log_impbus_impressions.reserve_price") =!= lit(0.0d),
           col("log_impbus_impressions.reserve_price")
      ).as("reserve_price"),
      coalesce(when((col("imp_type").cast(IntegerType) === lit(1))
                      .or(buyer_member_id === lit(0))
                      .and(is_not_null(advertiser_id)),
                    lit(0)
               ),
               advertiser_id
      ).as("advertiser_id"),
      coalesce(when((col("imp_type").cast(IntegerType) === lit(1))
                      .or(buyer_member_id === lit(0))
                      .and(is_not_null(campaign_group_id)),
                    lit(0)
               ),
               campaign_group_id
      ).as("campaign_group_id"),
      coalesce(
        when((col("imp_type").cast(IntegerType) === lit(1))
               .or(buyer_member_id === lit(0))
               .and(is_not_null(campaign_id)),
             lit(0)
        ),
        when((imp_type =!= lit(6)).and(campaign_id =!= lit(0)), campaign_id)
      ).as("campaign_id"),
      coalesce(
        when((_f_is_buy_side === lit(1)).and(
               col("log_dw_bid.creative_freq").cast(IntegerType) =!= lit(0)
             ),
             col("log_dw_bid.creative_freq").cast(IntegerType)
        ),
        when(_f_is_buy_side === lit(0),
             virtual_log_dw_bid.getField("creative_freq")
        )
      ).as("creative_freq"),
      coalesce(
        when((_f_is_buy_side === lit(1)).and(
               col("log_dw_bid.creative_rec").cast(IntegerType) =!= lit(0)
             ),
             col("log_dw_bid.creative_rec").cast(IntegerType)
        ),
        when(_f_is_buy_side === lit(0),
             virtual_log_dw_bid.getField("creative_rec")
        )
      ).as("creative_rec"),
      coalesce(
        when((_f_is_buy_side === lit(1)).and(
               col("log_dw_bid.predict_type").cast(IntegerType) === lit(1)
             ),
             lit(1)
        ),
        when(
          (_f_is_buy_side === lit(1)).and(
            (col("log_dw_bid.predict_type").cast(IntegerType) === lit(0)).or(
              (col("log_dw_bid.predict_type").cast(IntegerType) >= lit(2)).and(
                col("log_dw_bid.predict_type").cast(IntegerType) =!= lit(9)
              )
            )
          ),
          lit(2)
        ),
        lit(0)
      ).as("is_learn"),
      when((_f_is_buy_side === lit(1)).and(
             col("log_dw_bid.is_remarketing").cast(IntegerType) =!= lit(0)
           ),
           col("log_dw_bid.is_remarketing").cast(IntegerType)
      ).as("is_remarketing"),
      coalesce(
        when((_f_is_buy_side === lit(1)).and(
               col("log_dw_bid.advertiser_freq").cast(IntegerType) =!= lit(0)
             ),
             col("log_dw_bid.advertiser_freq").cast(IntegerType)
        ),
        when((_f_is_buy_side === lit(1)).and(imp_type === lit(9)), lit(0)),
        when(_f_is_buy_side === lit(0),
             virtual_log_dw_bid.getField("advertiser_freq")
        )
      ).as("advertiser_frequency"),
      coalesce(
        when((_f_is_buy_side === lit(1)).and(
               col("log_dw_bid.advertiser_rec").cast(IntegerType) =!= lit(0)
             ),
             col("log_dw_bid.advertiser_rec").cast(IntegerType)
        ),
        when((_f_is_buy_side === lit(1)).and(imp_type === lit(9)), lit(0)),
        when(_f_is_buy_side === lit(0),
             virtual_log_dw_bid.getField("advertiser_rec")
        )
      ).as("advertiser_recency"),
      coalesce(
        when(col("log_impbus_impressions.user_id_64").cast(LongType) > lit(
               9223372036854775807L
             ).cast(LongType),
             lit(-1)
        ),
        when(
          (is_placeholder_bid === lit(1))
            .or(col("imp_type").cast(IntegerType) === lit(1))
            .or(col("imp_type").cast(IntegerType) === lit(3))
            .or(imp_type === lit(8))
            .and(
              is_not_null(
                col("log_impbus_impressions.user_id_64").cast(LongType)
              )
            ),
          col("log_impbus_impressions.user_id_64").cast(LongType) % lit(1000)
        ),
        when(
          (virtual_log_dw_bid.getField("log_type") === lit(2)).or(
            is_not_null(col("log_dw_bid.log_type").cast(IntegerType))
              .and(col("log_dw_bid.log_type").cast(IntegerType) === lit(2))
          ),
          col("log_impbus_impressions.user_group_id").cast(IntegerType)
        ),
        col("log_dw_bid.user_group_id").cast(IntegerType)
      ).as("user_group_id"),
      lit(null).cast(IntegerType).as("camp_dp_id"),
      coalesce(when(imp_type.isin(lit(8), lit(1), lit(3)), lit(0)),
               col("publisher_id").cast(IntegerType)
      ).as("media_buy_id"),
      media_buy_cost.as("media_buy_cost"),
      f_preempt_over_impression_non_zero_explicit(
        is_not_null(col("log_impbus_preempt")),
        col("log_impbus_impressions.brand_id").cast(IntegerType),
        col("log_impbus_preempt.brand_id").cast(IntegerType)
      ).as("brand_id"),
      f_preempt_over_impression_non_zero_explicit(
        is_not_null(col("log_impbus_preempt")),
        col("log_impbus_impressions.cleared_direct").cast(IntegerType),
        col("log_impbus_preempt.cleared_direct").cast(IntegerType)
      ).as("cleared_direct"),
      lit(null).cast(DoubleType).as("clear_fees"),
      when(imp_type =!= lit(9),
           col("log_impbus_impressions.media_buy_rev_share_pct")
      ).otherwise(col("log_dw_bid.media_buy_rev_share_pct"))
        .as("media_buy_rev_share_pct"),
      revenue_value.as("revenue_value"),
      coalesce(
        when(
          (_f_is_buy_side === lit(1)).and(
            string_compare(col("log_dw_bid.pricing_type"), lit("--")) =!= lit(0)
          ),
          col("log_dw_bid.pricing_type")
        ),
        when(_f_is_buy_side === lit(0),
             virtual_log_dw_bid.getField("pricing_type")
        )
      ).as("pricing_type"),
      when((_f_is_buy_side === lit(1))
             .and(col("log_dw_bid.can_convert").cast(IntegerType) =!= lit(0)),
           col("log_dw_bid.can_convert").cast(IntegerType)
      ).as("can_convert"),
      coalesce(
        when(
          (is_placeholder_bid === lit(1))
            .or(col("imp_type").cast(IntegerType) === lit(1))
            .or(col("imp_type").cast(IntegerType) === lit(3))
            .or(imp_type === lit(8)),
          lit(0)
        ),
        col("log_impbus_impressions.pub_rule_id").cast(IntegerType)
      ).as("pub_rule_id"),
      coalesce(
        when((_f_is_buy_side === lit(1))
               .and(col("log_dw_bid.is_control").cast(IntegerType) =!= lit(0)),
             col("log_dw_bid.is_control").cast(IntegerType)
        ),
        when((_f_is_buy_side === lit(0))
               .and(virtual_log_dw_bid.getField("is_control") =!= lit(0)),
             virtual_log_dw_bid.getField("is_control")
        )
      ).as("is_control"),
      when(col("log_dw_bid.control_pct") =!= lit(0),
           col("log_dw_bid.control_pct")
      ).as("control_pct"),
      when(col("log_dw_bid.control_creative_id").cast(IntegerType) =!= lit(0),
           col("log_dw_bid.control_creative_id").cast(IntegerType)
      ).as("control_creative_id"),
      lit(null).cast(DoubleType).as("predicted_cpm"),
      coalesce(
        when(
          (_f_is_buy_side === lit(1)).and(col("log_dw_bid.price") > lit(999)),
          lit(999)
        ),
        when(
          (_f_is_buy_side === lit(1)).and(col("log_dw_bid.price") <= lit(999)),
          col("log_dw_bid.price")
        ),
        lit(0)
      ).as("actual_bid"),
      when(col("log_impbus_impressions.site_id").cast(IntegerType) =!= lit(0),
           col("log_impbus_impressions.site_id").cast(IntegerType)
      ).as("site_id"),
      when(col("log_impbus_impressions.content_category_id").cast(
             IntegerType
           ) =!= lit(0),
           col("log_impbus_impressions.content_category_id").cast(IntegerType)
      ).as("content_category_id"),
      coalesce(
        when((imp_type === lit(6))
               .and(is_dw_normalized === lit(1))
               .and(should_process_views === lit(1)),
             lit(0)
        ),
        when((imp_type === lit(7))
               .and(is_dw_normalized === lit(1))
               .and(should_process_views === lit(1)),
             auction_service_fees
        ),
        when((imp_type === lit(7))
               .and(is_dw_normalized === lit(0))
               .and(should_process_views === lit(1)),
             auction_service_fees
        ),
        when(f_is_non_cpm_payment_or_payment(imp_type,
                                             payment_type_normalized,
                                             revenue_type_normalized
             ) === lit(1),
             lit(0)
        ).otherwise(auction_service_fees)
      ).as("auction_service_fees"),
      coalesce(
        when((imp_type === lit(6))
               .and(is_dw_normalized === lit(1))
               .and(should_process_views === lit(1)),
             lit(0)
        ),
        when((imp_type === lit(7))
               .and(is_dw_normalized === lit(1))
               .and(should_process_views === lit(1)),
             discrepancy_allowance
        ),
        when((imp_type === lit(7))
               .and(is_dw_normalized === lit(0))
               .and(should_process_views === lit(1)),
             discrepancy_allowance
        ),
        when(f_is_non_cpm_payment_or_payment(imp_type,
                                             payment_type_normalized,
                                             revenue_type_normalized
             ) === lit(1),
             lit(0)
        ).otherwise(discrepancy_allowance)
      ).as("discrepancy_allowance"),
      lit(null).cast(DoubleType).as("forex_allowance"),
      coalesce(
        when((imp_type === lit(6))
               .and(is_dw_normalized === lit(1))
               .and(should_process_views === lit(1)),
             lit(0)
        ),
        when((imp_type === lit(7))
               .and(is_dw_normalized === lit(1))
               .and(should_process_views === lit(1)),
             creative_overage_fees
        ),
        when((imp_type === lit(7))
               .and(is_dw_normalized === lit(0))
               .and(should_process_views === lit(1)),
             creative_overage_fees
        ),
        when(f_is_non_cpm_payment_or_payment(imp_type,
                                             payment_type_normalized,
                                             revenue_type_normalized
             ) === lit(1),
             lit(0)
        ).otherwise(creative_overage_fees)
      ).as("creative_overage_fees"),
      coalesce(col("log_impbus_impressions.fold_position").cast(IntegerType),
               lit(0)
      ).as("fold_position"),
      when(col("log_impbus_impressions.external_inv_id").cast(
             IntegerType
           ) =!= lit(0),
           col("log_impbus_impressions.external_inv_id").cast(IntegerType)
      ).as("external_inv_id"),
      coalesce(
        when((_f_is_buy_side === lit(1))
               .and(col("log_dw_bid.cadence_modifier") =!= lit(0)),
             col("log_dw_bid.cadence_modifier")
        ),
        when((_f_is_buy_side === lit(0))
               .and(virtual_log_dw_bid.getField("cadence_modifier") =!= lit(0)),
             virtual_log_dw_bid.getField("cadence_modifier")
        )
      ).as("cadence_modifier"),
      col("imp_type").cast(IntegerType).as("imp_type"),
      coalesce(
        when(_f_is_buy_side === lit(1), col("log_dw_bid.advertiser_currency")),
        lit("USD")
      ).as("advertiser_currency"),
      coalesce(
        when(
          (_f_is_buy_side === lit(1))
            .and(is_not_null(col("log_dw_bid.advertiser_exchange_rate")))
            .and(col("log_dw_bid.advertiser_exchange_rate") =!= lit(1.0d))
            .and(col("log_dw_bid.advertiser_exchange_rate") =!= lit(0.0d)),
          col("log_dw_bid.advertiser_exchange_rate")
        ),
        lit(1.0d)
      ).as("advertiser_exchange_rate"),
      col("log_impbus_impressions.ip_address").as("ip_address"),
      when(
        col("log_impbus_impressions.publisher_id").cast(IntegerType) =!= lit(0),
        col("log_impbus_impressions.publisher_id").cast(IntegerType)
      ).as("publisher_id"),
      coalesce(
        when((imp_type === lit(6))
               .and(is_dw_normalized === lit(1))
               .and(should_process_views === lit(1)),
             lit(0)
        ),
        when((imp_type === lit(7))
               .and(is_dw_normalized === lit(1))
               .and(should_process_views === lit(1)),
             auction_service_deduction
        ),
        when((imp_type === lit(7))
               .and(is_dw_normalized === lit(0))
               .and(should_process_views === lit(1)),
             auction_service_deduction
        ),
        when(f_is_non_cpm_payment_or_payment(imp_type,
                                             payment_type_normalized,
                                             revenue_type_normalized
             ) === lit(1),
             lit(0)
        ).otherwise(auction_service_deduction)
      ).as("auction_service_deduction"),
      coalesce(
        when((col("imp_type").cast(IntegerType) === lit(1))
               .or(buyer_member_id === lit(0))
               .and(is_not_null(insertion_order_id)),
             lit(0)
        ),
        when(insertion_order_id =!= lit(0), insertion_order_id)
      ).as("insertion_order_id"),
      coalesce(
        when(_f_is_buy_side === lit(1),
             col("log_dw_bid.predict_type").cast(IntegerType)
        ),
        when(_f_is_buy_side === lit(0),
             virtual_log_dw_bid.getField("predict_type")
        )
      ).as("predict_type_rev"),
      coalesce(
        when((_f_is_buy_side === lit(1)).and(
               col("log_dw_bid.predict_type_goal").cast(IntegerType) =!= lit(0)
             ),
             col("log_dw_bid.predict_type_goal").cast(IntegerType)
        ),
        when(_f_is_buy_side === lit(0),
             virtual_log_dw_bid.getField("predict_type_goal")
        )
      ).as("predict_type_goal"),
      coalesce(
        when(_f_is_buy_side === lit(1),
             col("log_dw_bid.predict_type").cast(IntegerType)
        ),
        when(_f_is_buy_side === lit(0),
             virtual_log_dw_bid.getField("predict_type")
        )
      ).as("predict_type_cost"),
      booked_revenue_dollars.as("booked_revenue_dollars"),
      booked_revenue_adv_curr.as("booked_revenue_adv_curr"),
      when(commission_cpm =!= lit(0),      commission_cpm).as("commission_cpm"),
      when(commission_revshare =!= lit(0), commission_revshare)
        .as("commission_revshare"),
      when(serving_fees_cpm =!= lit(0), serving_fees_cpm)
        .as("serving_fees_cpm"),
      when(serving_fees_revshare =!= lit(0), serving_fees_revshare)
        .as("serving_fees_revshare"),
      when(col("log_impbus_impressions.user_tz_offset").cast(
             IntegerType
           ) =!= lit(0),
           col("log_impbus_impressions.user_tz_offset").cast(IntegerType)
      ).as("user_tz_offset"),
      f_preempt_over_impression_non_zero_explicit(
        is_not_null(col("log_impbus_preempt.media_type").cast(IntegerType)),
        col("log_impbus_impressions.media_type").cast(IntegerType),
        col("log_impbus_preempt.media_type").cast(IntegerType)
      ).as("media_type"),
      when(col("log_impbus_impressions.operating_system").cast(
             IntegerType
           ) =!= lit(0),
           col("log_impbus_impressions.operating_system").cast(IntegerType)
      ).as("operating_system"),
      when(col("log_impbus_impressions.browser").cast(IntegerType) =!= lit(0),
           col("log_impbus_impressions.browser").cast(IntegerType)
      ).as("browser"),
      when(col("log_impbus_impressions.language").cast(IntegerType) =!= lit(0),
           col("log_impbus_impressions.language").cast(IntegerType)
      ).as("language"),
      col("log_impbus_impressions.seller_currency").as("publisher_currency"),
      coalesce(
        when(
          is_not_null(col("log_impbus_impressions.seller_exchange_rate")).and(
            col("log_impbus_impressions.seller_exchange_rate") =!= lit(1.0d)
          ),
          col("log_impbus_impressions.seller_exchange_rate")
        ),
        lit(1.0d)
      ).as("publisher_exchange_rate"),
      media_cost_dollars_cpm.as("media_cost_dollars_cpm"),
      coalesce(
        when(
          (is_placeholder_bid === lit(1)).or(
            (col("imp_type").cast(IntegerType) === lit(1))
              .or(col("imp_type").cast(IntegerType) === lit(3))
              .or(imp_type === lit(8))
          ),
          lit(0)
        ),
        payment_type_normalized
      ).as("payment_type"),
      coalesce(
        when(
          (is_placeholder_bid === lit(1)).or(
            (col("imp_type").cast(IntegerType) === lit(1))
              .or(imp_type === lit(2))
              .or(col("imp_type").cast(IntegerType) === lit(3))
              .or(imp_type === lit(8))
          ),
          lit(-1)
        ),
        revenue_type_normalized
      ).as("revenue_type"),
      coalesce(
        when(should_zero_seller_revenue.cast(BooleanType), lit(0)),
        when(
          (v_transaction_event_pricing
            .getField("buyer_charges")
            .getField("is_dw") === lit(1)).and(
            col("seller_member_id").cast(IntegerType) === col("buyer_member_id")
              .cast(IntegerType)
          ),
          seller_revenue_cpm
        ),
        when(_f_is_buy_side === lit(0), seller_revenue_cpm),
        when(
          not(
            (v_transaction_event_pricing
              .getField("buyer_charges")
              .getField("is_dw") === lit(1)).and(
              col("seller_member_id")
                .cast(IntegerType) === col("buyer_member_id").cast(IntegerType)
            )
          ),
          lit(0)
        ),
        when(imp_type === lit(9), lit(0))
      ).as("seller_revenue_cpm"),
      coalesce(
        col("log_impbus_preempt.bidder_id").cast(IntegerType),
        when(is_not_null(col("log_impbus_preempt")).cast(BooleanType), lit(0)),
        col("log_impbus_impressions.bidder_id").cast(IntegerType)
      ).as("bidder_id"),
      when(string_compare(col("log_impbus_impressions.inv_code"),
                          lit("---")
           ) =!= lit(0),
           col("log_impbus_impressions.inv_code")
      ).as("inv_code"),
      when(string_compare(col("log_impbus_impressions.application_id"),
                          lit("---")
           ) =!= lit(0),
           col("log_impbus_impressions.application_id")
      ).as("application_id"),
      when(col("log_impbus_impressions.shadow_price") =!= lit(0.0d),
           col("log_impbus_impressions.shadow_price")
      ).as("shadow_price"),
      when(col("log_impbus_impressions.eap") =!= lit(0),
           col("log_impbus_impressions.eap")
      ).as("eap"),
      when(col("log_impbus_impressions.datacenter_id").cast(
             IntegerType
           ) =!= lit(0),
           col("log_impbus_impressions.datacenter_id").cast(IntegerType)
      ).as("datacenter_id"),
      when(
        col("log_impbus_impressions.imp_blacklist_or_fraud").cast(
          IntegerType
        ) =!= lit(0),
        col("log_impbus_impressions.imp_blacklist_or_fraud").cast(IntegerType)
      ).as("imp_blacklist_or_fraud"),
      f_preempt_over_impression_non_zero_explicit(
        is_not_null(col("log_impbus_preempt")),
        col("log_impbus_impressions.vp_expose_domains").cast(IntegerType),
        col("log_impbus_preempt.vp_expose_domains").cast(IntegerType)
      ).as("vp_expose_domains"),
      f_preempt_over_impression_non_zero_explicit(
        is_not_null(col("log_impbus_preempt")),
        col("log_impbus_impressions.vp_expose_categories").cast(IntegerType),
        col("log_impbus_preempt.vp_expose_categories").cast(IntegerType)
      ).as("vp_expose_categories"),
      f_preempt_over_impression_non_zero_explicit(
        is_not_null(col("log_impbus_preempt")),
        col("log_impbus_impressions.vp_expose_pubs").cast(IntegerType),
        col("log_impbus_preempt.vp_expose_pubs").cast(IntegerType)
      ).as("vp_expose_pubs"),
      f_preempt_over_impression_non_zero_explicit(
        is_not_null(col("log_impbus_preempt")),
        col("log_impbus_impressions.vp_expose_tag").cast(IntegerType),
        col("log_impbus_preempt.vp_expose_tag").cast(IntegerType)
      ).as("vp_expose_tag"),
      when(col("log_dw_bid.vp_expose_age").cast(IntegerType) =!= lit(0),
           col("log_dw_bid.vp_expose_age").cast(IntegerType)
      ).as("vp_expose_age"),
      when(col("log_dw_bid.vp_expose_gender").cast(IntegerType) =!= lit(0),
           col("log_dw_bid.vp_expose_gender").cast(IntegerType)
      ).as("vp_expose_gender"),
      when(col("log_impbus_impressions.inventory_url_id").cast(
             IntegerType
           ) =!= lit(0),
           col("log_impbus_impressions.inventory_url_id").cast(IntegerType)
      ).as("inventory_url_id"),
      when(
        col("log_impbus_impressions.audit_type").cast(IntegerType) =!= lit(0),
        col("log_impbus_impressions.audit_type").cast(IntegerType)
      ).as("audit_type"),
      when(
        col("log_impbus_impressions.is_exclusive").cast(IntegerType) =!= lit(0),
        col("log_impbus_impressions.is_exclusive").cast(IntegerType)
      ).as("is_exclusive"),
      when(
        col("log_impbus_impressions.truncate_ip").cast(IntegerType) =!= lit(0),
        col("log_impbus_impressions.truncate_ip").cast(IntegerType)
      ).as("truncate_ip"),
      when(col("log_impbus_impressions.device_id").cast(IntegerType) =!= lit(0),
           col("log_impbus_impressions.device_id").cast(IntegerType)
      ).as("device_id"),
      when(
        col("log_impbus_impressions.carrier_id").cast(IntegerType) =!= lit(0),
        col("log_impbus_impressions.carrier_id").cast(IntegerType)
      ).as("carrier_id"),
      f_preempt_over_impression_non_zero_explicit(
        is_not_null(col("log_impbus_preempt")),
        col("log_impbus_impressions.creative_audit_status").cast(IntegerType),
        col("log_impbus_preempt.creative_audit_status").cast(IntegerType)
      ).as("creative_audit_status"),
      f_preempt_over_impression_non_zero_explicit(
        is_not_null(col("log_impbus_preempt")),
        col("log_impbus_impressions.is_creative_hosted").cast(IntegerType),
        col("log_impbus_preempt.is_creative_hosted").cast(IntegerType)
      ).as("is_creative_hosted"),
      coalesce(when(should_zero_seller_revenue.cast(BooleanType), lit(0)),
               seller_deduction
      ).as("seller_deduction"),
      when(col("log_impbus_impressions.city").cast(IntegerType) =!= lit(0),
           col("log_impbus_impressions.city").cast(IntegerType)
      ).as("city"),
      when(string_compare(col("log_impbus_impressions.latitude"),
                          lit("")
           ) =!= lit(0),
           col("log_impbus_impressions.latitude")
      ).as("latitude"),
      when(string_compare(col("log_impbus_impressions.longitude"),
                          lit("")
           ) =!= lit(0),
           col("log_impbus_impressions.longitude")
      ).as("longitude"),
      when(string_compare(col("log_impbus_impressions.device_unique_id"),
                          lit("")
           ) =!= lit(0),
           col("log_impbus_impressions.device_unique_id")
      ).as("device_unique_id"),
      when(
        col("log_impbus_impressions.package_id").cast(IntegerType) =!= lit(0),
        col("log_impbus_impressions.package_id").cast(IntegerType)
      ).as("package_id"),
      when((_f_is_buy_side === lit(1)).and(
             string_compare(col("log_dw_bid.targeted_segments"),
                            lit("")
             ) =!= lit(0)
           ),
           col("log_dw_bid.targeted_segments")
      ).as("targeted_segments"),
      when(
        col("log_impbus_impressions.supply_type").cast(IntegerType) =!= lit(0),
        col("log_impbus_impressions.supply_type").cast(IntegerType)
      ).as("supply_type"),
      when(
        col("log_impbus_impressions.is_toolbar").cast(IntegerType) =!= lit(0),
        col("log_impbus_impressions.is_toolbar").cast(IntegerType)
      ).as("is_toolbar"),
      deal_id.as("deal_id"),
      f_preempt_over_impression_non_zero_explicit(
        is_not_null(col("log_impbus_preempt")),
        col("log_impbus_impressions.vp_bitmap").cast(LongType),
        col("log_impbus_preempt.vp_bitmap").cast(LongType)
      ).as("vp_bitmap"),
      view_detection_enabled.as("view_detection_enabled"),
      view_result.as("view_result"),
      when(col("log_impbus_impressions.ozone_id").cast(IntegerType) =!= lit(0),
           col("log_impbus_impressions.ozone_id").cast(IntegerType)
      ).as("ozone_id"),
      when(col("log_impbus_impressions.is_performance").cast(
             IntegerType
           ) =!= lit(0),
           col("log_impbus_impressions.is_performance").cast(IntegerType)
      ).as("is_performance"),
      when(string_compare(col("log_impbus_impressions.sdk_version"),
                          lit("---")
           ) =!= lit(0),
           col("log_impbus_impressions.sdk_version")
      ).as("sdk_version"),
      when(
        col("log_impbus_impressions.inventory_session_frequency").cast(
          IntegerType
        ) =!= lit(0),
        col("log_impbus_impressions.inventory_session_frequency").cast(
          IntegerType
        )
      ).as("inventory_session_frequency"),
      when(
        col("log_impbus_impressions.device_type").cast(IntegerType) =!= lit(0),
        col("log_impbus_impressions.device_type").cast(IntegerType)
      ).as("device_type"),
      when(col("log_impbus_impressions.dma").cast(IntegerType) =!= lit(0),
           col("log_impbus_impressions.dma").cast(IntegerType)
      ).as("dma"),
      when(string_compare(col("log_impbus_impressions.postal"),
                          lit("---")
           ) =!= lit(0),
           col("log_impbus_impressions.postal")
      ).as("postal"),
      viewdef_definition_id.as("viewdef_definition_id"),
      viewdef_viewable.as("viewdef_viewable"),
      view_measurable.as("view_measurable"),
      viewable.as("viewable"),
      when(col("log_impbus_impressions.is_secure").cast(IntegerType) =!= lit(0),
           col("log_impbus_impressions.is_secure").cast(IntegerType)
      ).as("is_secure"),
      view_non_measurable_reason.as("view_non_measurable_reason"),
      coalesce(when((buyer_trx_event_id === lit(1)).and(imp_type === lit(6)),
                    data_costs_deal
               ),
               when(_f_is_buy_side === lit(1), data_costs)
      ).as("data_costs"),
      when(col("log_impbus_impressions.bidder_instance_id").cast(
             IntegerType
           ) =!= lit(0),
           col("log_impbus_impressions.bidder_instance_id").cast(IntegerType)
      ).as("bidder_instance_id"),
      coalesce(
        when(
          (_f_is_buy_side === lit(1)).and(
            col("log_dw_bid.campaign_group_freq").cast(IntegerType) =!= lit(-2)
          ),
          col("log_dw_bid.campaign_group_freq").cast(IntegerType)
        ),
        when((_f_is_buy_side === lit(0)).and(
               virtual_log_dw_bid.getField("campaign_group_freq") =!= lit(-2)
             ),
             virtual_log_dw_bid.getField("campaign_group_freq")
        )
      ).as("campaign_group_freq"),
      coalesce(
        when((_f_is_buy_side === lit(1)).and(
               col("log_dw_bid.campaign_group_rec").cast(IntegerType) =!= lit(0)
             ),
             col("log_dw_bid.campaign_group_rec").cast(IntegerType)
        ),
        when(_f_is_buy_side === lit(0),
             virtual_log_dw_bid.getField("campaign_group_rec")
        )
      ).as("campaign_group_rec"),
      coalesce(
        when(
          (_f_is_buy_side === lit(1)).and(
            col("log_dw_bid.insertion_order_freq").cast(IntegerType) =!= lit(-2)
          ),
          col("log_dw_bid.insertion_order_freq").cast(IntegerType)
        ),
        when((_f_is_buy_side === lit(0)).and(
               virtual_log_dw_bid.getField("insertion_order_freq") =!= lit(-2)
             ),
             virtual_log_dw_bid.getField("insertion_order_freq")
        )
      ).as("insertion_order_freq"),
      coalesce(
        when(
          (_f_is_buy_side === lit(1)).and(
            col("log_dw_bid.insertion_order_rec").cast(IntegerType) =!= lit(0)
          ),
          col("log_dw_bid.insertion_order_rec").cast(IntegerType)
        ),
        when(_f_is_buy_side === lit(0),
             virtual_log_dw_bid.getField("insertion_order_rec")
        )
      ).as("insertion_order_rec"),
      when((_f_is_buy_side === lit(1)).and(
             string_compare(col("log_dw_bid.buyer_gender"), lit("u")) =!= lit(0)
           ),
           col("log_dw_bid.buyer_gender")
      ).as("buyer_gender"),
      coalesce(
        when(
          (_f_is_buy_side === lit(1))
            .and(is_not_null(col("log_dw_bid.buyer_age").cast(IntegerType)))
            .and(col("log_dw_bid.buyer_age").cast(IntegerType) =!= lit(-1)),
          col("log_dw_bid.buyer_age").cast(IntegerType)
        ),
        lit(0)
      ).as("buyer_age"),
      when((_f_is_buy_side === lit(1))
             .and(is_not_null(col("log_dw_bid.targeted_segment_list"))),
           col("log_dw_bid.targeted_segment_list")
      ).as("targeted_segment_list"),
      when((_f_is_buy_side === lit(1)).and(
             col("log_dw_bid.custom_model_id").cast(IntegerType) =!= lit(0)
           ),
           col("log_dw_bid.custom_model_id").cast(IntegerType)
      ).as("custom_model_id"),
      when(
        (_f_is_buy_side === lit(1)).and(
          col("log_dw_bid.custom_model_last_modified").cast(LongType) =!= lit(0)
        ),
        col("log_dw_bid.custom_model_last_modified").cast(LongType)
      ).as("custom_model_last_modified"),
      coalesce(
        when((_f_is_buy_side === lit(1)).and(
               string_compare(col("log_dw_bid.custom_model_output_code"),
                              lit("")
               ) =!= lit(0)
             ),
             col("log_dw_bid.custom_model_output_code")
        ),
        when(_f_is_buy_side === lit(0),
             virtual_log_dw_bid.getField("custom_model_output_code")
        )
      ).as("custom_model_output_code"),
      when(string_compare(col("log_impbus_impressions.external_uid"),
                          lit("---")
           ) =!= lit(0),
           col("log_impbus_impressions.external_uid")
      ).as("external_uid"),
      when(string_compare(col("log_impbus_impressions.request_uuid"),
                          lit("---")
           ) =!= lit(0),
           col("log_impbus_impressions.request_uuid")
      ).as("request_uuid"),
      when(
        col("log_impbus_impressions.mobile_app_instance_id").cast(
          IntegerType
        ) =!= lit(0),
        col("log_impbus_impressions.mobile_app_instance_id").cast(IntegerType)
      ).as("mobile_app_instance_id"),
      when(string_compare(col("log_impbus_impressions.traffic_source_code"),
                          lit("---")
           ) =!= lit(0),
           col("log_impbus_impressions.traffic_source_code")
      ).as("traffic_source_code"),
      when(string_compare(col("log_impbus_impressions.external_request_id"),
                          lit("---")
           ) =!= lit(0),
           col("log_impbus_impressions.external_request_id")
      ).as("external_request_id"),
      when(string_compare(col("log_impbus_impressions.stitch_group_id"),
                          lit("---")
           ) =!= lit(0),
           col("log_impbus_impressions.stitch_group_id")
      ).as("stitch_group_id"),
      deal_type.as("deal_type"),
      f_preempt_over_impression_non_zero_explicit(
        is_not_null(col("log_impbus_preempt")),
        col("log_impbus_impressions.ym_floor_id").cast(IntegerType),
        col("log_impbus_preempt.ym_floor_id").cast(IntegerType)
      ).as("ym_floor_id"),
      f_preempt_over_impression_non_zero_explicit(
        is_not_null(col("log_impbus_preempt")),
        col("log_impbus_impressions.ym_bias_id").cast(IntegerType),
        col("log_impbus_preempt.ym_bias_id").cast(IntegerType)
      ).as("ym_bias_id"),
      when(col("log_dw_bid.bid_priority").cast(IntegerType) =!= lit(0),
           col("log_dw_bid.bid_priority").cast(IntegerType)
      ).as("bid_priority"),
      lit(null)
        .cast(
          StructType(
            List(
              StructField("rate_card_id", IntegerType, true),
              StructField("member_id",    IntegerType, true),
              StructField("is_dw",        BooleanType, true),
              StructField(
                "pricing_terms",
                ArrayType(
                  StructType(
                    List(
                      StructField("term_id",                 IntegerType, true),
                      StructField("amount",                  DoubleType,  true),
                      StructField("rate",                    DoubleType,  true),
                      StructField("is_deduction",            BooleanType, true),
                      StructField("is_media_cost_dependent", BooleanType, true),
                      StructField("data_member_id",          IntegerType, true)
                    )
                  ),
                  true
                ),
                true
              ),
              StructField("fx_margin_rate_id",      IntegerType, true),
              StructField("marketplace_owner_id",   IntegerType, true),
              StructField("virtual_marketplace_id", IntegerType, true),
              StructField("amino_enabled",          BooleanType, true)
            )
          )
        )
        .as("buyer_charges"),
      lit(null)
        .cast(
          StructType(
            List(
              StructField("rate_card_id", IntegerType, true),
              StructField("member_id",    IntegerType, true),
              StructField("is_dw",        BooleanType, true),
              StructField(
                "pricing_terms",
                ArrayType(
                  StructType(
                    List(
                      StructField("term_id",                 IntegerType, true),
                      StructField("amount",                  DoubleType,  true),
                      StructField("rate",                    DoubleType,  true),
                      StructField("is_deduction",            BooleanType, true),
                      StructField("is_media_cost_dependent", BooleanType, true),
                      StructField("data_member_id",          IntegerType, true)
                    )
                  ),
                  true
                ),
                true
              ),
              StructField("fx_margin_rate_id",      IntegerType, true),
              StructField("marketplace_owner_id",   IntegerType, true),
              StructField("virtual_marketplace_id", IntegerType, true),
              StructField("amino_enabled",          BooleanType, true)
            )
          )
        )
        .as("seller_charges"),
      when(col("log_dw_bid.explore_disposition").cast(IntegerType) =!= lit(0),
           col("log_dw_bid.explore_disposition").cast(IntegerType)
      ).as("explore_disposition"),
      when(col("log_impbus_impressions.device_make_id").cast(
             IntegerType
           ) =!= lit(0),
           col("log_impbus_impressions.device_make_id").cast(IntegerType)
      ).as("device_make_id"),
      when(
        col("log_impbus_impressions.operating_system_family_id").cast(
          IntegerType
        ) =!= lit(1),
        col("log_impbus_impressions.operating_system_family_id").cast(
          IntegerType
        )
      ).as("operating_system_family_id"),
      col("log_impbus_impressions.tag_sizes").as("tag_sizes"),
      lit(null)
        .cast(
          ArrayType(
            StructType(
              List(
                StructField("model_type", IntegerType, true),
                StructField("model_id",   IntegerType, true),
                StructField("leaf_code",  StringType,  true),
                StructField("origin",     IntegerType, true),
                StructField("experiment", IntegerType, true),
                StructField("value",      FloatType,   true)
              )
            ),
            true
          )
        )
        .as("campaign_group_models"),
      coalesce(col("log_impbus_impressions_pricing.rate_card_media_type").cast(
                 IntegerType
               ),
               lit(0)
      ).as("pricing_media_type"),
      buyer_trx_event_id.as("buyer_trx_event_id"),
      seller_trx_event_id.as("seller_trx_event_id"),
      coalesce(
        when(
          (_f_is_buy_side === lit(0)).and(
            virtual_log_dw_bid.getField("revenue_auction_event_type") =!= lit(0)
          ),
          virtual_log_dw_bid.getField("revenue_auction_event_type")
        ),
        when(
          (_f_is_buy_side === lit(1)).and(
            col("log_dw_bid.revenue_auction_event_type")
              .cast(IntegerType) =!= lit(0)
          ),
          col("log_dw_bid.revenue_auction_event_type").cast(IntegerType)
        )
      ).as("revenue_auction_event_type"),
      when(col("log_impbus_impressions.is_prebid").cast(ByteType) =!= lit(0),
           col("log_impbus_impressions.is_prebid").cast(BooleanType)
      ).as("is_prebid"),
      coalesce(
        when(imp_type === lit(6), has_seller_transacted.cast(BooleanType)),
        when(imp_type.isin(lit(7), lit(11)),
             has_buyer_transacted.cast(BooleanType)
        ),
        lit(1).cast(BooleanType)
      ).as("is_unit_of_trx"),
      imps_for_budget_caps_pacing.as("imps_for_budget_caps_pacing"),
      col("log_impbus_impressions.date_time")
        .cast(LongType)
        .as("auction_timestamp"),
      when(
        col("log_impbus_impressions_pricing.two_phase_reduction_applied").cast(
          ByteType
        ) =!= lit(0),
        col("log_impbus_impressions_pricing.two_phase_reduction_applied").cast(
          BooleanType
        )
      ).as("two_phase_reduction_applied"),
      when(col("log_impbus_impressions.region_id").cast(IntegerType) =!= lit(0),
           col("log_impbus_impressions.region_id").cast(IntegerType)
      ).as("region_id"),
      when(col("log_impbus_impressions.media_company_id").cast(
             IntegerType
           ) =!= lit(0),
           col("log_impbus_impressions.media_company_id").cast(IntegerType)
      ).as("media_company_id"),
      when(
        col("log_impbus_impressions_pricing.trade_agreement_id").cast(
          IntegerType
        ) =!= lit(0),
        col("log_impbus_impressions_pricing.trade_agreement_id").cast(
          IntegerType
        )
      ).as("trade_agreement_id"),
      lit(null)
        .cast(
          StructType(
            List(
              StructField("user_id_64",       LongType,   true),
              StructField("device_unique_id", StringType, true),
              StructField("external_uid",     StringType, true),
              StructField("ip_address",       BinaryType, true),
              StructField("crossdevice_group",
                          StructType(
                            List(StructField("graph_id", IntegerType, true),
                                 StructField("group_id", LongType,    true)
                            )
                          ),
                          true
              ),
              StructField("latitude",                 DoubleType,  true),
              StructField("longitude",                DoubleType,  true),
              StructField("ipv6_address",             BinaryType,  true),
              StructField("subject_to_gdpr",          BooleanType, true),
              StructField("geo_country",              StringType,  true),
              StructField("gdpr_consent_string",      StringType,  true),
              StructField("preempt_ip_address",       BinaryType,  true),
              StructField("device_type",              IntegerType, true),
              StructField("device_make_id",           IntegerType, true),
              StructField("device_model_id",          IntegerType, true),
              StructField("new_user_id_64",           LongType,    true),
              StructField("is_service_provider_mode", BooleanType, true),
              StructField("is_personal_info_sale",    BooleanType, true)
            )
          )
        )
        .as("personal_data"),
      coalesce(when(is_not_null(netflix_ppid).cast(BooleanType),
                    f_string_to_anon_user_info(netflix_ppid)
               ),
               col("log_impbus_impressions.anonymized_user_info")
      ).as("anonymized_user_info"),
      when(string_compare(col("log_impbus_impressions.gdpr_consent_cookie"),
                          lit("")
           ) =!= lit(0),
           col("log_impbus_impressions.gdpr_consent_cookie")
      ).as("gdpr_consent_cookie"),
      col("additional_clearing_events").as("additional_clearing_events"),
      col("log_impbus_impressions.fx_rate_snapshot_id")
        .cast(IntegerType)
        .as("fx_rate_snapshot_id"),
      coalesce(
        when(_f_is_buy_side === lit(1),
             col("log_dw_bid.crossdevice_group_anon")
        ),
        when(imp_type === lit(6), col("log_dw_bid_deal.crossdevice_group_anon"))
      ).as("crossdevice_group_anon"),
      coalesce(
        when((buyer_trx_event_id === lit(1)).and(imp_type === lit(6)),
             col("log_dw_bid_deal.crossdevice_graph_cost")
        ),
        when((_f_is_buy_side === lit(1)).and(imp_type =!= lit(6)),
             col("log_dw_bid.crossdevice_graph_cost")
        )
      ).as("crossdevice_graph_cost"),
      when(
        (_f_is_buy_side === lit(1)).and(
          col("log_dw_bid.revenue_event_type_id").cast(IntegerType) =!= lit(0)
        ),
        col("log_dw_bid.revenue_event_type_id").cast(IntegerType)
      ).as("revenue_event_type_id"),
      buyer_trx_event_type_id.as("buyer_trx_event_type_id"),
      seller_trx_event_type_id.as("seller_trx_event_type_id"),
      coalesce(col("log_impbus_preempt.external_creative_id"),
               when(is_not_null(col("log_impbus_preempt")).cast(BooleanType),
                    lit("---")
               )
      ).as("external_creative_id"),
      coalesce(
        when(is_not_null(col("log_dw_bid_deal")).and(imp_type === lit(6)),
             col("log_dw_bid_deal.targeted_segment_details")
        ),
        when(_f_is_buy_side === lit(1),
             col("log_dw_bid.targeted_segment_details")
        )
      ).as("targeted_segment_details"),
      coalesce(
        col("log_impbus_preempt.seat_id").cast(IntegerType),
        when(is_not_null(col("log_impbus_preempt")).cast(BooleanType), lit(0)),
        lit(0)
      ).as("bidder_seat_id"),
      when(col("log_impbus_impressions.is_whiteops_scanned").cast(
             ByteType
           ) =!= lit(0),
           col("log_impbus_impressions.is_whiteops_scanned").cast(BooleanType)
      ).as("is_whiteops_scanned"),
      lit(null).cast(StringType).as("default_referrer_url"),
      when(is_curated === lit(1), lit(1).cast(BooleanType)).as("is_curated"),
      curator_member_id.as("curator_member_id"),
      coalesce(
        when(
          (_f_is_buy_side === lit(1))
            .and(
              is_not_null(
                col("log_dw_view.revenue_info.total_partner_fees_microcents")
                  .cast(LongType)
              )
            )
            .and(
              is_not_null(
                col("log_dw_bid.revenue_info.total_partner_fees_microcents")
                  .cast(LongType)
              )
            ),
          col("log_dw_view.revenue_info.total_partner_fees_microcents")
            .cast(LongType) + col(
            "log_dw_bid.revenue_info.total_partner_fees_microcents"
          ).cast(LongType)
        ),
        when(
          (_f_is_buy_side === lit(1)).and(
            is_not_null(
              col("log_dw_bid.revenue_info.total_partner_fees_microcents")
                .cast(LongType)
            )
          ),
          col("log_dw_bid.revenue_info.total_partner_fees_microcents").cast(
            LongType
          )
        ),
        when(_f_is_buy_side === lit(1), lit(0))
      ).as("total_partner_fees_microcents"),
      lit(null).cast(DoubleType).as("net_buyer_spend"),
      when(col("log_impbus_preempt.is_prebid_server").cast(ByteType) =!= lit(0),
           col("log_impbus_preempt.is_prebid_server").cast(BooleanType)
      ).as("is_prebid_server"),
      when(
        (_f_is_buy_side === lit(1)).and(
          col("log_dw_bid.cold_start_price_type").cast(IntegerType) =!= lit(-1)
        ),
        col("log_dw_bid.cold_start_price_type").cast(IntegerType)
      ).as("cold_start_price_type"),
      when((_f_is_buy_side === lit(1)).and(
             col("log_dw_bid.discovery_state").cast(IntegerType) =!= lit(-1)
           ),
           col("log_dw_bid.discovery_state").cast(IntegerType)
      ).as("discovery_state"),
      coalesce(
        when(
          _f_is_buy_side === lit(1),
          col("log_dw_bid.insertion_order_budget_interval_id").cast(IntegerType)
        ),
        billing_period_id,
        lit(0)
      ).as("billing_period_id"),
      coalesce(
        when(
          _f_is_buy_side === lit(1),
          col("log_dw_bid.campaign_group_budget_interval_id").cast(IntegerType)
        ),
        flight_id,
        lit(0)
      ).as("flight_id"),
      split_id.as("split_id"),
      when(
        (imp_type === lit(7)).and(
          v_transaction_event_pricing.getField("buyer_transacted") === lit(1)
        ),
        v_transaction_event_pricing
          .getField("net_payment_value_microcents")
          .cast(DoubleType) / lit(100000.0d)
      ).as("net_media_cost_dollars_cpm"),
      coalesce(
        when(
          (_f_is_buy_side === lit(1))
            .and(
              is_not_null(
                col("log_dw_view.revenue_info.total_data_costs_microcents")
                  .cast(LongType)
              )
            )
            .and(
              is_not_null(
                col("log_dw_bid.revenue_info.total_data_costs_microcents")
                  .cast(LongType)
              )
            ),
          col("log_dw_view.revenue_info.total_data_costs_microcents")
            .cast(LongType) + col(
            "log_dw_bid.revenue_info.total_data_costs_microcents"
          ).cast(LongType)
        ),
        when(
          (_f_is_buy_side === lit(1)).and(
            is_not_null(
              col("log_dw_bid.revenue_info.total_data_costs_microcents")
                .cast(LongType)
            )
          ),
          col("log_dw_bid.revenue_info.total_data_costs_microcents").cast(
            LongType
          )
        ),
        when(_f_is_buy_side === lit(1), lit(0))
      ).as("total_data_costs_microcents"),
      coalesce(
        when(
          (_f_is_buy_side === lit(1))
            .and(
              is_not_null(
                col("log_dw_view.revenue_info.total_profit_microcents")
                  .cast(LongType)
              )
            )
            .and(
              is_not_null(
                col("log_dw_bid.revenue_info.total_profit_microcents")
                  .cast(LongType)
              )
            ),
          col("log_dw_view.revenue_info.total_profit_microcents")
            .cast(LongType) + col(
            "log_dw_bid.revenue_info.total_profit_microcents"
          ).cast(LongType)
        ),
        when(
          (_f_is_buy_side === lit(1)).and(
            is_not_null(
              col("log_dw_bid.revenue_info.total_profit_microcents")
                .cast(LongType)
            )
          ),
          col("log_dw_bid.revenue_info.total_profit_microcents").cast(LongType)
        ),
        when(_f_is_buy_side === lit(1), lit(0))
      ).as("total_profit_microcents"),
      when(
        (_f_is_buy_side === lit(1)).and(
          col("log_dw_bid.targeted_crossdevice_graph_id")
            .cast(IntegerType) =!= lit(0)
        ),
        col("log_dw_bid.targeted_crossdevice_graph_id").cast(IntegerType)
      ).as("targeted_crossdevice_graph_id"),
      when((_f_is_buy_side === lit(1))
             .and(col("log_dw_bid.discovery_prediction") =!= lit(0.0d)),
           col("log_dw_bid.discovery_prediction")
      ).as("discovery_prediction"),
      campaign_group_type_id.as("campaign_group_type_id"),
      when(col("log_impbus_impressions.hb_source").cast(IntegerType) =!= lit(0),
           col("log_impbus_impressions.hb_source").cast(IntegerType)
      ).as("hb_source"),
      f_preempt_over_impression_94298.as("external_campaign_id"),
      when(_f_is_buy_side === lit(1),
           col("log_dw_bid.excluded_targeted_segment_details")
      ).as("excluded_targeted_segment_details"),
      lit(null).cast(StringType).as("trust_id"),
      when((_f_is_buy_side === lit(1))
             .and(col("log_dw_bid.predicted_kpi_event_rate") =!= lit(0.0d)),
           col("log_dw_bid.predicted_kpi_event_rate")
      ).as("predicted_kpi_event_rate"),
      coalesce(
        when(_f_is_buy_side === lit(1),
             col("log_dw_bid.has_crossdevice_reach_extension").cast(BooleanType)
        ),
        when(imp_type === lit(6),
             col("log_dw_bid_deal.has_crossdevice_reach_extension").cast(
               BooleanType
             )
        )
      ).as("has_crossdevice_reach_extension"),
      coalesce(
        when(_f_is_buy_side === lit(1),
             col("log_dw_bid.crossdevice_graph_membership")
        ),
        when(imp_type === lit(6),
             col("log_dw_bid_deal.crossdevice_graph_membership")
        )
      ).as("crossdevice_graph_membership"),
      when(
        _f_is_buy_side === lit(1),
        coalesce(col(
                   "log_dw_bid.revenue_info.total_segment_data_costs_microcents"
                 ).cast(LongType),
                 lit(0)
        ) + coalesce(
          col("log_dw_view.revenue_info.total_segment_data_costs_microcents")
            .cast(LongType),
          lit(0)
        )
      ).otherwise(lit(0)).as("total_segment_data_costs_microcents"),
      when(
        _f_is_buy_side === lit(1),
        coalesce(col("log_dw_bid.revenue_info.total_feature_costs_microcents")
                   .cast(LongType),
                 lit(0)
        ) + coalesce(col(
                       "log_dw_view.revenue_info.total_feature_costs_microcents"
                     ).cast(LongType),
                     lit(0)
        )
      ).otherwise(lit(0)).as("total_feature_costs_microcents"),
      when(
        col("log_impbus_impressions_pricing.counterparty_ruleset_type").cast(
          IntegerType
        ) =!= lit(0),
        col("log_impbus_impressions_pricing.counterparty_ruleset_type").cast(
          IntegerType
        )
      ).as("counterparty_ruleset_type"),
      coalesce(col("log_impbus_preempt.log_product_ads"),
               col("log_impbus_impressions.log_product_ads")
      ).as("log_product_ads"),
      coalesce(when(imp_type.isin(lit(7),                          lit(5)),
                    coalesce(col("log_dw_bid.line_item_currency"), lit("---"))
               ),
               lit("---")
      ).as("buyer_line_item_currency"),
      coalesce(
        when(imp_type === lit(6),
             coalesce(col("log_dw_bid_deal.line_item_currency"), lit("---"))
        ),
        lit("---")
      ).as("deal_line_item_currency"),
      coalesce(when(_f_is_buy_side === lit(1),
                    col("log_dw_bid.measurement_fee_cpm_usd")
               ),
               lit(0)
      ).as("measurement_fee_usd"),
      coalesce(
        when(_f_is_buy_side === lit(1),
             col("log_dw_bid.measurement_provider_member_id").cast(IntegerType)
        ),
        lit(0)
      ).as("measurement_provider_member_id"),
      when(_f_is_buy_side === lit(1),
           col("log_dw_bid.offline_attribution_provider_member_id").cast(
             IntegerType
           )
      ).as("offline_attribution_provider_member_id"),
      when(_f_is_buy_side === lit(1),
           col("log_dw_bid.offline_attribution_cost_usd_cpm")
      ).as("offline_attribution_cost_usd_cpm"),
      when(col("log_impbus_impressions.pred_info").cast(IntegerType) =!= lit(0),
           col("log_impbus_impressions.pred_info").cast(IntegerType)
      ).as("pred_info"),
      col("log_impbus_impressions.imp_rejecter_do_auction")
        .cast(BooleanType)
        .as("imp_rejecter_do_auction"),
      col("log_impbus_impressions.personal_identifiers")
        .as("personal_identifiers"),
      col("log_impbus_impressions.is_imp_rejecter_applied")
        .cast(BooleanType)
        .as("imp_rejecter_applied"),
      lit(null).cast(FloatType).as("ip_derived_latitude"),
      lit(null).cast(FloatType).as("ip_derived_longitude"),
      col("log_impbus_impressions.personal_identifiers_experimental")
        .as("personal_identifiers_experimental"),
      col("log_impbus_impressions.postal_code_ext_id")
        .cast(IntegerType)
        .as("postal_code_ext_id"),
      coalesce(when(buyer_trx_event_id === lit(2),
                    col("log_dw_view.ecpm_conversion_rate")
               ),
               lit(1.0d)
      ).as("ecpm_conversion_rate"),
      when(sup_ip_range_lookup_count > lit(0), lit(1).cast(BooleanType))
        .otherwise(lit(0).cast(BooleanType))
        .as("is_residential_ip"),
      col("log_impbus_impressions.hashed_ip").as("hashed_ip"),
      coalesce(
        when(is_not_null(col("log_dw_bid_deal")).and(imp_type === lit(6)),
             col("log_dw_bid_deal.targeted_segment_details_by_id_type")
        ),
        when(_f_is_buy_side === lit(1),
             col("log_dw_bid.targeted_segment_details_by_id_type")
        )
      ).as("targeted_segment_details_by_id_type"),
      when(_f_is_buy_side === lit(1), col("log_dw_bid.offline_attribution"))
        .as("offline_attribution"),
      col("log_dw_bid.frequency_cap_type_internal")
        .cast(IntegerType)
        .as("frequency_cap_type_internal"),
      col("log_dw_bid.modeled_cap_did_override_line_item_daily_cap")
        .cast(BooleanType)
        .as("modeled_cap_did_override_line_item_daily_cap"),
      col("log_dw_bid.modeled_cap_user_sample_rate")
        .as("modeled_cap_user_sample_rate"),
      when(
        coalesce(col("log_impbus_impressions.device_type").cast(IntegerType),
                 lit(99)
        ) === lit(8),
        coalesce(
          col("log_impbus_impressions_pricing_dup.estimated_audience_imps"),
          lit(1.0d)
        )
      ).otherwise(lit(1.0d)).as("estimated_audience_imps"),
      when(
        coalesce(col("log_impbus_impressions.device_type").cast(IntegerType),
                 lit(99)
        ) === lit(8),
        coalesce(col("log_impbus_impressions_pricing_dup.audience_imps"),
                 lit(1.0d)
        )
      ).otherwise(lit(1.0d)).as("audience_imps"),
      col("log_dw_bid.district_postal_code_lists")
        .as("district_postal_code_lists"),
      coalesce(
        when(imp_type.isin(lit(4), lit(7), lit(5)),
             coalesce(col("log_dw_bid.bidding_host_id").cast(IntegerType),
                      lit(0)
             )
        ),
        when((imp_type === lit(6)).and(is_not_null(col("log_dw_bid_deal"))),
             coalesce(col("log_dw_bid_deal.bidding_host_id").cast(IntegerType),
                      lit(0)
             )
        )
      ).as("bidding_host_id"),
      lit(null).cast(LongType).as("buyer_dpvp_bitmap"),
      lit(null).cast(LongType).as("seller_dpvp_bitmap"),
      col("log_impbus_impressions.browser_code_id")
        .cast(IntegerType)
        .as("browser_code_id"),
      col("log_impbus_impressions.is_prebid_server_included")
        .cast(IntegerType)
        .as("is_prebid_server_included"),
      coalesce(col("log_dw_bid.feature_tests_bitmap").cast(IntegerType), lit(0))
        .as("feature_tests_bitmap"),
      coalesce(col("log_impbus_impressions.private_auction_eligible").cast(
                 BooleanType
               ),
               lit(0).cast(BooleanType)
      ).as("private_auction_eligible"),
      coalesce(
        col("log_impbus_impressions.chrome_traffic_label").cast(IntegerType),
        lit(0)
      ).as("chrome_traffic_label"),
      coalesce(
        col("log_impbus_impressions.is_private_auction").cast(BooleanType),
        lit(0).cast(BooleanType)
      ).as("is_private_auction"),
      f_preempt_over_impression_95337.as("creative_media_subtype_id"),
      col("log_impbus_impressions.allowed_media_types").as(
        "allowed_media_types"
      )
    )

  def resolve_exchange_rate(
    code:                Column,
    fx_rate_snapshot_id: Column,
    fallback:            Column
  ): Column = {
    var l_lookup_exchange_rate: org.apache.spark.sql.Column =
      lit(null).cast(DoubleType)
    var l_lookup_exchange_rate_0: org.apache.spark.sql.Column =
      lit(null).cast(DoubleType)
    var l_lookup_exchange_rate_2: org.apache.spark.sql.Column =
      lit(null).cast(DoubleType)
    var l_resolved_rate: org.apache.spark.sql.Column = fallback.cast(DoubleType)
    var l_resolved_rate_1: org.apache.spark.sql.Column =
      lit(null).cast(DoubleType)
    var l_resolved_rate_0: org.apache.spark.sql.Column =
      lit(null).cast(DoubleType)
    l_lookup_exchange_rate_0 = when(
      string_compare(code, lit("USD")) =!= lit(0),
      when(is_not_null(fx_rate_snapshot_id).cast(BooleanType),
           lookup("sup_bidder_fx_rates", fx_rate_snapshot_id, code)
             .getField("rate")
             .cast(DoubleType)
      ).otherwise(l_lookup_exchange_rate)
    ).otherwise(l_lookup_exchange_rate).cast(DoubleType)
    l_lookup_exchange_rate_2 = when(
      string_compare(code, lit("USD")) =!= lit(0),
      when(isnull(l_lookup_exchange_rate_0).cast(BooleanType),
           lookup("sup_code_fx_rate", code).getField("rate").cast(DoubleType)
      ).otherwise(l_lookup_exchange_rate_0)
    ).otherwise(l_lookup_exchange_rate_2).cast(DoubleType)
    l_resolved_rate_1 = when(
      string_compare(code, lit("USD")) =!= lit(0),
      when(is_not_null(l_lookup_exchange_rate_2).cast(BooleanType),
           l_lookup_exchange_rate_2.cast(DoubleType)
      ).otherwise(l_resolved_rate)
    ).otherwise(l_resolved_rate_1).cast(DoubleType)
    l_resolved_rate_0 = when(string_compare(code, lit("USD")) =!= lit(0),
                             l_resolved_rate_1
    ).otherwise(lit(1.0d).cast(DoubleType)).cast(DoubleType)
    l_resolved_rate_0
  }

  def f_get_agg_tl_network_analytics_pb_costs(
    imp_type:                            Column,
    total_partner_fees_microcents:       Column,
    total_data_costs_microcents:         Column,
    total_feature_costs_microcents:      Column,
    total_segment_data_costs_microcents: Column,
    media_cost_dollars_cpm:              Column,
    serving_fees_cpm:                    Column,
    serving_fees_revshare:               Column,
    commission_cpm:                      Column,
    commission_revshare:                 Column,
    booked_revenue_dollars:              Column
  ): Column = {
    var l_total_partner_fees_microcents: org.apache.spark.sql.Column =
      coalesce(total_partner_fees_microcents, lit(0)).cast(LongType)
    var l_total_data_costs_microcents: org.apache.spark.sql.Column =
      coalesce(total_data_costs_microcents, lit(0)).cast(LongType)
    var l_total_feature_costs_microcents: org.apache.spark.sql.Column =
      coalesce(total_feature_costs_microcents, lit(0)).cast(LongType)
    var l_total_segment_data_costs_microcents: org.apache.spark.sql.Column =
      coalesce(total_segment_data_costs_microcents, lit(0)).cast(LongType)
    var l_media_cost_dollars_cpm: org.apache.spark.sql.Column =
      coalesce(media_cost_dollars_cpm, lit(0.0d)).cast(DoubleType)
    var l_serving_fees_cpm: org.apache.spark.sql.Column =
      coalesce(serving_fees_cpm, lit(0.0d)).cast(DoubleType)
    var l_serving_fees_revshare: org.apache.spark.sql.Column =
      coalesce(serving_fees_revshare, lit(0.0d)).cast(DoubleType)
    var l_commission_cpm: org.apache.spark.sql.Column =
      coalesce(commission_cpm, lit(0.0d)).cast(DoubleType)
    var l_commission_revshare: org.apache.spark.sql.Column =
      coalesce(commission_revshare, lit(0.0d)).cast(DoubleType)
    var l_booked_revenue_dollars: org.apache.spark.sql.Column =
      coalesce(booked_revenue_dollars, lit(0.0d)).cast(DoubleType)
    var l_agg_tl_network_analytics_pb_costs: org.apache.spark.sql.Column =
      lit(null).cast(
        StructType(
          List(
            StructField("total_partner_fees_microcents",       DoubleType, true),
            StructField("total_data_costs_microcents",         DoubleType, true),
            StructField("total_cost_microcents",               DoubleType, true),
            StructField("total_feature_costs_microcents",      DoubleType, true),
            StructField("total_segment_data_costs_microcents", DoubleType, true)
          )
        )
      )
    var l_media_cost_microcents:   org.apache.spark.sql.Column = lit(0)
    var l_serving_fees_microcents: org.apache.spark.sql.Column = lit(0)
    var l_commissions_microcents:  org.apache.spark.sql.Column = lit(0)
    var l_agg_tl_network_analytics_pb_costs_total_partner_fees_microcents
      : org.apache.spark.sql.Column =
      l_total_partner_fees_microcents.cast(DoubleType)
    var l_agg_tl_network_analytics_pb_costs_total_data_costs_microcents
      : org.apache.spark.sql.Column =
      l_total_data_costs_microcents.cast(DoubleType)
    var l_agg_tl_network_analytics_pb_costs_total_feature_costs_microcents
      : org.apache.spark.sql.Column =
      l_total_feature_costs_microcents.cast(DoubleType)
    var l_agg_tl_network_analytics_pb_costs_total_segment_data_costs_microcents
      : org.apache.spark.sql.Column =
      l_total_segment_data_costs_microcents.cast(DoubleType)
    var l_agg_tl_network_analytics_pb_costs_total_cost_microcents
      : org.apache.spark.sql.Column = lit(null).cast(DoubleType)
    l_media_cost_microcents =
      (l_media_cost_dollars_cpm * lit(100000.0d)).cast(LongType)
    l_serving_fees_microcents =
      ((l_serving_fees_cpm + l_serving_fees_revshare * l_media_cost_dollars_cpm) * lit(
        100000.0d
      )).cast(LongType)
    l_commissions_microcents =
      ((l_commission_cpm + l_commission_revshare * (l_booked_revenue_dollars * lit(
        1000.0d
      ))) * lit(100000.0d)).cast(LongType)
    l_agg_tl_network_analytics_pb_costs_total_cost_microcents =
      when(coalesce(imp_type, lit(0)) === lit(6), l_media_cost_microcents)
        .when(
          (coalesce(imp_type, lit(0)) === lit(7))
            .or(coalesce(imp_type, lit(0)) === lit(5))
            .or(coalesce(imp_type, lit(0)) === lit(11))
            .or(coalesce(imp_type, lit(0)) === lit(10))
            .or(coalesce(imp_type, lit(0)) === lit(9)), {
            l_agg_tl_network_analytics_pb_costs_total_cost_microcents =
              (l_agg_tl_network_analytics_pb_costs_total_data_costs_microcents + l_media_cost_microcents)
                .cast(DoubleType)
            l_agg_tl_network_analytics_pb_costs_total_cost_microcents = when(
              l_agg_tl_network_analytics_pb_costs_total_partner_fees_microcents > lit(
                0
              ),
              l_agg_tl_network_analytics_pb_costs_total_cost_microcents + l_agg_tl_network_analytics_pb_costs_total_partner_fees_microcents
            ).otherwise(
                l_agg_tl_network_analytics_pb_costs_total_cost_microcents + l_serving_fees_microcents + l_commissions_microcents
              )
              .cast(DoubleType)
            l_agg_tl_network_analytics_pb_costs_total_cost_microcents
          }
        )
        .otherwise(l_agg_tl_network_analytics_pb_costs_total_cost_microcents)
        .cast(DoubleType)
    l_agg_tl_network_analytics_pb_costs = struct(
      l_agg_tl_network_analytics_pb_costs_total_partner_fees_microcents.as(
        "total_partner_fees_microcents"
      ),
      l_agg_tl_network_analytics_pb_costs_total_data_costs_microcents.as(
        "total_data_costs_microcents"
      ),
      l_agg_tl_network_analytics_pb_costs_total_cost_microcents.as(
        "total_cost_microcents"
      ),
      l_agg_tl_network_analytics_pb_costs_total_feature_costs_microcents.as(
        "total_feature_costs_microcents"
      ),
      l_agg_tl_network_analytics_pb_costs_total_segment_data_costs_microcents
        .as("total_segment_data_costs_microcents")
    )
    l_agg_tl_network_analytics_pb_costs
  }

  def f_get_agg_tl_network_analytics_pb_currency(
    fx_rate_snapshot_id:               Column,
    publisher_currency:                Column,
    publisher_currency_rate:           Column,
    advertiser_currency:               Column,
    advertiser_currency_rate:          Column,
    advertiser_id:                     Column,
    imp_type:                          Column,
    buyer_member_id:                   Column,
    seller_member_id:                  Column,
    sup_bidder_advertiser_pb_lookup_2: Column,
    sup_bidder_advertiser_pb_lookup:   Column
  ): Column = {
    var l_member_id:   org.apache.spark.sql.Column = lit(null).cast(IntegerType)
    var l_member_id_0: org.apache.spark.sql.Column = lit(null).cast(IntegerType)
    var l_agg_tl_network_analytics_pb_currency: org.apache.spark.sql.Column =
      lit(null).cast(
        StructType(
          List(
            StructField("fx_rate_snapshot_id",              IntegerType, true),
            StructField("publisher_currency",               StringType,  true),
            StructField("publisher_exchange_rate",          DoubleType,  true),
            StructField("advertiser_currency",              StringType,  true),
            StructField("advertiser_exchange_rate",         DoubleType,  true),
            StructField("advertiser_default_currency",      StringType,  true),
            StructField("advertiser_default_exchange_rate", DoubleType,  true),
            StructField("member_currency",                  StringType,  true),
            StructField("member_exchange_rate",             DoubleType,  true),
            StructField("billing_currency",                 StringType,  true),
            StructField("billing_exchange_rate",            DoubleType,  true)
          )
        )
      )
    var l_sup_bidder_fx_rate: org.apache.spark.sql.Column = lit(null).cast(
      StructType(
        List(
          StructField("fx_rate_snapshot_id", IntegerType, true),
          StructField("currency_id",         IntegerType, true),
          StructField("code",                StringType,  true),
          StructField("rate",                DoubleType,  true),
          StructField("as_of_timestamp",     LongType,    true)
        )
      )
    )
    var l_sup_bidder_fx_rate_1: org.apache.spark.sql.Column = lit(null).cast(
      StructType(
        List(
          StructField("fx_rate_snapshot_id", IntegerType, true),
          StructField("currency_id",         IntegerType, true),
          StructField("code",                StringType,  true),
          StructField("rate",                DoubleType,  true),
          StructField("as_of_timestamp",     LongType,    true)
        )
      )
    )
    var l_sup_bidder_fx_rate_3: org.apache.spark.sql.Column = lit(null).cast(
      StructType(
        List(
          StructField("fx_rate_snapshot_id", IntegerType, true),
          StructField("currency_id",         IntegerType, true),
          StructField("code",                StringType,  true),
          StructField("rate",                DoubleType,  true),
          StructField("as_of_timestamp",     LongType,    true)
        )
      )
    )
    var l_sup_bidder_fx_rate_5: org.apache.spark.sql.Column = lit(null).cast(
      StructType(
        List(
          StructField("fx_rate_snapshot_id", IntegerType, true),
          StructField("currency_id",         IntegerType, true),
          StructField("code",                StringType,  true),
          StructField("rate",                DoubleType,  true),
          StructField("as_of_timestamp",     LongType,    true)
        )
      )
    )
    var l_agg_tl_network_analytics_pb_currency_fx_rate_snapshot_id
      : org.apache.spark.sql.Column = fx_rate_snapshot_id.cast(IntegerType)
    var l_agg_tl_network_analytics_pb_currency_fx_rate_snapshot_id_1
      : org.apache.spark.sql.Column = lit(null).cast(IntegerType)
    var l_agg_tl_network_analytics_pb_currency_fx_rate_snapshot_id_3
      : org.apache.spark.sql.Column = lit(null).cast(IntegerType)
    var l_agg_tl_network_analytics_pb_currency_fx_rate_snapshot_id_5
      : org.apache.spark.sql.Column = lit(null).cast(IntegerType)
    var l_agg_tl_network_analytics_pb_currency_fx_rate_snapshot_id_0
      : org.apache.spark.sql.Column = lit(null).cast(IntegerType)
    var l_agg_tl_network_analytics_pb_currency_publisher_currency
      : org.apache.spark.sql.Column = coalesce(publisher_currency, lit("USD"))
    var l_agg_tl_network_analytics_pb_currency_publisher_currency_0
      : org.apache.spark.sql.Column = lit(null).cast(StringType)
    var l_agg_tl_network_analytics_pb_currency_publisher_currency_2
      : org.apache.spark.sql.Column = lit(null).cast(StringType)
    var l_agg_tl_network_analytics_pb_currency_publisher_exchange_rate_2
      : org.apache.spark.sql.Column = lit(null).cast(DoubleType)
    var l_agg_tl_network_analytics_pb_currency_advertiser_currency
      : org.apache.spark.sql.Column = coalesce(advertiser_currency, lit("USD"))
    l_agg_tl_network_analytics_pb_currency_publisher_currency_0 =
      when(isnull(fx_rate_snapshot_id).and(
             isnull(publisher_currency).or(isnull(publisher_currency_rate))
           ),
           lit("USD").cast(StringType)
      ).otherwise(l_agg_tl_network_analytics_pb_currency_publisher_currency)
    var l_agg_tl_network_analytics_pb_currency_advertiser_default_currency
      : org.apache.spark.sql.Column = lit("USD")
    var l_agg_tl_network_analytics_pb_currency_advertiser_default_currency_0
      : org.apache.spark.sql.Column = lit(null).cast(StringType)
    var l_agg_tl_network_analytics_pb_currency_advertiser_default_exchange_rate
      : org.apache.spark.sql.Column = lit(1.0d)
    var l_agg_tl_network_analytics_pb_currency_advertiser_default_exchange_rate_0
      : org.apache.spark.sql.Column = lit(null).cast(DoubleType)
    var l_agg_tl_network_analytics_pb_currency_billing_currency
      : org.apache.spark.sql.Column = lit("USD")
    var l_agg_tl_network_analytics_pb_currency_billing_currency_0
      : org.apache.spark.sql.Column = lit(null).cast(StringType)
    var l_agg_tl_network_analytics_pb_currency_billing_exchange_rate
      : org.apache.spark.sql.Column = lit(1.0d)
    var l_agg_tl_network_analytics_pb_currency_billing_exchange_rate_0
      : org.apache.spark.sql.Column = lit(null).cast(DoubleType)
    var l_agg_tl_network_analytics_pb_currency_member_currency
      : org.apache.spark.sql.Column = lit("USD")
    var l_agg_tl_network_analytics_pb_currency_member_currency_0
      : org.apache.spark.sql.Column = lit(null).cast(StringType)
    var l_agg_tl_network_analytics_pb_currency_member_exchange_rate
      : org.apache.spark.sql.Column = lit(1.0d)
    var l_agg_tl_network_analytics_pb_currency_member_exchange_rate_0
      : org.apache.spark.sql.Column = lit(null).cast(DoubleType)
    l_member_id_0 = when(
      is_not_null(imp_type).cast(BooleanType),
      when(imp_type
             .isin(lit(9), lit(10), lit(11), lit(7), lit(5))
             .and(is_not_null(buyer_member_id)),
           buyer_member_id.cast(IntegerType)
      ).when(is_not_null(seller_member_id).cast(BooleanType),
              seller_member_id.cast(IntegerType)
        )
        .otherwise(l_member_id)
    ).otherwise(l_member_id).cast(IntegerType)
    l_agg_tl_network_analytics_pb_currency_fx_rate_snapshot_id_1 = when(
      isnull(fx_rate_snapshot_id).and(
        string_compare(
          l_agg_tl_network_analytics_pb_currency_publisher_currency_0,
          lit("USD")
        ) =!= lit(0)
      ),
      when(isnull(lookup("sup_code_fx_rate", publisher_currency)).cast(
             BooleanType
           ),
           l_agg_tl_network_analytics_pb_currency_fx_rate_snapshot_id
      ).otherwise(
        lookup("sup_code_fx_rate", publisher_currency)
          .getField("fx_rate_snapshot_id")
          .cast(IntegerType)
      )
    ).when(
        isnull(fx_rate_snapshot_id).and(
          not(
            string_compare(
              l_agg_tl_network_analytics_pb_currency_publisher_currency_0,
              lit("USD")
            ) =!= lit(0)
          )
        ),
        l_agg_tl_network_analytics_pb_currency_fx_rate_snapshot_id
      )
      .otherwise(l_agg_tl_network_analytics_pb_currency_fx_rate_snapshot_id_1)
      .cast(IntegerType)
    l_agg_tl_network_analytics_pb_currency_publisher_currency_2 = when(
      is_not_null(fx_rate_snapshot_id).cast(BooleanType),
      when(
        string_compare(
          l_agg_tl_network_analytics_pb_currency_publisher_currency_0,
          lit("USD")
        ) =!= lit(0),
        when(isnull(
               lookup("sup_bidder_fx_rates",
                      fx_rate_snapshot_id,
                      publisher_currency
               ).getField("rate")
             ).cast(BooleanType),
             lit("USD").cast(StringType)
        ).otherwise(l_agg_tl_network_analytics_pb_currency_publisher_currency_0)
      ).otherwise(l_agg_tl_network_analytics_pb_currency_publisher_currency_0)
    ).when(
        string_compare(
          l_agg_tl_network_analytics_pb_currency_publisher_currency_0,
          lit("USD")
        ) =!= lit(0),
        when(isnull(lookup("sup_code_fx_rate", publisher_currency))
               .cast(BooleanType),
             lit("USD").cast(StringType)
        ).otherwise(l_agg_tl_network_analytics_pb_currency_publisher_currency_0)
      )
      .otherwise(l_agg_tl_network_analytics_pb_currency_publisher_currency_0)
    l_agg_tl_network_analytics_pb_currency_publisher_exchange_rate_2 = when(
      is_not_null(fx_rate_snapshot_id).cast(BooleanType),
      when(
        string_compare(
          l_agg_tl_network_analytics_pb_currency_publisher_currency_0,
          lit("USD")
        ) =!= lit(0),
        when(isnull(
               lookup("sup_bidder_fx_rates",
                      fx_rate_snapshot_id,
                      publisher_currency
               ).getField("rate")
             ).cast(BooleanType),
             lit(1.0d).cast(DoubleType)
        ).otherwise(
          lookup("sup_bidder_fx_rates", fx_rate_snapshot_id, publisher_currency)
            .getField("rate")
            .cast(DoubleType)
        )
      ).otherwise(lit(1.0d).cast(DoubleType))
    ).when(
        string_compare(
          l_agg_tl_network_analytics_pb_currency_publisher_currency_0,
          lit("USD")
        ) =!= lit(0),
        when(isnull(lookup("sup_code_fx_rate", publisher_currency))
               .cast(BooleanType),
             lit(1.0d).cast(DoubleType)
        ).otherwise(
          lookup("sup_code_fx_rate", publisher_currency)
            .getField("rate")
            .cast(DoubleType)
        )
      )
      .otherwise(lit(1.0d).cast(DoubleType))
      .cast(DoubleType)
    l_sup_bidder_fx_rate_1 = when(
      isnull(fx_rate_snapshot_id).and(
        string_compare(
          l_agg_tl_network_analytics_pb_currency_publisher_currency_0,
          lit("USD")
        ) =!= lit(0)
      ),
      lookup("sup_code_fx_rate", publisher_currency)
    ).when(
        isnull(fx_rate_snapshot_id).and(
          not(
            string_compare(
              l_agg_tl_network_analytics_pb_currency_publisher_currency_0,
              lit("USD")
            ) =!= lit(0)
          )
        ),
        l_sup_bidder_fx_rate
      )
      .otherwise(l_sup_bidder_fx_rate_1)
    l_agg_tl_network_analytics_pb_currency_fx_rate_snapshot_id_5 = when(
      isnull(fx_rate_snapshot_id).and(is_not_null(l_member_id_0)),
      when(
        is_not_null(lookup("sup_api_member_pb", l_member_id_0)).cast(
          BooleanType
        ),
        when(
          is_not_null(
            lookup("sup_api_member_pb", l_member_id_0).getField(
              "member_currency"
            )
          ).and(
            string_compare(lookup("sup_api_member_pb", l_member_id_0)
                             .getField("member_currency"),
                           lit("USD")
            ) =!= lit(0)
          ),
          when(isnull(
                 l_agg_tl_network_analytics_pb_currency_member_exchange_rate
               ).cast(BooleanType),
               l_agg_tl_network_analytics_pb_currency_fx_rate_snapshot_id_3
          ).otherwise(
            lookup("sup_code_fx_rate",
                   lookup("sup_api_member_pb", l_member_id_0).getField(
                     "member_currency"
                   )
            ).getField("fx_rate_snapshot_id").cast(IntegerType)
          )
        ).otherwise(
          l_agg_tl_network_analytics_pb_currency_fx_rate_snapshot_id_3
        )
      ).otherwise(l_agg_tl_network_analytics_pb_currency_fx_rate_snapshot_id_5)
    ).otherwise(l_agg_tl_network_analytics_pb_currency_fx_rate_snapshot_id_5)
      .cast(IntegerType)
    l_agg_tl_network_analytics_pb_currency_fx_rate_snapshot_id_3 = when(
      isnull(fx_rate_snapshot_id).and(is_not_null(l_member_id_0)),
      when(
        is_not_null(advertiser_id)
          .cast(BooleanType)
          .and(
            is_not_null(
              rpad(sup_bidder_advertiser_pb_lookup
                     .getField("advertiser_default_currency"),
                   3,
                   " "
              )
            ).and(
              string_compare(rpad(sup_bidder_advertiser_pb_lookup
                                    .getField("advertiser_default_currency"),
                                  3,
                                  " "
                             ),
                             lit("USD")
              ) =!= lit(0)
            )
          )
          .and(
            is_not_null(
              lookup("sup_code_fx_rate",
                     rpad(sup_bidder_advertiser_pb_lookup
                            .getField("advertiser_default_currency"),
                          3,
                          " "
                     )
              )
            ).cast(BooleanType)
          ),
        lookup("sup_code_fx_rate",
               rpad(sup_bidder_advertiser_pb_lookup.getField(
                      "advertiser_default_currency"
                    ),
                    3,
                    " "
               )
        ).getField("fx_rate_snapshot_id").cast(IntegerType)
      ).otherwise(l_agg_tl_network_analytics_pb_currency_fx_rate_snapshot_id_1)
    ).otherwise(l_agg_tl_network_analytics_pb_currency_fx_rate_snapshot_id_3)
      .cast(IntegerType)
    l_agg_tl_network_analytics_pb_currency_advertiser_default_currency_0 = when(
      is_not_null(fx_rate_snapshot_id).cast(BooleanType),
      when(
        is_not_null(l_member_id_0)
          .cast(BooleanType)
          .and(is_not_null(advertiser_id).cast(BooleanType))
          .and(
            is_not_null(
              rpad(sup_bidder_advertiser_pb_lookup
                     .getField("advertiser_default_currency"),
                   3,
                   " "
              )
            ).and(
              string_compare(rpad(sup_bidder_advertiser_pb_lookup
                                    .getField("advertiser_default_currency"),
                                  3,
                                  " "
                             ),
                             lit("USD")
              ) =!= lit(0)
            )
          ),
        when(
          is_not_null(
            lookup("sup_bidder_fx_rates",
                   fx_rate_snapshot_id,
                   rpad(sup_bidder_advertiser_pb_lookup.getField(
                          "advertiser_default_currency"
                        ),
                        3,
                        " "
                   )
            ).getField("rate").cast(DoubleType)
          ).cast(BooleanType),
          sup_bidder_advertiser_pb_lookup
            .getField("advertiser_default_currency")
            .cast(StringType)
        ).otherwise(lit("USD").cast(StringType))
      ).otherwise(
        l_agg_tl_network_analytics_pb_currency_advertiser_default_currency
      )
    ).when(
        is_not_null(l_member_id_0)
          .cast(BooleanType)
          .and(is_not_null(advertiser_id).cast(BooleanType))
          .and(
            is_not_null(
              rpad(sup_bidder_advertiser_pb_lookup
                     .getField("advertiser_default_currency"),
                   3,
                   " "
              )
            ).and(
              string_compare(rpad(sup_bidder_advertiser_pb_lookup
                                    .getField("advertiser_default_currency"),
                                  3,
                                  " "
                             ),
                             lit("USD")
              ) =!= lit(0)
            )
          ),
        when(
          is_not_null(
            lookup("sup_code_fx_rate",
                   rpad(sup_bidder_advertiser_pb_lookup
                          .getField("advertiser_default_currency"),
                        3,
                        " "
                   )
            )
          ).cast(BooleanType),
          sup_bidder_advertiser_pb_lookup
            .getField("advertiser_default_currency")
            .cast(StringType)
        ).otherwise(lit("USD").cast(StringType))
      )
      .otherwise(
        l_agg_tl_network_analytics_pb_currency_advertiser_default_currency
      )
    l_agg_tl_network_analytics_pb_currency_advertiser_default_exchange_rate_0 =
      when(
        is_not_null(fx_rate_snapshot_id).cast(BooleanType),
        when(
          is_not_null(l_member_id_0)
            .cast(BooleanType)
            .and(is_not_null(advertiser_id).cast(BooleanType))
            .and(
              is_not_null(
                rpad(sup_bidder_advertiser_pb_lookup
                       .getField("advertiser_default_currency"),
                     3,
                     " "
                )
              ).and(
                string_compare(rpad(sup_bidder_advertiser_pb_lookup
                                      .getField("advertiser_default_currency"),
                                    3,
                                    " "
                               ),
                               lit("USD")
                ) =!= lit(0)
              )
            ),
          when(
            is_not_null(
              lookup("sup_bidder_fx_rates",
                     fx_rate_snapshot_id,
                     rpad(sup_bidder_advertiser_pb_lookup.getField(
                            "advertiser_default_currency"
                          ),
                          3,
                          " "
                     )
              ).getField("rate").cast(DoubleType)
            ).cast(BooleanType),
            lookup("sup_bidder_fx_rates",
                   fx_rate_snapshot_id,
                   rpad(sup_bidder_advertiser_pb_lookup.getField(
                          "advertiser_default_currency"
                        ),
                        3,
                        " "
                   )
            ).getField("rate").cast(DoubleType)
          ).otherwise(lit(1.0d).cast(DoubleType))
        ).otherwise(
          l_agg_tl_network_analytics_pb_currency_advertiser_default_exchange_rate
        )
      ).when(
          is_not_null(l_member_id_0)
            .cast(BooleanType)
            .and(is_not_null(advertiser_id).cast(BooleanType))
            .and(
              is_not_null(
                rpad(sup_bidder_advertiser_pb_lookup
                       .getField("advertiser_default_currency"),
                     3,
                     " "
                )
              ).and(
                string_compare(rpad(sup_bidder_advertiser_pb_lookup
                                      .getField("advertiser_default_currency"),
                                    3,
                                    " "
                               ),
                               lit("USD")
                ) =!= lit(0)
              )
            ),
          when(
            is_not_null(
              lookup("sup_code_fx_rate",
                     rpad(sup_bidder_advertiser_pb_lookup
                            .getField("advertiser_default_currency"),
                          3,
                          " "
                     )
              )
            ).cast(BooleanType),
            lookup("sup_code_fx_rate",
                   rpad(sup_bidder_advertiser_pb_lookup.getField(
                          "advertiser_default_currency"
                        ),
                        3,
                        " "
                   )
            ).getField("rate").cast(DoubleType)
          ).otherwise(lit(1.0d).cast(DoubleType))
        )
        .otherwise(
          l_agg_tl_network_analytics_pb_currency_advertiser_default_exchange_rate
        )
        .cast(DoubleType)
    l_agg_tl_network_analytics_pb_currency_member_exchange_rate_0 = when(
      is_not_null(fx_rate_snapshot_id).cast(BooleanType),
      when(
        is_not_null(l_member_id_0)
          .cast(BooleanType)
          .and(
            is_not_null(lookup("sup_api_member_pb", l_member_id_0))
              .cast(BooleanType)
          )
          .and(
            is_not_null(
              lookup("sup_api_member_pb", l_member_id_0)
                .getField("member_currency")
            ).cast(BooleanType)
          ),
        when(
          isnull(
            lookup("sup_bidder_fx_rates",
                   fx_rate_snapshot_id,
                   lookup("sup_api_member_pb", l_member_id_0).getField(
                     "member_currency"
                   )
            ).getField("rate")
          ).cast(BooleanType),
          lit(1.0d).cast(DoubleType)
        ).otherwise(
          lookup("sup_bidder_fx_rates",
                 fx_rate_snapshot_id,
                 lookup("sup_api_member_pb", l_member_id_0).getField(
                   "member_currency"
                 )
          ).getField("rate").cast(DoubleType)
        )
      ).otherwise(l_agg_tl_network_analytics_pb_currency_member_exchange_rate)
    ).when(
        is_not_null(l_member_id_0)
          .cast(BooleanType)
          .and(
            is_not_null(lookup("sup_api_member_pb", l_member_id_0))
              .cast(BooleanType)
          )
          .and(
            is_not_null(
              lookup("sup_api_member_pb", l_member_id_0)
                .getField("member_currency")
            ).and(
              string_compare(lookup("sup_api_member_pb", l_member_id_0)
                               .getField("member_currency"),
                             lit("USD")
              ) =!= lit(0)
            )
          ),
        when(isnull(l_agg_tl_network_analytics_pb_currency_member_exchange_rate)
               .cast(BooleanType),
             lit(1.0d).cast(DoubleType)
        ).otherwise(
          lookup("sup_code_fx_rate",
                 lookup("sup_api_member_pb", l_member_id_0)
                   .getField("member_currency")
          ).getField("rate").cast(DoubleType)
        )
      )
      .otherwise(l_agg_tl_network_analytics_pb_currency_member_exchange_rate)
      .cast(DoubleType)
    l_agg_tl_network_analytics_pb_currency_member_currency_0 = when(
      is_not_null(fx_rate_snapshot_id).cast(BooleanType),
      when(
        is_not_null(l_member_id_0)
          .cast(BooleanType)
          .and(
            is_not_null(lookup("sup_api_member_pb", l_member_id_0))
              .cast(BooleanType)
          )
          .and(
            is_not_null(
              lookup("sup_api_member_pb", l_member_id_0)
                .getField("member_currency")
            ).cast(BooleanType)
          ),
        when(
          isnull(
            lookup("sup_bidder_fx_rates",
                   fx_rate_snapshot_id,
                   lookup("sup_api_member_pb", l_member_id_0).getField(
                     "member_currency"
                   )
            ).getField("rate")
          ).cast(BooleanType),
          lit("USD").cast(StringType)
        ).otherwise(
          lookup("sup_api_member_pb", l_member_id_0)
            .getField("member_currency")
            .cast(StringType)
        )
      ).otherwise(l_agg_tl_network_analytics_pb_currency_member_currency)
    ).when(
        is_not_null(l_member_id_0)
          .cast(BooleanType)
          .and(
            is_not_null(lookup("sup_api_member_pb", l_member_id_0))
              .cast(BooleanType)
          )
          .and(
            is_not_null(
              lookup("sup_api_member_pb", l_member_id_0)
                .getField("member_currency")
            ).and(
              string_compare(lookup("sup_api_member_pb", l_member_id_0)
                               .getField("member_currency"),
                             lit("USD")
              ) =!= lit(0)
            )
          ),
        when(isnull(l_agg_tl_network_analytics_pb_currency_member_exchange_rate)
               .cast(BooleanType),
             lit("USD").cast(StringType)
        ).otherwise(
          lookup("sup_api_member_pb", l_member_id_0)
            .getField("member_currency")
            .cast(StringType)
        )
      )
      .otherwise(l_agg_tl_network_analytics_pb_currency_member_currency)
    l_agg_tl_network_analytics_pb_currency_billing_currency_0 = when(
      is_not_null(fx_rate_snapshot_id).cast(BooleanType),
      when(
        is_not_null(l_member_id_0)
          .cast(BooleanType)
          .and(
            is_not_null(lookup("sup_api_member_pb", l_member_id_0))
              .cast(BooleanType)
          )
          .and(
            is_not_null(
              lookup("sup_api_member_pb", l_member_id_0)
                .getField("billing_currency")
            ).cast(BooleanType)
          ),
        when(
          isnull(
            lookup("sup_bidder_fx_rates",
                   fx_rate_snapshot_id,
                   lookup("sup_api_member_pb", l_member_id_0).getField(
                     "billing_currency"
                   )
            ).getField("rate")
          ).cast(BooleanType),
          lit("USD").cast(StringType)
        ).otherwise(
          lookup("sup_api_member_pb", l_member_id_0)
            .getField("billing_currency")
            .cast(StringType)
        )
      ).otherwise(l_agg_tl_network_analytics_pb_currency_billing_currency)
    ).when(
        is_not_null(l_member_id_0)
          .cast(BooleanType)
          .and(
            is_not_null(lookup("sup_api_member_pb", l_member_id_0))
              .cast(BooleanType)
          )
          .and(
            is_not_null(
              lookup("sup_api_member_pb", l_member_id_0)
                .getField("billing_currency")
            ).and(
              string_compare(lookup("sup_api_member_pb", l_member_id_0)
                               .getField("billing_currency"),
                             lit("USD")
              ) =!= lit(0)
            )
          ),
        when(
          isnull(
            lookup("sup_code_fx_rate",
                   lookup("sup_api_member_pb", l_member_id_0)
                     .getField("billing_currency")
            )
          ).cast(BooleanType),
          lit("USD").cast(StringType)
        ).otherwise(
          lookup("sup_api_member_pb", l_member_id_0)
            .getField("billing_currency")
            .cast(StringType)
        )
      )
      .otherwise(l_agg_tl_network_analytics_pb_currency_billing_currency)
    l_agg_tl_network_analytics_pb_currency_billing_exchange_rate_0 = when(
      is_not_null(fx_rate_snapshot_id).cast(BooleanType),
      when(
        is_not_null(l_member_id_0)
          .cast(BooleanType)
          .and(
            is_not_null(lookup("sup_api_member_pb", l_member_id_0))
              .cast(BooleanType)
          )
          .and(
            is_not_null(
              lookup("sup_api_member_pb", l_member_id_0)
                .getField("billing_currency")
            ).cast(BooleanType)
          ),
        when(
          isnull(
            lookup("sup_bidder_fx_rates",
                   fx_rate_snapshot_id,
                   lookup("sup_api_member_pb", l_member_id_0).getField(
                     "billing_currency"
                   )
            ).getField("rate")
          ).cast(BooleanType),
          lit(1.0d).cast(DoubleType)
        ).otherwise(
          lookup("sup_bidder_fx_rates",
                 fx_rate_snapshot_id,
                 lookup("sup_api_member_pb", l_member_id_0).getField(
                   "billing_currency"
                 )
          ).getField("rate").cast(DoubleType)
        )
      ).otherwise(l_agg_tl_network_analytics_pb_currency_billing_exchange_rate)
    ).when(
        is_not_null(l_member_id_0)
          .cast(BooleanType)
          .and(
            is_not_null(lookup("sup_api_member_pb", l_member_id_0))
              .cast(BooleanType)
          )
          .and(
            is_not_null(
              lookup("sup_api_member_pb", l_member_id_0)
                .getField("billing_currency")
            ).and(
              string_compare(lookup("sup_api_member_pb", l_member_id_0)
                               .getField("billing_currency"),
                             lit("USD")
              ) =!= lit(0)
            )
          ),
        when(
          isnull(
            lookup("sup_code_fx_rate",
                   lookup("sup_api_member_pb", l_member_id_0)
                     .getField("billing_currency")
            )
          ).cast(BooleanType),
          lit(1.0d).cast(DoubleType)
        ).otherwise(
          lookup("sup_code_fx_rate",
                 lookup("sup_api_member_pb", l_member_id_0)
                   .getField("billing_currency")
          ).getField("rate").cast(DoubleType)
        )
      )
      .otherwise(l_agg_tl_network_analytics_pb_currency_billing_exchange_rate)
      .cast(DoubleType)
    l_agg_tl_network_analytics_pb_currency_fx_rate_snapshot_id_0 =
      when(is_not_null(fx_rate_snapshot_id).cast(BooleanType),
           l_agg_tl_network_analytics_pb_currency_fx_rate_snapshot_id
      ).when(
          is_not_null(l_member_id_0).cast(BooleanType),
          when(
            is_not_null(lookup("sup_api_member_pb", l_member_id_0))
              .cast(BooleanType),
            when(
              is_not_null(
                lookup("sup_api_member_pb", l_member_id_0)
                  .getField("billing_currency")
              ).and(
                string_compare(lookup("sup_api_member_pb", l_member_id_0)
                                 .getField("billing_currency"),
                               lit("USD")
                ) =!= lit(0)
              ),
              when(
                isnull(
                  lookup("sup_code_fx_rate",
                         lookup("sup_api_member_pb", l_member_id_0)
                           .getField("billing_currency")
                  )
                ).cast(BooleanType),
                l_agg_tl_network_analytics_pb_currency_fx_rate_snapshot_id_5
              ).otherwise(
                lookup("sup_code_fx_rate",
                       lookup("sup_api_member_pb", l_member_id_0)
                         .getField("billing_currency")
                ).getField("fx_rate_snapshot_id").cast(IntegerType)
              )
            ).otherwise(
              l_agg_tl_network_analytics_pb_currency_fx_rate_snapshot_id_5
            )
          ).otherwise(
            l_agg_tl_network_analytics_pb_currency_fx_rate_snapshot_id_3
          )
        )
        .otherwise(l_agg_tl_network_analytics_pb_currency_fx_rate_snapshot_id_1)
        .cast(IntegerType)
    var l_agg_tl_network_analytics_pb_currency_advertiser_exchange_rate_0
      : org.apache.spark.sql.Column =
      resolve_exchange_rate(coalesce(advertiser_currency, lit("USD")),
                            fx_rate_snapshot_id,
                            coalesce(advertiser_currency_rate, lit(1.0d))
      ).cast(DoubleType)
    l_agg_tl_network_analytics_pb_currency = struct(
      l_agg_tl_network_analytics_pb_currency_fx_rate_snapshot_id_0.as(
        "fx_rate_snapshot_id"
      ),
      l_agg_tl_network_analytics_pb_currency_publisher_currency_2.as(
        "publisher_currency"
      ),
      l_agg_tl_network_analytics_pb_currency_publisher_exchange_rate_2.as(
        "publisher_exchange_rate"
      ),
      l_agg_tl_network_analytics_pb_currency_advertiser_currency.as(
        "advertiser_currency"
      ),
      l_agg_tl_network_analytics_pb_currency_advertiser_exchange_rate_0.as(
        "advertiser_exchange_rate"
      ),
      l_agg_tl_network_analytics_pb_currency_advertiser_default_currency_0.as(
        "advertiser_default_currency"
      ),
      l_agg_tl_network_analytics_pb_currency_advertiser_default_exchange_rate_0
        .as("advertiser_default_exchange_rate"),
      l_agg_tl_network_analytics_pb_currency_member_currency_0.as(
        "member_currency"
      ),
      l_agg_tl_network_analytics_pb_currency_member_exchange_rate_0.as(
        "member_exchange_rate"
      ),
      l_agg_tl_network_analytics_pb_currency_billing_currency_0.as(
        "billing_currency"
      ),
      l_agg_tl_network_analytics_pb_currency_billing_exchange_rate_0.as(
        "billing_exchange_rate"
      )
    )
    l_agg_tl_network_analytics_pb_currency
  }

  def is_currency_2_enabled_member(member_id: Column): Column = {
    var i: org.apache.spark.sql.Column = lit(0)
    var currency_2_enabled_members: org.apache.spark.sql.Column = array(
      lit(958),
      lit(1540),
      lit(1558),
      lit(1705),
      lit(1783),
      lit(2025),
      lit(2331),
      lit(2902),
      lit(2914),
      lit(2982),
      lit(2991),
      lit(3232),
      lit(3273),
      lit(3296),
      lit(3397),
      lit(3525),
      lit(3542),
      lit(3646),
      lit(3660),
      lit(3665),
      lit(3700),
      lit(3741),
      lit(3754),
      lit(3927),
      lit(4013),
      lit(6834),
      lit(6839),
      lit(6925),
      lit(6931),
      lit(6949),
      lit(6989),
      lit(7037),
      lit(7068),
      lit(7164),
      lit(7254),
      lit(7404),
      lit(7458),
      lit(7463),
      lit(7544),
      lit(7632),
      lit(7648),
      lit(7729),
      lit(7800),
      lit(7823),
      lit(7876),
      lit(8099),
      lit(8125),
      lit(8147),
      lit(8186),
      lit(8253),
      lit(8266),
      lit(8272),
      lit(8427),
      lit(8484),
      lit(8492),
      lit(8740),
      lit(8878),
      lit(8907),
      lit(9027),
      lit(9178),
      lit(9238),
      lit(9284),
      lit(9290),
      lit(9343),
      lit(9394),
      lit(9419),
      lit(9446),
      lit(9470),
      lit(9518),
      lit(9568),
      lit(9573),
      lit(9677),
      lit(9700),
      lit(9722),
      lit(9724),
      lit(9742),
      lit(9795),
      lit(9820),
      lit(9826),
      lit(9844),
      lit(9923),
      lit(9953),
      lit(9957),
      lit(10040),
      lit(10043),
      lit(10157),
      lit(10199),
      lit(10251),
      lit(10272),
      lit(10316),
      lit(10360),
      lit(10653)
    )
    var max_i: org.apache.spark.sql.Column =
      size(currency_2_enabled_members).cast(IntegerType)
    lit(1)
  }

  def f_get_agg_platform_video_analytics_hourly_pb_currency(
    fx_rate_snapshot_id:               Column,
    advertiser_currency:               Column,
    advertiser_currency_rate:          Column,
    publisher_currency:                Column,
    publisher_currency_rate:           Column,
    advertiser_id:                     Column,
    imp_type:                          Column,
    buyer_member_id:                   Column,
    seller_member_id:                  Column,
    sup_code_fx_rate_lookup_5:         Column,
    sup_code_fx_rate_lookup_4:         Column,
    sup_code_fx_rate_lookup_3:         Column,
    sup_bidder_advertiser_pb_lookup_2: Column,
    sup_code_fx_rate_lookup_2:         Column,
    sup_code_fx_rate_lookup:           Column,
    sup_bidder_advertiser_pb_lookup:   Column
  ): Column = {
    var l_agg_platform_video_analytics_hourly_pb_currency_fx_rate_snapshot_id
      : org.apache.spark.sql.Column = lit(null).cast(IntegerType)
    var l_agg_platform_video_analytics_hourly_pb_currency_advertiser_currency
      : org.apache.spark.sql.Column = lit(null).cast(StringType)
    var l_agg_platform_video_analytics_hourly_pb_currency_advertiser_exchange_rate
      : org.apache.spark.sql.Column = lit(null).cast(DoubleType)
    var l_agg_platform_video_analytics_hourly_pb_currency_publisher_currency
      : org.apache.spark.sql.Column = lit(null).cast(StringType)
    var l_agg_platform_video_analytics_hourly_pb_currency_publisher_exchange_rate
      : org.apache.spark.sql.Column = lit(null).cast(DoubleType)
    var l_agg_platform_video_analytics_hourly_pb_currency_advertiser_default_currency
      : org.apache.spark.sql.Column = lit(null).cast(StringType)
    var l_agg_platform_video_analytics_hourly_pb_currency_advertiser_default_exchange_rate
      : org.apache.spark.sql.Column = lit(null).cast(DoubleType)
    var l_agg_platform_video_analytics_hourly_pb_currency_member_currency
      : org.apache.spark.sql.Column = lit(null).cast(StringType)
    var l_agg_platform_video_analytics_hourly_pb_currency_member_exchange_rate
      : org.apache.spark.sql.Column = lit(null).cast(DoubleType)
    var l_agg_platform_video_analytics_hourly_pb_currency_billing_currency
      : org.apache.spark.sql.Column = lit(null).cast(StringType)
    var l_agg_platform_video_analytics_hourly_pb_currency_billing_exchange_rate
      : org.apache.spark.sql.Column = lit(null).cast(DoubleType)
    var l_api_member_dde: org.apache.spark.sql.Column = lit(null).cast(
      StructType(
        List(
          StructField("id",                    IntegerType, true),
          StructField("billing_name",          StringType,  true),
          StructField("bidder_id",             IntegerType, true),
          StructField("is_billable",           IntegerType, true),
          StructField("enable_ip_truncation",  IntegerType, true),
          StructField("vendor_id",             IntegerType, true),
          StructField("final_auction_type_id", IntegerType, true),
          StructField("default_referrer_url",  StringType,  true),
          StructField("member_currency",       StringType,  true),
          StructField("billing_currency",      StringType,  true)
        )
      )
    )
    var l_api_member_dde_1: org.apache.spark.sql.Column = lit(null).cast(
      StructType(
        List(
          StructField("id",                    IntegerType, true),
          StructField("billing_name",          StringType,  true),
          StructField("bidder_id",             IntegerType, true),
          StructField("is_billable",           IntegerType, true),
          StructField("enable_ip_truncation",  IntegerType, true),
          StructField("vendor_id",             IntegerType, true),
          StructField("final_auction_type_id", IntegerType, true),
          StructField("default_referrer_url",  StringType,  true),
          StructField("member_currency",       StringType,  true),
          StructField("billing_currency",      StringType,  true)
        )
      )
    )
    var l_api_member_dde_3: org.apache.spark.sql.Column = lit(null).cast(
      StructType(
        List(
          StructField("id",                    IntegerType, true),
          StructField("billing_name",          StringType,  true),
          StructField("bidder_id",             IntegerType, true),
          StructField("is_billable",           IntegerType, true),
          StructField("enable_ip_truncation",  IntegerType, true),
          StructField("vendor_id",             IntegerType, true),
          StructField("final_auction_type_id", IntegerType, true),
          StructField("default_referrer_url",  StringType,  true),
          StructField("member_currency",       StringType,  true),
          StructField("billing_currency",      StringType,  true)
        )
      )
    )
    var l_currency:    org.apache.spark.sql.Column = rpad(lit(null), 3, " ")
    var l_currency_1:  org.apache.spark.sql.Column = rpad(lit(null), 3, " ")
    var l_currency_3:  org.apache.spark.sql.Column = rpad(lit(null), 3, " ")
    var l_member_id:   org.apache.spark.sql.Column = lit(null).cast(IntegerType)
    var l_member_id_0: org.apache.spark.sql.Column = lit(null).cast(IntegerType)
    var l_agg_platform_video_analytics_hourly_pb_currency
      : org.apache.spark.sql.Column = lit(null).cast(
      StructType(
        List(
          StructField("fx_rate_snapshot_id",              IntegerType, true),
          StructField("advertiser_currency",              StringType,  true),
          StructField("advertiser_exchange_rate",         DoubleType,  true),
          StructField("publisher_currency",               StringType,  true),
          StructField("publisher_exchange_rate",          DoubleType,  true),
          StructField("advertiser_default_currency",      StringType,  true),
          StructField("advertiser_default_exchange_rate", DoubleType,  true),
          StructField("member_currency",                  StringType,  true),
          StructField("member_exchange_rate",             DoubleType,  true),
          StructField("billing_currency",                 StringType,  true),
          StructField("billing_exchange_rate",            DoubleType,  true)
        )
      )
    )
    var l_sup_bidder_fx_rate: org.apache.spark.sql.Column = lit(null).cast(
      StructType(
        List(
          StructField("fx_rate_snapshot_id", IntegerType, true),
          StructField("currency_id",         IntegerType, true),
          StructField("code",                StringType,  true),
          StructField("rate",                DoubleType,  true),
          StructField("as_of_timestamp",     LongType,    true)
        )
      )
    )
    var l_sup_bidder_fx_rate_5: org.apache.spark.sql.Column = lit(null).cast(
      StructType(
        List(
          StructField("fx_rate_snapshot_id", IntegerType, true),
          StructField("currency_id",         IntegerType, true),
          StructField("code",                StringType,  true),
          StructField("rate",                DoubleType,  true),
          StructField("as_of_timestamp",     LongType,    true)
        )
      )
    )
    var l_sup_bidder_fx_rate_4: org.apache.spark.sql.Column = lit(null).cast(
      StructType(
        List(
          StructField("fx_rate_snapshot_id", IntegerType, true),
          StructField("currency_id",         IntegerType, true),
          StructField("code",                StringType,  true),
          StructField("rate",                DoubleType,  true),
          StructField("as_of_timestamp",     LongType,    true)
        )
      )
    )
    var l_sup_bidder_fx_rate_1: org.apache.spark.sql.Column = lit(null).cast(
      StructType(
        List(
          StructField("fx_rate_snapshot_id", IntegerType, true),
          StructField("currency_id",         IntegerType, true),
          StructField("code",                StringType,  true),
          StructField("rate",                DoubleType,  true),
          StructField("as_of_timestamp",     LongType,    true)
        )
      )
    )
    var l_sup_bidder_fx_rate_0: org.apache.spark.sql.Column = lit(null).cast(
      StructType(
        List(
          StructField("fx_rate_snapshot_id", IntegerType, true),
          StructField("currency_id",         IntegerType, true),
          StructField("code",                StringType,  true),
          StructField("rate",                DoubleType,  true),
          StructField("as_of_timestamp",     LongType,    true)
        )
      )
    )
    l_agg_platform_video_analytics_hourly_pb_currency_fx_rate_snapshot_id =
      fx_rate_snapshot_id.cast(IntegerType)
    l_agg_platform_video_analytics_hourly_pb_currency_advertiser_currency =
      coalesce(advertiser_currency, lit("USD"))
    l_agg_platform_video_analytics_hourly_pb_currency_advertiser_exchange_rate =
      coalesce(advertiser_currency_rate, lit(1.0d)).cast(DoubleType)
    l_agg_platform_video_analytics_hourly_pb_currency_advertiser_currency =
      when(isnull(fx_rate_snapshot_id).and(
             isnull(advertiser_currency).or(isnull(advertiser_currency_rate))
           ),
           lit("USD")
      ).otherwise(
        l_agg_platform_video_analytics_hourly_pb_currency_advertiser_currency
      )
    l_agg_platform_video_analytics_hourly_pb_currency_advertiser_exchange_rate =
      when(isnull(fx_rate_snapshot_id).and(
             isnull(advertiser_currency).or(isnull(advertiser_currency_rate))
           ),
           lit(1.0d)
      ).otherwise(
          l_agg_platform_video_analytics_hourly_pb_currency_advertiser_exchange_rate
        )
        .cast(DoubleType)
    l_agg_platform_video_analytics_hourly_pb_currency_publisher_currency =
      coalesce(publisher_currency, lit("USD"))
    l_agg_platform_video_analytics_hourly_pb_currency_publisher_exchange_rate =
      coalesce(publisher_currency_rate, lit(1.0d)).cast(DoubleType)
    l_agg_platform_video_analytics_hourly_pb_currency_publisher_currency =
      when(isnull(fx_rate_snapshot_id).and(
             isnull(publisher_currency).or(isnull(publisher_currency_rate))
           ),
           lit("USD")
      ).otherwise(
        l_agg_platform_video_analytics_hourly_pb_currency_publisher_currency
      )
    l_agg_platform_video_analytics_hourly_pb_currency_publisher_exchange_rate =
      when(isnull(fx_rate_snapshot_id).and(
             isnull(publisher_currency).or(isnull(publisher_currency_rate))
           ),
           lit(1.0d)
      ).otherwise(
          l_agg_platform_video_analytics_hourly_pb_currency_publisher_exchange_rate
        )
        .cast(DoubleType)
    l_agg_platform_video_analytics_hourly_pb_currency_advertiser_default_currency =
      lit("USD")
    l_agg_platform_video_analytics_hourly_pb_currency_advertiser_default_exchange_rate =
      lit(1.0d).cast(DoubleType)
    l_agg_platform_video_analytics_hourly_pb_currency_billing_currency = lit(
      "USD"
    )
    l_agg_platform_video_analytics_hourly_pb_currency_billing_exchange_rate =
      lit(1.0d).cast(DoubleType)
    l_agg_platform_video_analytics_hourly_pb_currency_member_currency = lit(
      "USD"
    )
    l_agg_platform_video_analytics_hourly_pb_currency_member_exchange_rate =
      lit(1.0d).cast(DoubleType)
    l_member_id_0 = when(
      is_not_null(imp_type).cast(BooleanType),
      when(imp_type
             .isin(lit(9), lit(10), lit(11), lit(7), lit(5))
             .and(is_not_null(buyer_member_id)),
           buyer_member_id.cast(IntegerType)
      ).when(is_not_null(seller_member_id).cast(BooleanType),
              seller_member_id.cast(IntegerType)
        )
        .otherwise(l_member_id)
    ).otherwise(l_member_id).cast(IntegerType)
    l_agg_platform_video_analytics_hourly_pb_currency_fx_rate_snapshot_id =
      when(
        isnull(fx_rate_snapshot_id).and(
          string_compare(
            l_agg_platform_video_analytics_hourly_pb_currency_advertiser_currency,
            lit("USD")
          ) =!= lit(0)
        ),
        when(is_not_null(sup_code_fx_rate_lookup),
             sup_code_fx_rate_lookup.getField("fx_rate_snapshot_id")
        ).otherwise(
          l_agg_platform_video_analytics_hourly_pb_currency_fx_rate_snapshot_id
        )
      ).otherwise(
          l_agg_platform_video_analytics_hourly_pb_currency_fx_rate_snapshot_id
        )
        .cast(IntegerType)
    l_agg_platform_video_analytics_hourly_pb_currency_fx_rate_snapshot_id =
      when(
        isnull(fx_rate_snapshot_id).and(
          string_compare(
            l_agg_platform_video_analytics_hourly_pb_currency_publisher_currency,
            lit("USD")
          ) =!= lit(0)
        ),
        when(is_not_null(sup_code_fx_rate_lookup_2),
             sup_code_fx_rate_lookup_2.getField("fx_rate_snapshot_id")
        ).otherwise(
          l_agg_platform_video_analytics_hourly_pb_currency_fx_rate_snapshot_id
        )
      ).otherwise(
          l_agg_platform_video_analytics_hourly_pb_currency_fx_rate_snapshot_id
        )
        .cast(IntegerType)
    l_agg_platform_video_analytics_hourly_pb_currency_fx_rate_snapshot_id =
      when(
        isnull(fx_rate_snapshot_id).and(is_not_null(l_member_id_0)), {
          l_agg_platform_video_analytics_hourly_pb_currency_fx_rate_snapshot_id =
            when(
              is_not_null(advertiser_id)
                .cast(BooleanType)
                .and(
                  is_not_null(
                    rpad(sup_bidder_advertiser_pb_lookup
                           .getField("advertiser_default_currency"),
                         3,
                         " "
                    )
                  ).and(
                    string_compare(
                      rpad(sup_bidder_advertiser_pb_lookup
                             .getField("advertiser_default_currency"),
                           3,
                           " "
                      ),
                      lit("USD")
                    ) =!= lit(0)
                  )
                )
                .and(is_not_null(sup_code_fx_rate_lookup_3).cast(BooleanType)),
              sup_code_fx_rate_lookup_3.getField("fx_rate_snapshot_id")
            ).otherwise(
                l_agg_platform_video_analytics_hourly_pb_currency_fx_rate_snapshot_id
              )
              .cast(IntegerType)
          l_agg_platform_video_analytics_hourly_pb_currency_fx_rate_snapshot_id =
            when(
              is_not_null(lookup("sup_api_member_pb", l_member_id_0)).cast(
                BooleanType
              ), {
                l_agg_platform_video_analytics_hourly_pb_currency_fx_rate_snapshot_id =
                  when(
                    is_not_null(
                      lookup("sup_api_member_pb", l_member_id_0).getField(
                        "member_currency"
                      )
                    ).and(
                        is_currency_2_enabled_member(l_member_id_0) === lit(1)
                      )
                      .and(
                        string_compare(lookup("sup_api_member_pb",
                                              l_member_id_0
                                       ).getField("member_currency"),
                                       lit("USD")
                        ) =!= lit(0)
                      ),
                    when(
                      is_not_null(
                        l_agg_platform_video_analytics_hourly_pb_currency_member_exchange_rate
                      ),
                      sup_code_fx_rate_lookup_4.getField("fx_rate_snapshot_id")
                    ).otherwise(
                      l_agg_platform_video_analytics_hourly_pb_currency_fx_rate_snapshot_id
                    )
                  ).otherwise(
                      l_agg_platform_video_analytics_hourly_pb_currency_fx_rate_snapshot_id
                    )
                    .cast(IntegerType)
                l_agg_platform_video_analytics_hourly_pb_currency_fx_rate_snapshot_id =
                  when(
                    is_not_null(
                      lookup("sup_api_member_pb", l_member_id_0).getField(
                        "billing_currency"
                      )
                    ).and(
                      string_compare(lookup("sup_api_member_pb", l_member_id_0)
                                       .getField("billing_currency"),
                                     lit("USD")
                      ) =!= lit(0)
                    ),
                    when(
                      is_not_null(sup_code_fx_rate_lookup_5),
                      sup_code_fx_rate_lookup_5.getField("fx_rate_snapshot_id")
                    ).otherwise(
                      l_agg_platform_video_analytics_hourly_pb_currency_fx_rate_snapshot_id
                    )
                  ).otherwise(
                      l_agg_platform_video_analytics_hourly_pb_currency_fx_rate_snapshot_id
                    )
                    .cast(IntegerType)
                l_agg_platform_video_analytics_hourly_pb_currency_fx_rate_snapshot_id
              }
            ).otherwise(
                l_agg_platform_video_analytics_hourly_pb_currency_fx_rate_snapshot_id
              )
              .cast(IntegerType)
          l_agg_platform_video_analytics_hourly_pb_currency_fx_rate_snapshot_id
        }
      ).otherwise(
          l_agg_platform_video_analytics_hourly_pb_currency_fx_rate_snapshot_id
        )
        .cast(IntegerType)
    l_agg_platform_video_analytics_hourly_pb_currency_advertiser_exchange_rate =
      when(
        is_not_null(fx_rate_snapshot_id).cast(BooleanType),
        when(
          string_compare(
            l_agg_platform_video_analytics_hourly_pb_currency_advertiser_currency,
            lit("USD")
          ) =!= lit(0), {
            l_agg_platform_video_analytics_hourly_pb_currency_advertiser_exchange_rate =
              lookup("sup_bidder_fx_rates",
                     fx_rate_snapshot_id,
                     advertiser_currency
              ).getField("rate").cast(DoubleType)
            l_agg_platform_video_analytics_hourly_pb_currency_advertiser_exchange_rate =
              when(isnull(
                     lookup("sup_bidder_fx_rates",
                            fx_rate_snapshot_id,
                            advertiser_currency
                     ).getField("rate")
                   ).cast(BooleanType),
                   lit(1.0d)
              ).otherwise(
                  l_agg_platform_video_analytics_hourly_pb_currency_advertiser_exchange_rate
                )
                .cast(DoubleType)
            l_agg_platform_video_analytics_hourly_pb_currency_advertiser_exchange_rate
          }
        ).otherwise(lit(1.0d))
      ).when(
          string_compare(
            l_agg_platform_video_analytics_hourly_pb_currency_advertiser_currency,
            lit("USD")
          ) =!= lit(0),
          when(isnull(sup_code_fx_rate_lookup).cast(BooleanType), lit(1.0d))
            .otherwise(sup_code_fx_rate_lookup.getField("rate"))
        )
        .otherwise(lit(1.0d))
        .cast(DoubleType)
    l_agg_platform_video_analytics_hourly_pb_currency_advertiser_currency =
      when(
        is_not_null(fx_rate_snapshot_id).cast(BooleanType),
        when(
          string_compare(
            l_agg_platform_video_analytics_hourly_pb_currency_advertiser_currency,
            lit("USD")
          ) =!= lit(0),
          when(isnull(
                 lookup("sup_bidder_fx_rates",
                        fx_rate_snapshot_id,
                        advertiser_currency
                 ).getField("rate")
               ).cast(BooleanType),
               lit("USD")
          ).otherwise(
            l_agg_platform_video_analytics_hourly_pb_currency_advertiser_currency
          )
        ).otherwise(
          l_agg_platform_video_analytics_hourly_pb_currency_advertiser_currency
        )
      ).when(
          string_compare(
            l_agg_platform_video_analytics_hourly_pb_currency_advertiser_currency,
            lit("USD")
          ) =!= lit(0),
          when(isnull(sup_code_fx_rate_lookup).cast(BooleanType), lit("USD"))
            .otherwise(
              l_agg_platform_video_analytics_hourly_pb_currency_advertiser_currency
            )
        )
        .otherwise(
          l_agg_platform_video_analytics_hourly_pb_currency_advertiser_currency
        )
    l_sup_bidder_fx_rate_1 = when(
      isnull(fx_rate_snapshot_id).and(
        string_compare(
          l_agg_platform_video_analytics_hourly_pb_currency_advertiser_currency,
          lit("USD")
        ) =!= lit(0)
      ),
      sup_code_fx_rate_lookup
    ).when(
        isnull(fx_rate_snapshot_id).and(
          not(
            string_compare(
              l_agg_platform_video_analytics_hourly_pb_currency_advertiser_currency,
              lit("USD")
            ) =!= lit(0)
          )
        ),
        l_sup_bidder_fx_rate
      )
      .otherwise(l_sup_bidder_fx_rate_1)
    l_agg_platform_video_analytics_hourly_pb_currency_publisher_exchange_rate =
      when(
        is_not_null(fx_rate_snapshot_id).cast(BooleanType),
        when(
          string_compare(
            l_agg_platform_video_analytics_hourly_pb_currency_publisher_currency,
            lit("USD")
          ) =!= lit(0), {
            l_agg_platform_video_analytics_hourly_pb_currency_publisher_exchange_rate =
              lookup("sup_bidder_fx_rates",
                     fx_rate_snapshot_id,
                     publisher_currency
              ).getField("rate").cast(DoubleType)
            l_agg_platform_video_analytics_hourly_pb_currency_publisher_exchange_rate =
              when(isnull(
                     lookup("sup_bidder_fx_rates",
                            fx_rate_snapshot_id,
                            publisher_currency
                     ).getField("rate")
                   ).cast(BooleanType),
                   lit(1.0d)
              ).otherwise(
                  l_agg_platform_video_analytics_hourly_pb_currency_publisher_exchange_rate
                )
                .cast(DoubleType)
            l_agg_platform_video_analytics_hourly_pb_currency_publisher_exchange_rate
          }
        ).otherwise(lit(1.0d))
      ).when(
          string_compare(
            l_agg_platform_video_analytics_hourly_pb_currency_publisher_currency,
            lit("USD")
          ) =!= lit(0),
          when(isnull(sup_code_fx_rate_lookup_2).cast(BooleanType), lit(1.0d))
            .otherwise(sup_code_fx_rate_lookup_2.getField("rate"))
        )
        .otherwise(lit(1.0d))
        .cast(DoubleType)
    l_sup_bidder_fx_rate_5 = when(
      isnull(fx_rate_snapshot_id).and(is_not_null(l_member_id_0)),
      when(
        is_not_null(advertiser_id).cast(BooleanType),
        when(
          is_not_null(
            rpad(sup_bidder_advertiser_pb_lookup.getField(
                   "advertiser_default_currency"
                 ),
                 3,
                 " "
            )
          ).and(
            string_compare(rpad(sup_bidder_advertiser_pb_lookup
                                  .getField("advertiser_default_currency"),
                                3,
                                " "
                           ),
                           lit("USD")
            ) =!= lit(0)
          ),
          sup_code_fx_rate_lookup_3
        ).otherwise(l_sup_bidder_fx_rate_0)
      ).otherwise(l_sup_bidder_fx_rate_0)
    ).otherwise(l_sup_bidder_fx_rate_5)
    l_agg_platform_video_analytics_hourly_pb_currency_publisher_currency = when(
      is_not_null(fx_rate_snapshot_id).cast(BooleanType),
      when(
        string_compare(
          l_agg_platform_video_analytics_hourly_pb_currency_publisher_currency,
          lit("USD")
        ) =!= lit(0),
        when(isnull(
               lookup("sup_bidder_fx_rates",
                      fx_rate_snapshot_id,
                      publisher_currency
               ).getField("rate")
             ).cast(BooleanType),
             lit("USD")
        ).otherwise(
          l_agg_platform_video_analytics_hourly_pb_currency_publisher_currency
        )
      ).otherwise(
        l_agg_platform_video_analytics_hourly_pb_currency_publisher_currency
      )
    ).when(
        string_compare(
          l_agg_platform_video_analytics_hourly_pb_currency_publisher_currency,
          lit("USD")
        ) =!= lit(0),
        when(isnull(sup_code_fx_rate_lookup_2).cast(BooleanType), lit("USD"))
          .otherwise(
            l_agg_platform_video_analytics_hourly_pb_currency_publisher_currency
          )
      )
      .otherwise(
        l_agg_platform_video_analytics_hourly_pb_currency_publisher_currency
      )
    l_agg_platform_video_analytics_hourly_pb_currency_advertiser_default_exchange_rate =
      when(
        is_not_null(fx_rate_snapshot_id).cast(BooleanType),
        when(
          is_not_null(l_member_id_0)
            .cast(BooleanType)
            .and(is_not_null(advertiser_id).cast(BooleanType))
            .and(
              is_not_null(
                rpad(sup_bidder_advertiser_pb_lookup
                       .getField("advertiser_default_currency"),
                     3,
                     " "
                )
              ).and(
                string_compare(rpad(sup_bidder_advertiser_pb_lookup
                                      .getField("advertiser_default_currency"),
                                    3,
                                    " "
                               ),
                               lit("USD")
                ) =!= lit(0)
              )
            ), {
            l_agg_platform_video_analytics_hourly_pb_currency_advertiser_default_exchange_rate =
              lookup("sup_bidder_fx_rates",
                     fx_rate_snapshot_id,
                     rpad(sup_bidder_advertiser_pb_lookup.getField(
                            "advertiser_default_currency"
                          ),
                          3,
                          " "
                     )
              ).getField("rate").cast(DoubleType)
            l_agg_platform_video_analytics_hourly_pb_currency_advertiser_default_exchange_rate =
              when(isnull(
                     l_agg_platform_video_analytics_hourly_pb_currency_advertiser_default_exchange_rate
                   ),
                   lit(1.0d)
              ).otherwise(
                  l_agg_platform_video_analytics_hourly_pb_currency_advertiser_default_exchange_rate
                )
                .cast(DoubleType)
            l_agg_platform_video_analytics_hourly_pb_currency_advertiser_default_exchange_rate
          }
        ).otherwise(
          l_agg_platform_video_analytics_hourly_pb_currency_advertiser_default_exchange_rate
        )
      ).when(
          is_not_null(l_member_id_0)
            .cast(BooleanType)
            .and(is_not_null(advertiser_id).cast(BooleanType))
            .and(
              is_not_null(
                rpad(sup_bidder_advertiser_pb_lookup
                       .getField("advertiser_default_currency"),
                     3,
                     " "
                )
              ).and(
                string_compare(rpad(sup_bidder_advertiser_pb_lookup
                                      .getField("advertiser_default_currency"),
                                    3,
                                    " "
                               ),
                               lit("USD")
                ) =!= lit(0)
              )
            ),
          when(is_not_null(sup_code_fx_rate_lookup_3).cast(BooleanType),
               sup_code_fx_rate_lookup_3.getField("rate")
          ).otherwise(lit(1.0d))
        )
        .otherwise(
          l_agg_platform_video_analytics_hourly_pb_currency_advertiser_default_exchange_rate
        )
        .cast(DoubleType)
    l_agg_platform_video_analytics_hourly_pb_currency_advertiser_default_currency =
      when(
        is_not_null(fx_rate_snapshot_id).cast(BooleanType),
        when(
          is_not_null(l_member_id_0)
            .cast(BooleanType)
            .and(is_not_null(advertiser_id).cast(BooleanType))
            .and(
              is_not_null(
                rpad(sup_bidder_advertiser_pb_lookup
                       .getField("advertiser_default_currency"),
                     3,
                     " "
                )
              ).and(
                string_compare(rpad(sup_bidder_advertiser_pb_lookup
                                      .getField("advertiser_default_currency"),
                                    3,
                                    " "
                               ),
                               lit("USD")
                ) =!= lit(0)
              )
            ),
          when(
            is_not_null(
              l_agg_platform_video_analytics_hourly_pb_currency_advertiser_default_exchange_rate
            ).cast(BooleanType),
            rpad(sup_bidder_advertiser_pb_lookup.getField(
                   "advertiser_default_currency"
                 ),
                 3,
                 " "
            )
          ).otherwise(lit("USD"))
        ).otherwise(
          l_agg_platform_video_analytics_hourly_pb_currency_advertiser_default_currency
        )
      ).when(
          is_not_null(l_member_id_0)
            .cast(BooleanType)
            .and(is_not_null(advertiser_id).cast(BooleanType))
            .and(
              is_not_null(
                rpad(sup_bidder_advertiser_pb_lookup
                       .getField("advertiser_default_currency"),
                     3,
                     " "
                )
              ).and(
                string_compare(rpad(sup_bidder_advertiser_pb_lookup
                                      .getField("advertiser_default_currency"),
                                    3,
                                    " "
                               ),
                               lit("USD")
                ) =!= lit(0)
              )
            ),
          when(is_not_null(sup_code_fx_rate_lookup_3).cast(BooleanType),
               rpad(sup_bidder_advertiser_pb_lookup.getField(
                      "advertiser_default_currency"
                    ),
                    3,
                    " "
               )
          ).otherwise(lit("USD"))
        )
        .otherwise(
          l_agg_platform_video_analytics_hourly_pb_currency_advertiser_default_currency
        )
    l_agg_platform_video_analytics_hourly_pb_currency_billing_currency = when(
      is_not_null(fx_rate_snapshot_id).cast(BooleanType),
      when(
        is_not_null(l_member_id_0)
          .cast(BooleanType)
          .and(
            is_not_null(lookup("sup_api_member_pb", l_member_id_0))
              .cast(BooleanType)
          )
          .and(
            is_not_null(
              lookup("sup_api_member_pb", l_member_id_0)
                .getField("billing_currency")
            ).cast(BooleanType)
          ), {
          l_agg_platform_video_analytics_hourly_pb_currency_billing_currency =
            lookup("sup_api_member_pb", l_member_id_0).getField(
              "billing_currency"
            )
          l_agg_platform_video_analytics_hourly_pb_currency_billing_currency =
            when(
              isnull(
                lookup("sup_bidder_fx_rates",
                       fx_rate_snapshot_id,
                       lookup("sup_api_member_pb", l_member_id_0).getField(
                         "billing_currency"
                       )
                ).getField("rate")
              ).cast(BooleanType),
              lit("USD")
            ).otherwise(
              l_agg_platform_video_analytics_hourly_pb_currency_billing_currency
            )
          l_agg_platform_video_analytics_hourly_pb_currency_billing_currency
        }
      ).otherwise(
        l_agg_platform_video_analytics_hourly_pb_currency_billing_currency
      )
    ).when(
        is_not_null(l_member_id_0)
          .cast(BooleanType)
          .and(
            is_not_null(lookup("sup_api_member_pb", l_member_id_0))
              .cast(BooleanType)
          )
          .and(
            is_not_null(
              lookup("sup_api_member_pb", l_member_id_0)
                .getField("billing_currency")
            ).and(
              string_compare(lookup("sup_api_member_pb", l_member_id_0)
                               .getField("billing_currency"),
                             lit("USD")
              ) =!= lit(0)
            )
          ),
        when(isnull(sup_code_fx_rate_lookup_5).cast(BooleanType), lit("USD"))
          .otherwise(
            lookup("sup_api_member_pb", l_member_id_0)
              .getField("billing_currency")
          )
      )
      .otherwise(
        l_agg_platform_video_analytics_hourly_pb_currency_billing_currency
      )
    l_agg_platform_video_analytics_hourly_pb_currency_member_exchange_rate =
      when(
        is_not_null(fx_rate_snapshot_id).cast(BooleanType),
        when(
          is_not_null(l_member_id_0)
            .cast(BooleanType)
            .and(
              is_not_null(lookup("sup_api_member_pb", l_member_id_0))
                .cast(BooleanType)
            )
            .and(
              is_not_null(
                lookup("sup_api_member_pb", l_member_id_0)
                  .getField("member_currency")
              ).and(is_currency_2_enabled_member(l_member_id_0) === lit(1))
            ), {
            l_agg_platform_video_analytics_hourly_pb_currency_member_exchange_rate =
              lookup("sup_bidder_fx_rates",
                     fx_rate_snapshot_id,
                     lookup("sup_api_member_pb", l_member_id_0).getField(
                       "member_currency"
                     )
              ).getField("rate").cast(DoubleType)
            l_agg_platform_video_analytics_hourly_pb_currency_member_exchange_rate =
              when(
                isnull(
                  lookup("sup_bidder_fx_rates",
                         fx_rate_snapshot_id,
                         lookup("sup_api_member_pb", l_member_id_0).getField(
                           "member_currency"
                         )
                  ).getField("rate")
                ).cast(BooleanType),
                lit(1.0d)
              ).otherwise(
                  l_agg_platform_video_analytics_hourly_pb_currency_member_exchange_rate
                )
                .cast(DoubleType)
            l_agg_platform_video_analytics_hourly_pb_currency_member_exchange_rate
          }
        ).otherwise(
          l_agg_platform_video_analytics_hourly_pb_currency_member_exchange_rate
        )
      ).when(
          is_not_null(l_member_id_0)
            .cast(BooleanType)
            .and(
              is_not_null(lookup("sup_api_member_pb", l_member_id_0))
                .cast(BooleanType)
            )
            .and(
              is_not_null(
                lookup("sup_api_member_pb", l_member_id_0)
                  .getField("member_currency")
              ).and(is_currency_2_enabled_member(l_member_id_0) === lit(1))
                .and(
                  string_compare(lookup("sup_api_member_pb", l_member_id_0)
                                   .getField("member_currency"),
                                 lit("USD")
                  ) =!= lit(0)
                )
            ),
          when(isnull(
                 l_agg_platform_video_analytics_hourly_pb_currency_member_exchange_rate
               ).cast(BooleanType),
               lit(1.0d)
          ).otherwise(sup_code_fx_rate_lookup_4.getField("rate"))
        )
        .otherwise(
          l_agg_platform_video_analytics_hourly_pb_currency_member_exchange_rate
        )
        .cast(DoubleType)
    l_agg_platform_video_analytics_hourly_pb_currency_billing_exchange_rate =
      when(
        is_not_null(fx_rate_snapshot_id).cast(BooleanType),
        when(
          is_not_null(l_member_id_0)
            .cast(BooleanType)
            .and(
              is_not_null(lookup("sup_api_member_pb", l_member_id_0))
                .cast(BooleanType)
            )
            .and(
              is_not_null(
                lookup("sup_api_member_pb", l_member_id_0)
                  .getField("billing_currency")
              ).cast(BooleanType)
            ), {
            l_agg_platform_video_analytics_hourly_pb_currency_billing_exchange_rate =
              lookup("sup_bidder_fx_rates",
                     fx_rate_snapshot_id,
                     lookup("sup_api_member_pb", l_member_id_0).getField(
                       "billing_currency"
                     )
              ).getField("rate").cast(DoubleType)
            l_agg_platform_video_analytics_hourly_pb_currency_billing_exchange_rate =
              when(
                isnull(
                  lookup("sup_bidder_fx_rates",
                         fx_rate_snapshot_id,
                         lookup("sup_api_member_pb", l_member_id_0).getField(
                           "billing_currency"
                         )
                  ).getField("rate")
                ).cast(BooleanType),
                lit(1.0d)
              ).otherwise(
                  l_agg_platform_video_analytics_hourly_pb_currency_billing_exchange_rate
                )
                .cast(DoubleType)
            l_agg_platform_video_analytics_hourly_pb_currency_billing_exchange_rate
          }
        ).otherwise(
          l_agg_platform_video_analytics_hourly_pb_currency_billing_exchange_rate
        )
      ).when(
          is_not_null(l_member_id_0)
            .cast(BooleanType)
            .and(
              is_not_null(lookup("sup_api_member_pb", l_member_id_0))
                .cast(BooleanType)
            )
            .and(
              is_not_null(
                lookup("sup_api_member_pb", l_member_id_0)
                  .getField("billing_currency")
              ).and(
                string_compare(lookup("sup_api_member_pb", l_member_id_0)
                                 .getField("billing_currency"),
                               lit("USD")
                ) =!= lit(0)
              )
            ),
          when(isnull(sup_code_fx_rate_lookup_5).cast(BooleanType), lit(1.0d))
            .otherwise(sup_code_fx_rate_lookup_5.getField("rate"))
        )
        .otherwise(
          l_agg_platform_video_analytics_hourly_pb_currency_billing_exchange_rate
        )
        .cast(DoubleType)
    l_agg_platform_video_analytics_hourly_pb_currency_member_currency = when(
      is_not_null(fx_rate_snapshot_id).cast(BooleanType),
      when(
        is_not_null(l_member_id_0)
          .cast(BooleanType)
          .and(
            is_not_null(lookup("sup_api_member_pb", l_member_id_0))
              .cast(BooleanType)
          )
          .and(
            is_not_null(
              lookup("sup_api_member_pb", l_member_id_0)
                .getField("member_currency")
            ).and(is_currency_2_enabled_member(l_member_id_0) === lit(1))
          ), {
          l_agg_platform_video_analytics_hourly_pb_currency_member_currency =
            lookup("sup_api_member_pb", l_member_id_0).getField(
              "member_currency"
            )
          l_agg_platform_video_analytics_hourly_pb_currency_member_currency =
            when(
              isnull(
                lookup("sup_bidder_fx_rates",
                       fx_rate_snapshot_id,
                       lookup("sup_api_member_pb", l_member_id_0).getField(
                         "member_currency"
                       )
                ).getField("rate")
              ).cast(BooleanType),
              lit("USD")
            ).otherwise(
              l_agg_platform_video_analytics_hourly_pb_currency_member_currency
            )
          l_agg_platform_video_analytics_hourly_pb_currency_member_currency
        }
      ).otherwise(
        l_agg_platform_video_analytics_hourly_pb_currency_member_currency
      )
    ).when(
        is_not_null(l_member_id_0)
          .cast(BooleanType)
          .and(
            is_not_null(lookup("sup_api_member_pb", l_member_id_0))
              .cast(BooleanType)
          )
          .and(
            is_not_null(
              lookup("sup_api_member_pb", l_member_id_0)
                .getField("member_currency")
            ).and(is_currency_2_enabled_member(l_member_id_0) === lit(1))
              .and(
                string_compare(lookup("sup_api_member_pb", l_member_id_0)
                                 .getField("member_currency"),
                               lit("USD")
                ) =!= lit(0)
              )
          ),
        when(isnull(
               l_agg_platform_video_analytics_hourly_pb_currency_member_exchange_rate
             ).cast(BooleanType),
             lit("USD")
        ).otherwise(
          lookup("sup_api_member_pb", l_member_id_0).getField("member_currency")
        )
      )
      .otherwise(
        l_agg_platform_video_analytics_hourly_pb_currency_member_currency
      )
    l_agg_platform_video_analytics_hourly_pb_currency = struct(
      l_agg_platform_video_analytics_hourly_pb_currency_fx_rate_snapshot_id.as(
        "fx_rate_snapshot_id"
      ),
      l_agg_platform_video_analytics_hourly_pb_currency_advertiser_currency.as(
        "advertiser_currency"
      ),
      l_agg_platform_video_analytics_hourly_pb_currency_advertiser_exchange_rate
        .as("advertiser_exchange_rate"),
      l_agg_platform_video_analytics_hourly_pb_currency_publisher_currency.as(
        "publisher_currency"
      ),
      l_agg_platform_video_analytics_hourly_pb_currency_publisher_exchange_rate
        .as("publisher_exchange_rate"),
      l_agg_platform_video_analytics_hourly_pb_currency_advertiser_default_currency
        .as("advertiser_default_currency"),
      l_agg_platform_video_analytics_hourly_pb_currency_advertiser_default_exchange_rate
        .as("advertiser_default_exchange_rate"),
      l_agg_platform_video_analytics_hourly_pb_currency_member_currency.as(
        "member_currency"
      ),
      l_agg_platform_video_analytics_hourly_pb_currency_member_exchange_rate.as(
        "member_exchange_rate"
      ),
      l_agg_platform_video_analytics_hourly_pb_currency_billing_currency.as(
        "billing_currency"
      ),
      l_agg_platform_video_analytics_hourly_pb_currency_billing_exchange_rate
        .as("billing_exchange_rate")
    )
    l_agg_platform_video_analytics_hourly_pb_currency
  }

}
