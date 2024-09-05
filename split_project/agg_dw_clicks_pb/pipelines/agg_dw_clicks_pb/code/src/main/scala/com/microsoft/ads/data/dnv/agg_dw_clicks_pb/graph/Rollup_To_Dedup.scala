package com.microsoft.ads.data.dnv.agg_dw_clicks_pb.graph

import io.prophecy.libs._
import com.microsoft.ads.data.dnv.agg_dw_clicks_pb.udfs.PipelineInitCode._
import com.microsoft.ads.data.dnv.agg_dw_clicks_pb.udfs.UDFs._
import com.microsoft.ads.data.dnv.agg_dw_clicks_pb.udfs.ColumnFunctions._
import com.microsoft.ads.data.dnv.agg_dw_clicks_pb.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Rollup_To_Dedup {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.groupBy(col("auction_id_64").cast(LongType).as("auction_id_64"))
      .agg(
        min(col("date_time").cast(LongType)).cast(LongType).as("date_time"),
        last(col("user_id_64")).cast(LongType).as("user_id_64"),
        last(col("tracker_id")).cast(IntegerType).as("tracker_id"),
        last(col("member_id")).cast(IntegerType).as("member_id"),
        min(col("advertiser_id").cast(IntegerType))
          .cast(IntegerType)
          .as("advertiser_id"),
        min(col("campaign_group_id").cast(IntegerType))
          .cast(IntegerType)
          .as("campaign_group_id"),
        last(col("can_convert")).cast(IntegerType).as("can_convert"),
        max(col("insertion_order_id").cast(IntegerType))
          .cast(IntegerType)
          .as("insertion_order_id"),
        max(col("revenue_value")).cast(DoubleType).as("revenue_value"),
        max(col("revenue_value_adv_curr"))
          .cast(DoubleType)
          .as("revenue_value_adv_curr"),
        max(col("commission_cpm")).cast(DoubleType).as("commission_cpm"),
        max(col("commission_revshare"))
          .cast(DoubleType)
          .as("commission_revshare"),
        max(col("pricing_type")).as("pricing_type"),
        max(col("advertiser_currency")).as("advertiser_currency"),
        max(col("advertiser_exchange_rate"))
          .cast(DoubleType)
          .as("advertiser_exchange_rate"),
        max(col("media_buy_cost")).cast(DoubleType).as("media_buy_cost"),
        max(col("media_buy_rev_share_pct"))
          .cast(DoubleType)
          .as("media_buy_rev_share_pct"),
        max(col("revenue_type").cast(IntegerType))
          .cast(IntegerType)
          .as("revenue_type"),
        max(col("payment_type").cast(IntegerType))
          .cast(IntegerType)
          .as("payment_type"),
        last(col("revenue_info")).as("revenue_info")
      )

}
