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

object Rollup_To_Dedup_Reformat {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(
      col("date_time").cast(LongType).as("date_time"),
      col("auction_id_64").cast(LongType).as("auction_id_64"),
      col("user_id_64").cast(LongType).as("user_id_64"),
      col("tracker_id").cast(IntegerType).as("tracker_id"),
      col("member_id").cast(IntegerType).as("member_id"),
      col("advertiser_id").cast(IntegerType).as("advertiser_id"),
      col("campaign_group_id").cast(IntegerType).as("campaign_group_id"),
      col("can_convert").cast(IntegerType).as("can_convert"),
      col("insertion_order_id").cast(IntegerType).as("insertion_order_id"),
      col("revenue_value"),
      col("revenue_value_adv_curr"),
      col("commission_cpm"),
      col("commission_revshare"),
      col("pricing_type"),
      col("advertiser_currency"),
      col("advertiser_exchange_rate"),
      col("media_buy_cost"),
      col("media_buy_rev_share_pct"),
      col("revenue_type").cast(IntegerType).as("revenue_type"),
      col("payment_type").cast(IntegerType).as("payment_type"),
      col("revenue_info")
    )

}
