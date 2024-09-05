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

object Rollup {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.groupBy(col("auction_id_64").cast(LongType).as("auction_id_64"))
      .agg(
        min(col("date_time").cast(LongType)).as("date_time"),
        when(min(col("user_id_64")) === lit(-1), lit(-1L))
          .otherwise(max(col("user_id_64")))
          .as("user_id_64")
          .as("user_id_64"),
        last(col("referrer_url")).as("referrer_url"),
        max(col("advertiser_currency")).as("advertiser_currency"),
        max(col("advertiser_exchange_rate")).as("advertiser_exchange_rate"),
        max(col("booked_revenue_dollars")).as("booked_revenue_dollars"),
        max(col("booked_revenue_adv_curr")).as("booked_revenue_adv_curr"),
        max(col("publisher_currency")).as("publisher_currency"),
        max(col("publisher_exchange_rate")).as("publisher_exchange_rate"),
        max(col("payment_value")).as("payment_value"),
        last(col("is_multi_click")).cast(IntegerType).as("is_multi_click"),
        last(col("ip_address")).as("ip_address"),
        first(col("anonymized_user_info")).as("anonymized_user_info"),
        last(col("revenue_info")).as("revenue_info"),
        last(col("use_revenue_info")).cast(BooleanType).as("use_revenue_info"),
        last(col("is_deferred")).cast(BooleanType).as("is_deferred")
      )

}
