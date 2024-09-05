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

object Rollup_Reformat {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(
      col("date_time").cast(LongType).as("date_time"),
      col("auction_id_64").cast(LongType).as("auction_id_64"),
      col("user_id_64").cast(LongType).as("user_id_64"),
      col("referrer_url"),
      col("advertiser_currency"),
      col("advertiser_exchange_rate"),
      col("booked_revenue_dollars"),
      col("booked_revenue_adv_curr"),
      col("publisher_currency"),
      col("publisher_exchange_rate"),
      col("payment_value"),
      col("is_multi_click").cast(IntegerType).as("is_multi_click"),
      col("ip_address"),
      col("anonymized_user_info"),
      col("revenue_info"),
      col("use_revenue_info"),
      col("is_deferred")
    )

}
