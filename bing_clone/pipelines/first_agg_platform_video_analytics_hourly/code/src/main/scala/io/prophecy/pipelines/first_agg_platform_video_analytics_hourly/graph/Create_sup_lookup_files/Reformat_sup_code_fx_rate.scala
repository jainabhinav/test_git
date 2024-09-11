package io.prophecy.pipelines.first_agg_platform_video_analytics_hourly.graph.Create_sup_lookup_files

import io.prophecy.libs._
import io.prophecy.pipelines.first_agg_platform_video_analytics_hourly.udfs.PipelineInitCode._
import io.prophecy.pipelines.first_agg_platform_video_analytics_hourly.udfs.UDFs._
import io.prophecy.pipelines.first_agg_platform_video_analytics_hourly.udfs.ColumnFunctions._
import io.prophecy.pipelines.first_agg_platform_video_analytics_hourly.graph.Create_sup_lookup_files.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Reformat_sup_code_fx_rate {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(
      col("fx_rate_snapshot_id").cast(IntegerType).as("fx_rate_snapshot_id"),
      col("currency_id").cast(IntegerType).as("currency_id"),
      col("code"),
      col("rate").cast(DoubleType).as("rate"),
      col("as_of_timestamp").cast(LongType).as("as_of_timestamp")
    )

}
