package io.prophecy.pipelines.second_agg_platform_video_analytics_hourly.graph

import io.prophecy.libs._
import io.prophecy.pipelines.second_agg_platform_video_analytics_hourly.udfs.PipelineInitCode._
import io.prophecy.pipelines.second_agg_platform_video_analytics_hourly.udfs.UDFs._
import io.prophecy.pipelines.second_agg_platform_video_analytics_hourly.udfs.ColumnFunctions._
import io.prophecy.pipelines.second_agg_platform_video_analytics_hourly.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object repartition_by_auction_id_1 {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.repartition(3200.toInt, col("auction_id_64"))

}
