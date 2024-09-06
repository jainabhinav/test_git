package com.microsoft.ads.data.dnv.test_agg_platform_video_analytics_hourly.graph.Create_sup_lookup_files

import io.prophecy.libs._
import com.microsoft.ads.data.dnv.test_agg_platform_video_analytics_hourly.udfs.PipelineInitCode._
import com.microsoft.ads.data.dnv.test_agg_platform_video_analytics_hourly.udfs.UDFs._
import com.microsoft.ads.data.dnv.test_agg_platform_video_analytics_hourly.udfs.ColumnFunctions._
import com.microsoft.ads.data.dnv.test_agg_platform_video_analytics_hourly.graph.Create_sup_lookup_files.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Dedup_Sorted_sup_code_fx_rate {

  def apply(context: Context, in: DataFrame): DataFrame = {
    import org.apache.spark.sql.expressions.Window
    in.withColumn(
        "row_number",
        row_number().over(
          Window.partitionBy("code").orderBy(col("fx_rate_snapshot_id").asc)
        )
      )
      .withColumn("count", count("*").over(Window.partitionBy("code")))
      .filter(col("row_number") === col("count"))
      .drop("row_number")
      .drop("count")
  }

}
