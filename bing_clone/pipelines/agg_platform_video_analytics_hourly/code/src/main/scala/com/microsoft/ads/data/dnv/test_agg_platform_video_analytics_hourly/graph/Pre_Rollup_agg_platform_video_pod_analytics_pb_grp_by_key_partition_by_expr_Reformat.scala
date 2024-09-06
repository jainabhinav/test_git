package com.microsoft.ads.data.dnv.test_agg_platform_video_analytics_hourly.graph

import io.prophecy.libs._
import com.microsoft.ads.data.dnv.test_agg_platform_video_analytics_hourly.udfs.PipelineInitCode._
import com.microsoft.ads.data.dnv.test_agg_platform_video_analytics_hourly.udfs.UDFs._
import com.microsoft.ads.data.dnv.test_agg_platform_video_analytics_hourly.udfs.ColumnFunctions._
import com.microsoft.ads.data.dnv.test_agg_platform_video_analytics_hourly.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Pre_Rollup_agg_platform_video_pod_analytics_pb_grp_by_key_partition_by_expr_Reformat {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(col("key"),
              col("value"),
              col("pod_id_64").cast(LongType).as("pod_id_64"),
              col("pod_id_64_vector")
    )

}
