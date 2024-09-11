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

object Filter_valid_pods {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.filter(is_not_null(col("pod_id_64")).and(col("pod_id_64") > lit(0)))

}
