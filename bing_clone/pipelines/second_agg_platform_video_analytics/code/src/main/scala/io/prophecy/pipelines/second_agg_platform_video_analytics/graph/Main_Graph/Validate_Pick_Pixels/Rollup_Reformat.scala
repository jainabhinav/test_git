package io.prophecy.pipelines.second_agg_platform_video_analytics.graph.Main_Graph.Validate_Pick_Pixels

import io.prophecy.libs._
import io.prophecy.pipelines.second_agg_platform_video_analytics.udfs.PipelineInitCode._
import io.prophecy.pipelines.second_agg_platform_video_analytics.udfs.UDFs._
import io.prophecy.pipelines.second_agg_platform_video_analytics.udfs.ColumnFunctions._
import io.prophecy.pipelines.second_agg_platform_video_analytics.graph.Main_Graph.Validate_Pick_Pixels.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Rollup_Reformat {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(col("rtb"), col("resold"), col("others"))

}
