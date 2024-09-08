package io.prophecy.pipelines.first_agg_platform_video_analytics.graph.Main_Graph.Validate_Pick_Clicks

import io.prophecy.libs._
import io.prophecy.pipelines.first_agg_platform_video_analytics.udfs.PipelineInitCode._
import io.prophecy.pipelines.first_agg_platform_video_analytics.udfs.UDFs._
import io.prophecy.pipelines.first_agg_platform_video_analytics.udfs.ColumnFunctions._
import io.prophecy.pipelines.first_agg_platform_video_analytics.graph.Main_Graph.Validate_Pick_Clicks.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Sort_1_3 {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.orderBy(col("auction_id_64").asc, col("imp_type").asc)

}
