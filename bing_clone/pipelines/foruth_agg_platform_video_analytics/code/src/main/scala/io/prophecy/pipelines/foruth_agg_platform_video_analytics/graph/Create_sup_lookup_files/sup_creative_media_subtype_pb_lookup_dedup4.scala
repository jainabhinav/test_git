package io.prophecy.pipelines.foruth_agg_platform_video_analytics.graph.Create_sup_lookup_files

import io.prophecy.libs._
import io.prophecy.pipelines.foruth_agg_platform_video_analytics.udfs.PipelineInitCode._
import io.prophecy.pipelines.foruth_agg_platform_video_analytics.udfs.UDFs._
import io.prophecy.pipelines.foruth_agg_platform_video_analytics.udfs.ColumnFunctions._
import io.prophecy.pipelines.foruth_agg_platform_video_analytics.graph.Create_sup_lookup_files.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object sup_creative_media_subtype_pb_lookup_dedup4 {

  def apply(context: Context, in: DataFrame): DataFrame = {
    import org.apache.spark.sql.expressions.Window
    in.dropDuplicates(List("id"))
  }

}
