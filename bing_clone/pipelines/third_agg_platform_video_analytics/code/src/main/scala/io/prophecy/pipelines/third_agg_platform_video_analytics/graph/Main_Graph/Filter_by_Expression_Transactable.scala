package io.prophecy.pipelines.third_agg_platform_video_analytics.graph.Main_Graph

import io.prophecy.libs._
import io.prophecy.pipelines.third_agg_platform_video_analytics.udfs.PipelineInitCode._
import io.prophecy.pipelines.third_agg_platform_video_analytics.udfs.UDFs._
import io.prophecy.pipelines.third_agg_platform_video_analytics.udfs.ColumnFunctions._
import io.prophecy.pipelines.third_agg_platform_video_analytics.graph.Main_Graph.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Filter_by_Expression_Transactable {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.filter(
      when(
        (coalesce(col("num_of_bids").cast(IntegerType), lit(0)) === lit(0))
          .or(coalesce(col("media_type").cast(IntegerType), lit(0)) <= lit(1)),
        lit(0)
      ).otherwise(lit(1)) === lit(1)
    )

}
