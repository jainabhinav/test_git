package io.prophecy.pipelines.first_agg_platform_video_analytics.graph.Main_Graph

import io.prophecy.libs._
import io.prophecy.pipelines.first_agg_platform_video_analytics.udfs.PipelineInitCode._
import io.prophecy.pipelines.first_agg_platform_video_analytics.graph.Main_Graph.xr_partition_key_filter_checkpointed_sort_agg_dw_pixels.config._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._
package object xr_partition_key_filter_checkpointed_sort_agg_dw_pixels {

  def apply(context: Context, in: DataFrame): DataFrame = {
    val df_repartition_dataframe_2 = repartition_dataframe_2(context, in)
    val df_Filter_by_Expression_2 =
      Filter_by_Expression_2(context, df_repartition_dataframe_2)
    df_Filter_by_Expression_2
  }

}
