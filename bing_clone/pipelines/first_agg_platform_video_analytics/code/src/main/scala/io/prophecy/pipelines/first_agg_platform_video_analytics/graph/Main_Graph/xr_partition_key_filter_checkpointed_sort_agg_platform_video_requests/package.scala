package io.prophecy.pipelines.first_agg_platform_video_analytics.graph.Main_Graph

import io.prophecy.libs._
import io.prophecy.pipelines.first_agg_platform_video_analytics.udfs.PipelineInitCode._
import io.prophecy.pipelines.first_agg_platform_video_analytics.graph.Main_Graph.xr_partition_key_filter_checkpointed_sort_agg_platform_video_requests.config._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._
package object xr_partition_key_filter_checkpointed_sort_agg_platform_video_requests {

  def apply(context: Context, in1: DataFrame, in: DataFrame): DataFrame = {
    val df_Partition_by_Key_UnionAll =
      Partition_by_Key_UnionAll(context, in, in1)
    val df_Repartition_1 = Repartition_1(context, df_Partition_by_Key_UnionAll)
    val df_Filter_by_Expression_3 =
      Filter_by_Expression_3(context, df_Repartition_1)
    df_Filter_by_Expression_3
  }

}
