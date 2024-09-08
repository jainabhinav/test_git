package io.prophecy.pipelines.third_agg_platform_video_analytics.graph.Main_Graph.Validate_Pick_Video_Events

import io.prophecy.libs._
import io.prophecy.pipelines.third_agg_platform_video_analytics.udfs.PipelineInitCode._
import io.prophecy.pipelines.third_agg_platform_video_analytics.udfs.UDFs._
import io.prophecy.pipelines.third_agg_platform_video_analytics.udfs.ColumnFunctions._
import io.prophecy.pipelines.third_agg_platform_video_analytics.graph.Main_Graph.Validate_Pick_Video_Events.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Reformat_1_1 {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(
      coalesce(
        when(is_not_null(col("rtb_imp_type"))
               .and(is_not_null(col("resold_imp_type"))),
             col("rtb_imp_type")
        ),
        when(
          is_not_null(col("rtb_imp_type")).and(col("resold_imp_type").isNull),
          col("rtb_imp_type")
        ),
        when(
          col("rtb_imp_type").isNull.and(is_not_null(col("resold_imp_type"))),
          col("resold_imp_type")
        ),
        col("others_imp_type"),
        when(is_not_null(col("rtb_request_imp_type"))
               .and(is_not_null(col("resold_request_imp_type"))),
             col("rtb_request_imp_type")
        ),
        when(is_not_null(col("rtb_request_imp_type"))
               .and(col("resold_request_imp_type").isNull),
             col("rtb_request_imp_type")
        ),
        when(col("rtb_request_imp_type").isNull
               .and(is_not_null(col("resold_request_imp_type"))),
             col("resold_request_imp_type")
        ),
        col("others_request_imp_type")
      ).as("unflattened_struct_out")
    )

}
