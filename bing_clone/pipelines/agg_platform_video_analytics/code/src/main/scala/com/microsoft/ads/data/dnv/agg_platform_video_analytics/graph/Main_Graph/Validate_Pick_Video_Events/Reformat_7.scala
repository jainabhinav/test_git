package com.microsoft.ads.data.dnv.agg_platform_video_analytics.graph.Main_Graph.Validate_Pick_Video_Events

import io.prophecy.libs._
import com.microsoft.ads.data.dnv.agg_platform_video_analytics.udfs.PipelineInitCode._
import com.microsoft.ads.data.dnv.agg_platform_video_analytics.udfs.UDFs._
import com.microsoft.ads.data.dnv.agg_platform_video_analytics.udfs.ColumnFunctions._
import com.microsoft.ads.data.dnv.agg_platform_video_analytics.graph.Main_Graph.Validate_Pick_Video_Events.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Reformat_7 {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(
      when(to_json(col("rtb_imp_type")) === "{}", lit(null))
        .otherwise(col("rtb_imp_type"))
        .as("rtb_imp_type"),
      when(to_json(col("resold_imp_type")) === "{}", lit(null))
        .otherwise(col("resold_imp_type"))
        .as("resold_imp_type"),
      when(to_json(col("rtb_request_imp_type")) === "{}", lit(null))
        .otherwise(col("rtb_request_imp_type"))
        .as("rtb_request_imp_type"),
      when(to_json(col("resold_request_imp_type")) === "{}", lit(null))
        .otherwise(col("resold_request_imp_type"))
        .as("resold_request_imp_type"),
      when(to_json(col("others_imp_type")) === "{}", lit(null))
        .otherwise(col("others_imp_type"))
        .as("others_imp_type"),
      when(to_json(col("others_request_imp_type")) === "{}", lit(null))
        .otherwise(col("others_request_imp_type"))
        .as("others_request_imp_type")
    )

}
