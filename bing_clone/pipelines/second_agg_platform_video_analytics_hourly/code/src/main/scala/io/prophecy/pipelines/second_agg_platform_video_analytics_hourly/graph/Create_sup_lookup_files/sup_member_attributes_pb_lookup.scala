package io.prophecy.pipelines.second_agg_platform_video_analytics_hourly.graph.Create_sup_lookup_files

import io.prophecy.libs._
import io.prophecy.pipelines.second_agg_platform_video_analytics_hourly.graph.Create_sup_lookup_files.config.Context
import io.prophecy.pipelines.second_agg_platform_video_analytics_hourly.udfs.UDFs._
import io.prophecy.pipelines.second_agg_platform_video_analytics_hourly.udfs.ColumnFunctions._
import io.prophecy.pipelines.second_agg_platform_video_analytics_hourly.udfs.PipelineInitCode._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object sup_member_attributes_pb_lookup {

  def apply(context: Context, in: DataFrame): Unit =
    createLookup("sup_member_attributes_pb",
                 in,
                 context.spark,
                 List("id"),
                 "id",
                 "is_external_supply",
                 "seller_member_group_id"
    )

}
