package io.prophecy.pipelines.foruth_agg_platform_video_analytics.graph

import io.prophecy.libs._
import io.prophecy.pipelines.foruth_agg_platform_video_analytics.config.Context
import io.prophecy.pipelines.foruth_agg_platform_video_analytics.udfs.UDFs._
import io.prophecy.pipelines.foruth_agg_platform_video_analytics.udfs.ColumnFunctions._
import io.prophecy.pipelines.foruth_agg_platform_video_analytics.udfs.PipelineInitCode._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object join_and_lookup_creatives {
  def apply(context: Context, in0: DataFrame, in1: DataFrame): DataFrame = {
    val spark = context.spark
    val Config = context.config
    println("#####Step name: join by id#####")
    println("step start time: " + Instant.now().atZone(ZoneId.of("America/Chicago"))) 
    
    def addLookupStruct(alias: String): Column = {
        when(
          is_not_null(col(s"$alias.id")),
          struct(
            col(s"$alias.id").as("id"),
            col(s"$alias.media_subtype").as("media_subtype"),
            col(s"$alias.advertiser_id").as("advertiser_id"),
            col(s"$alias.duration_ms").as("duration_ms"),
            col(s"$alias.vast_type_id").as("vast_type_id"),
            col(s"$alias.minimum_vast_version_id").as("minimum_vast_version_id"),
            col(s"$alias.is_skippable").as("is_skippable"),
            col(s"$alias.framework_ids").as("framework_ids")
          )
        )
      }
    
    import org.apache.spark.storage.StorageLevel
    
    // Persist the DataFrame to disk only
    // val new_in0 = in0.repartition(12000).persist(StorageLevel.DISK_ONLY)
    
    // val rep_count = 600
    val new_in1 = in1.repartition(col("id")).persist(StorageLevel.MEMORY_AND_DISK)
    
    println("#####Step name: join by id#####")
    println("step persist time: " + Instant.now().atZone(ZoneId.of("America/Chicago"))) 
    
    // Perform the join once for in1 and create multiple lookup columns
    val joinedDF = in0.as("in0")
      .join(new_in1.as("in1"), col("in1.id") === col("in0.agg_dw_clicks_creative_id"), "left_outer")
      .join(new_in1.as("in2"), col("in2.id") === col("in0.agg_platform_video_requests_creative_id"), "left_outer")
      .join(new_in1.as("in3"), col("in3.id") === col("in0.agg_impbus_clicks_creative_id"), "left_outer")
      .join(new_in1.as("in4"), col("in4.id") === col("in0.agg_platform_video_impressions_creative_id"), "left_outer")
      .join(new_in1.as("in5"), col("in5.id") === col("in0.agg_dw_video_events_creative_id"), "left_outer")
      .join(new_in1.as("in6"), col("in6.id") === col("in0.agg_dw_pixels_creative_id"), "left_outer")
      .join(new_in1.as("in7"), col("in7.id") === col("in0.f_calc_derived_fields"), "left_outer")
    
    // Create the respective lookup columns
    val out0 = joinedDF
      .select(
        col("in0.*"),  // Select all columns from in0
        addLookupStruct("in2").as("_sup_creative_media_subtype_pb_LOOKUP1"),  // Include the newly generated columns
        addLookupStruct("in5").as("_sup_creative_media_subtype_pb_LOOKUP2"),
        addLookupStruct("in4").as("_sup_creative_media_subtype_pb_LOOKUP3"),
        addLookupStruct("in1").as("_sup_creative_media_subtype_pb_LOOKUP4"),
        addLookupStruct("in6").as("_sup_creative_media_subtype_pb_LOOKUP5"),
        addLookupStruct("in3").as("_sup_creative_media_subtype_pb_LOOKUP6"),
        addLookupStruct("in7").as("_sup_creative_media_subtype_pb_LOOKUP"),
        col("_sup_placement_video_attributes_pb_LOOKUP")
      )
    
    println("#####Step name: join by id#####")
    println("step end time: " + Instant.now().atZone(ZoneId.of("America/Chicago"))) 
    out0
  }

}