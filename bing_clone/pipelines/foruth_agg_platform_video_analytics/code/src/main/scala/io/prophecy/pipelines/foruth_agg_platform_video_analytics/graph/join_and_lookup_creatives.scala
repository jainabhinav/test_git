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
    
    // Repartition in1 to distribute data evenly
    val numPartitions = 1000
    val new_in1 = in1.repartition(numPartitions, col("id"))
    
    // Persist new_in1 since it will be reused multiple times
    new_in1.persist(StorageLevel.MEMORY_AND_DISK_SER)
    
    println("#####Step name: join by id#####")
    println("step persist time: " + Instant.now().atZone(ZoneId.of("America/Chicago"))) 
    
    // Perform the first join and persist the intermediate result
    val join1 = in0.as("in0")
      .join(new_in1.as("in1").hint("shuffle_hash"), col("in1.id") === col("in0.agg_dw_clicks_creative_id"), "left_outer")
      .persist(StorageLevel.DISK_ONLY)
    join1.count()  // Materialize the DataFrame
    
    // Unpersist in0 as it's no longer needed
    // Since in0 was not persisted, no need to unpersist it
    
    // Proceed with subsequent joins, each time unpersisting the previous DataFrame
    val join2 = join1
      .join(new_in1.as("in2").hint("shuffle_hash"), col("in2.id") === col("in0.agg_platform_video_requests_creative_id"), "left_outer")
      .persist(StorageLevel.DISK_ONLY)
    join2.count()
    
    // Unpersist join1 to free up resources
    join1.unpersist()
    
    val join3 = join2
      .join(new_in1.as("in3").hint("shuffle_hash"), col("in3.id") === col("in0.agg_impbus_clicks_creative_id"), "left_outer")
      .persist(StorageLevel.DISK_ONLY)
    join3.count()
    
    // Unpersist join2
    join2.unpersist()
    
    val join4 = join3
      .join(new_in1.as("in4").hint("shuffle_hash"), col("in4.id") === col("in0.agg_platform_video_impressions_creative_id"), "left_outer")
      .persist(StorageLevel.DISK_ONLY)
    join4.count()
    
    // Unpersist join3
    join3.unpersist()
    
    val join5 = join4
      .join(new_in1.as("in5").hint("shuffle_hash"), col("in5.id") === col("in0.agg_dw_video_events_creative_id"), "left_outer")
      .persist(StorageLevel.DISK_ONLY)
    join5.count()
    
    // Unpersist join4
    join4.unpersist()
    
    val join6 = join5
      .join(new_in1.as("in6").hint("shuffle_hash"), col("in6.id") === col("in0.agg_dw_pixels_creative_id"), "left_outer")
      .persist(StorageLevel.DISK_ONLY)
    join6.count()
    
    // Unpersist join5
    join5.unpersist()
    
    val join7 = join6
      .join(new_in1.as("in7").hint("shuffle_hash"), col("in7.id") === col("in0.f_calc_derived_fields"), "left_outer")
      .persist(StorageLevel.DISK_ONLY)
    join7.count()
    
    // Unpersist join6
    join6.unpersist()
    
    // Create the respective lookup columns
    val out0 = join7
      .select(
        col("in0.*"),  // Select all columns from in0
        addLookupStruct("in2").as("_sup_creative_media_subtype_pb_LOOKUP1"),
        addLookupStruct("in5").as("_sup_creative_media_subtype_pb_LOOKUP2"),
        addLookupStruct("in4").as("_sup_creative_media_subtype_pb_LOOKUP3"),
        addLookupStruct("in1").as("_sup_creative_media_subtype_pb_LOOKUP4"),
        addLookupStruct("in6").as("_sup_creative_media_subtype_pb_LOOKUP5"),
        addLookupStruct("in3").as("_sup_creative_media_subtype_pb_LOOKUP6"),
        addLookupStruct("in7").as("_sup_creative_media_subtype_pb_LOOKUP"),
        col("_sup_placement_video_attributes_pb_LOOKUP")
      ).persist(StorageLevel.DISK_ONLY)
    
    out0.count()
    // Unpersist join7 and new_in1 as they are no longer needed
    join7.unpersist()
    new_in1.unpersist()
    
    println("#####Step name: join by id#####")
    println("step end time: " + Instant.now().atZone(ZoneId.of("America/Chicago"))) 
    out0
  }

}
