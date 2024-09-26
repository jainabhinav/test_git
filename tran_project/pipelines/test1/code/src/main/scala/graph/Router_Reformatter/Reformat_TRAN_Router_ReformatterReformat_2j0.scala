package graph.Router_Reformatter

import io.prophecy.libs._
import udfs.PipelineInitCode._
import udfs.UDFs._
import udfs.ColumnFunctions._
import graph.Router_Reformatter.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Reformat_TRAN_Router_ReformatterReformat_2j0 {

  def apply(
    context: Context,
    in0:     DataFrame,
    in1:     DataFrame,
    in2:     DataFrame
  ): DataFrame =
    in0
      .as("in0")
      .join(in1.as("in1"),
            col("in0.log_impbus_impressions.tag_id").cast(IntegerType) === col(
              "in1.id"
            ),
            "left_outer"
      )
      .join(in2.as("in2"),
            col("in0.log_impbus_impressions.inventory_url_id")
              .cast(IntegerType) === col("in2.inventory_url_id"),
            "left_outer"
      )
      .select(
        when(
          is_not_null(col("in1.id")),
          struct(
            col("in1.id").as("id"),
            col("in1.supports_skippable").as("supports_skippable"),
            col("in1.max_duration_secs").as("max_duration_secs"),
            col("in1.max_ad_duration_secs").as("max_ad_duration_secs"),
            col("in1.maximum_number_ads").as("maximum_number_ads"),
            col("in1.start_delay_secs").as("start_delay_secs"),
            col("in1.playback_method").as("playback_method"),
            col("in1.video_context").as("video_context"),
            col("in1.is_mediated").as("is_mediated"),
            col("in1.skip_offset").as("skip_offset")
          )
        ).as("_sup_placement_video_attributes_pb_LOOKUP"),
        when(is_not_null(col("in2.inventory_url_id")),
             struct(col("in2.inventory_url_id").as("inventory_url_id"),
                    col("in2.inventory_url").as("inventory_url")
             )
        ).as("_inventory_url_by_id_LOOKUP"),
        col("in0.*")
      )

}
