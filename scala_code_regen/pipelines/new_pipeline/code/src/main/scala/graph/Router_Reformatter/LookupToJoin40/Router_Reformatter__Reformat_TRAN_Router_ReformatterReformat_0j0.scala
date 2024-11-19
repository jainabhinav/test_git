package graph.Router_Reformatter.LookupToJoin40

import io.prophecy.libs._
import udfs.PipelineInitCode._
import udfs.UDFs._
import udfs.ColumnFunctions._
import graph.Router_Reformatter.LookupToJoin40.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Router_Reformatter__Reformat_TRAN_Router_ReformatterReformat_0j0 {

  def apply(
    context: Context,
    in0:     DataFrame,
    in1:     DataFrame,
    in2:     DataFrame,
    in3:     DataFrame
  ): DataFrame =
    in0
      .as("in0")
      .join(in1.as("in1"),
            col("in0.log_impbus_preempt.curated_deal_id").cast(
              IntegerType
            ) === col("in1.id"),
            "left_outer"
      )
      .join(in2.as("in2"),
            col("in0.log_impbus_impressions.tag_id").cast(IntegerType) === col(
              "in2.id"
            ),
            "left_outer"
      )
      .join(in3.as("in3"),
            col("in0.log_impbus_preempt.deal_id").cast(IntegerType) === col(
              "in3.id"
            ),
            "left_outer"
      )
      .select(
        when(is_not_null(col("in1.id")),
             struct(col("in1.id").as("id"),
                    col("in1.member_id").as("member_id"),
                    col("in1.deal_type_id").as("deal_type_id")
             )
        ).as("_sup_common_deal_LOOKUP"),
        when(
          is_not_null(col("in2.id")),
          struct(
            col("in2.id").as("id"),
            col("in2.supports_skippable").as("supports_skippable"),
            col("in2.max_duration_secs").as("max_duration_secs"),
            col("in2.max_ad_duration_secs").as("max_ad_duration_secs"),
            col("in2.maximum_number_ads").as("maximum_number_ads"),
            col("in2.start_delay_secs").as("start_delay_secs"),
            col("in2.playback_method").as("playback_method"),
            col("in2.video_context").as("video_context"),
            col("in2.is_mediated").as("is_mediated"),
            col("in2.skip_offset").as("skip_offset")
          )
        ).as("_sup_placement_video_attributes_pb_LOOKUP"),
        when(is_not_null(col("in3.id")),
             struct(col("in3.id").as("id"),
                    col("in3.member_id").as("member_id"),
                    col("in3.deal_type_id").as("deal_type_id")
             )
        ).as("_sup_common_deal_LOOKUP1"),
        col("in0.*")
      )

}
