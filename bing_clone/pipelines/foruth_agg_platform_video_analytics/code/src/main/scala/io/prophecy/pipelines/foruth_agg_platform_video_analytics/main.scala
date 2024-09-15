package io.prophecy.pipelines.foruth_agg_platform_video_analytics

import io.prophecy.libs._
import io.prophecy.pipelines.foruth_agg_platform_video_analytics.config._
import io.prophecy.pipelines.foruth_agg_platform_video_analytics.udfs.UDFs._
import io.prophecy.pipelines.foruth_agg_platform_video_analytics.udfs.ColumnFunctions._
import io.prophecy.pipelines.foruth_agg_platform_video_analytics.udfs.PipelineInitCode._
import io.prophecy.pipelines.foruth_agg_platform_video_analytics.graph._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Main {

  def apply(context: Context): Unit = {
    Script_1(context)
    val df_temp_output2 = temp_output2(context)
    val df_repartition_by_auction_id_1_1 =
      repartition_by_auction_id_1_1(context, df_temp_output2)
    val (df_Create_sup_lookup_files_out2, df_Create_sup_lookup_files_out1) =
      Create_sup_lookup_files.apply(
        Create_sup_lookup_files.config
          .Context(context.spark, context.config.Create_sup_lookup_files)
      )
    val df_repartition_by_id =
      repartition_by_id(context, df_Create_sup_lookup_files_out2)
    val df_left_outer_join_video_attributes =
      left_outer_join_video_attributes(context,
                                       df_repartition_by_auction_id_1_1,
                                       df_repartition_by_id
      ).cache()
    val df_join_and_lookup_creatives = join_and_lookup_creatives(
      context,
      df_left_outer_join_video_attributes,
      df_Create_sup_lookup_files_out1
    )
    temp_output3(context, df_join_and_lookup_creatives)
  }

  def main(args: Array[String]): Unit = {
    val config = ConfigurationFactoryImpl.getConfig(args)
    val spark: SparkSession = SparkSession
      .builder()
      .appName("Prophecy Pipeline")
      .config("spark.default.parallelism",             "4")
      .config("spark.sql.legacy.allowUntypedScalaUDF", "true")
      .enableHiveSupport()
      .getOrCreate()
    val context = Context(spark, config)
    spark.conf.set("prophecy.metadata.pipeline.uri",
                   "pipelines/foruth_agg_platform_video_analytics"
    )
    spark.conf.set("spark.sql.adaptive.enabled",                   "true")
    spark.conf.set("park.sql.adaptive.coalescePartitions.enabled", "true")
    spark.conf
      .set("spark.sql.adaptive.coalescePartitions.minPartitionSize", "256MB")
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled",            "true")
    spark.conf.set("spark.sql.adaptive.join.enabled",                "true")
    spark.conf.set(
      "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes",
      "256MB"
    )
    registerUDFs(spark)
    MetricsCollector.instrument(spark,
                                "pipelines/foruth_agg_platform_video_analytics"
    ) {
      apply(context)
    }
  }

}
