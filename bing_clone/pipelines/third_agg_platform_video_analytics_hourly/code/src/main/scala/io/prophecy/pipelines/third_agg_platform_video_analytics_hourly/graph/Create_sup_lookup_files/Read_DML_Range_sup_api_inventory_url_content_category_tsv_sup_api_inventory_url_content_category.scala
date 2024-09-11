package io.prophecy.pipelines.third_agg_platform_video_analytics_hourly.graph.Create_sup_lookup_files

import io.prophecy.libs._
import io.prophecy.pipelines.third_agg_platform_video_analytics_hourly.graph.Create_sup_lookup_files.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Read_DML_Range_sup_api_inventory_url_content_category_tsv_sup_api_inventory_url_content_category {

  def apply(context: Context): DataFrame = {
    val Config = context.config
    context.spark.read
      .format("csv")
      .option("header", true)
      .option("sep",    "\t")
      .schema(
        StructType(
          Array(StructField("inventory_url_id",    StringType, true),
                StructField("content_category_id", StringType, true),
                StructField("parent_category_id",  StringType, true)
          )
        )
      )
      .load(
        s"${Config.datasets.inputs.sup_api_inventory_url_content_category}/${Config.system.dateSlash}/sup_api_inventory_url_content_category.tsv"
      )
  }

}
