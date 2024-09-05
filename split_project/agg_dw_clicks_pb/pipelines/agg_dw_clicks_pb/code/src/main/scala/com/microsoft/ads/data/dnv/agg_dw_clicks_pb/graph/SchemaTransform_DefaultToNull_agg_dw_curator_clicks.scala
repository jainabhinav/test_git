package com.microsoft.ads.data.dnv.agg_dw_clicks_pb.graph

import io.prophecy.libs._
import com.microsoft.ads.data.dnv.agg_dw_clicks_pb.udfs.PipelineInitCode._
import com.microsoft.ads.data.dnv.agg_dw_clicks_pb.udfs.UDFs._
import com.microsoft.ads.data.dnv.agg_dw_clicks_pb.udfs.ColumnFunctions._
import com.microsoft.ads.data.dnv.agg_dw_clicks_pb.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object SchemaTransform_DefaultToNull_agg_dw_curator_clicks {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.withColumn("video_context",
                  when(col("video_context") === lit(0),
                       lit(null).cast(IntegerType)
                  ).otherwise(col("video_context"))
      )
      .withColumn("mobile_app_instance_id",
                  when(col("mobile_app_instance_id") === lit(0),
                       lit(null).cast(IntegerType)
                  ).otherwise(col("mobile_app_instance_id"))
      )
      .withColumn("postal_code",
                  when(col("postal_code") === lit("---"),
                       lit(null).cast(StringType)
                  ).otherwise(col("postal_code"))
      )
      .withColumn("dma",
                  when(col("dma") === lit(0), lit(null).cast(IntegerType))
                    .otherwise(col("dma"))
      )
      .withColumn("geo_region",
                  when(col("geo_region") === lit("--"),
                       lit(null).cast(StringType)
                  ).otherwise(col("geo_region"))
      )

}
