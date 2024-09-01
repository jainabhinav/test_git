package com.microsoft.ads.data.dnv.agg_platform_video_analytics.graph.Main_Graph

import io.prophecy.libs._
import com.microsoft.ads.data.dnv.agg_platform_video_analytics.udfs.PipelineInitCode._
import com.microsoft.ads.data.dnv.agg_platform_video_analytics.udfs.UDFs._
import com.microsoft.ads.data.dnv.agg_platform_video_analytics.udfs.ColumnFunctions._
import com.microsoft.ads.data.dnv.agg_platform_video_analytics.graph.Main_Graph.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Filter_1 {

  def apply(context: Context, in: DataFrame): DataFrame = {
    val Config = context.config
    in.filter(
      when(
        (col("agg_platform_video_requests").getField(
          "date_time"
        ) < unix_timestamp(
          date_format(to_timestamp(concat(lit(Config.XR_BUSINESS_DATE),
                                          lit(Config.XR_BUSINESS_HOUR)
                                   ),
                                   "yyyyMMddHH"
                      ),
                      "yyyy-MM-dd HH:mm:ss"
          )
        )).and(
            coalesce(
              col("agg_platform_video_impressions").getField("date_time"),
              lit(0)
            ) < unix_timestamp(
              date_format(to_timestamp(concat(lit(Config.XR_BUSINESS_DATE),
                                              lit(Config.XR_BUSINESS_HOUR)
                                       ),
                                       "yyyyMMddHH"
                          ),
                          "yyyy-MM-dd HH:mm:ss"
              )
            )
          )
          .and(col("agg_dw_video_events").isNull)
          .and(col("agg_dw_clicks").isNull)
          .and(col("agg_dw_pixels").isNull)
          .and(col("agg_impbus_clicks").isNull),
        lit(0).cast(BooleanType)
      ).otherwise(lit(1).cast(BooleanType))
    )
  }

}
