package com.microsoft.ads.data.dnv.test_agg_platform_video_analytics_hourly.graph.Create_sup_lookup_files

import io.prophecy.libs._
import com.microsoft.ads.data.dnv.test_agg_platform_video_analytics_hourly.graph.Create_sup_lookup_files.config.Context
import com.microsoft.ads.data.dnv.test_agg_platform_video_analytics_hourly.udfs.UDFs._
import com.microsoft.ads.data.dnv.test_agg_platform_video_analytics_hourly.udfs.ColumnFunctions._
import com.microsoft.ads.data.dnv.test_agg_platform_video_analytics_hourly.udfs.PipelineInitCode._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object create_fx_rate_lookup {

  def apply(context: Context, in0: DataFrame): Unit =
    createLookup("sup_bidder_fx_rates",
                 in0,
                 context.spark,
                 List("fx_rate_snapshot_id", "code"),
                 "fx_rate_snapshot_id",
                 "currency_id",
                 "code",
                 "rate",
                 "as_of_timestamp"
    )

}
