package com.microsoft.ads.data.dnv.agg_platform_video_analytics.graph.Create_sup_lookup_files

import io.prophecy.libs._
import com.microsoft.ads.data.dnv.agg_platform_video_analytics.graph.Create_sup_lookup_files.config.Context
import com.microsoft.ads.data.dnv.agg_platform_video_analytics.udfs.UDFs._
import com.microsoft.ads.data.dnv.agg_platform_video_analytics.udfs.ColumnFunctions._
import com.microsoft.ads.data.dnv.agg_platform_video_analytics.udfs.PipelineInitCode._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object sup_bidder_fx_rates_lookup {

  def apply(context: Context, in: DataFrame): Unit =
    createLookup("sup_bidder_fx_rates",
                 in,
                 context.spark,
                 List("fx_rate_snapshot_id", "code"),
                 "fx_rate_snapshot_id",
                 "currency_id",
                 "code",
                 "rate",
                 "as_of_timestamp"
    )

}
