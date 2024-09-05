package com.microsoft.ads.data.dnv.agg_dw_clicks_pb.graph.External_Clicks

import io.prophecy.libs._
import com.microsoft.ads.data.dnv.agg_dw_clicks_pb.udfs.PipelineInitCode._
import com.microsoft.ads.data.dnv.agg_dw_clicks_pb.udfs.UDFs._
import com.microsoft.ads.data.dnv.agg_dw_clicks_pb.udfs.ColumnFunctions._
import com.microsoft.ads.data.dnv.agg_dw_clicks_pb.graph.External_Clicks.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Filter_by_Expression_aggImpbusClicksPb {

  def apply(context: Context, in: DataFrame): DataFrame = {
    val Config = context.config
    in.filter(
      (col("date_time") >= unix_timestamp(lit(Config.system.startDate),
                                          "yyyy-MM-dd HH:mm:ss"
      )).and(
        col("date_time") < unix_timestamp(
          date_format(lit(Config.system.startDate), "yyyy-MM-dd HH:mm:ss")
        ) + lit(3600)
      )
    )
  }

}
