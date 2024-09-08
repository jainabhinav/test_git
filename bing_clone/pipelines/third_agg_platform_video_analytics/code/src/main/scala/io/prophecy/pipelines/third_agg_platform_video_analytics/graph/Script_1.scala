package io.prophecy.pipelines.third_agg_platform_video_analytics.graph

import io.prophecy.libs._
import io.prophecy.pipelines.third_agg_platform_video_analytics.config.Context
import io.prophecy.pipelines.third_agg_platform_video_analytics.udfs.UDFs._
import io.prophecy.pipelines.third_agg_platform_video_analytics.udfs.ColumnFunctions._
import io.prophecy.pipelines.third_agg_platform_video_analytics.udfs.PipelineInitCode._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Script_1 {
  def apply(context: Context): Unit = {
    val spark = context.spark
    val Config = context.config
    import java.time._
    import org.joda.time.format._
    import org.joda.time.DateTime
    
     // a lot of this is taken from the generated code in the Read_Proto files. HOUR is probably not needed
        def HOUR: String = "hour".toLowerCase
        def isHourlyPartition: Boolean = "hour".trim.toLowerCase == "hour".toLowerCase
        val partitionDateOrHourArg : String = Config.system.startDate.trim
    
        // TODO get writing to config working
        // for Network Analytics:
        //          ymd         |        ymdh
        // ---------------------+---------------------
        //  2024-06-03 00:00:00 | 2024-06-03 04:00:00
        // for path to CSV file : 2024/06/04/03
    
        val parsed : DateTime = DateTimeFormat.forPattern("yyyy-MM-dd HH:00:00").parseDateTime(partitionDateOrHourArg)
        Config.XR_BUSINESS_DATE = DateTimeFormat.forPattern("yyyyMMdd").print(parsed)
        Config.XR_BUSINESS_HOUR = DateTimeFormat.forPattern("HH").print(parsed)
    
    println("XR_BUSINESS_DATE")
    println(Config.XR_BUSINESS_DATE)
    println("XR_BUSINESS_HOUR")
    println(Config.XR_BUSINESS_HOUR)
  }

}
