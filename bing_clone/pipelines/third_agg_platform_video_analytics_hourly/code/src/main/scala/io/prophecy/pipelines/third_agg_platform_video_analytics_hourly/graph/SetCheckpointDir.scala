package io.prophecy.pipelines.third_agg_platform_video_analytics_hourly.graph

import io.prophecy.libs._
import io.prophecy.pipelines.third_agg_platform_video_analytics_hourly.config.Context
import io.prophecy.pipelines.third_agg_platform_video_analytics_hourly.udfs.UDFs._
import io.prophecy.pipelines.third_agg_platform_video_analytics_hourly.udfs.ColumnFunctions._
import io.prophecy.pipelines.third_agg_platform_video_analytics_hourly.udfs.PipelineInitCode._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object SetCheckpointDir {
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
    
    val parsed : DateTime = (if (isHourlyPartition)
            DateTimeFormat.forPattern("yyyy-MM-dd HH:00:00")
        else DateTimeFormat.forPattern("yyyy-MM-dd 00:00:00"))
            .parseDateTime(partitionDateOrHourArg)
    
    Config.system.dateSlash = (if (isHourlyPartition)
                DateTimeFormat.forPattern("yyyy/MM/dd/HH")
                else DateTimeFormat.forPattern("yyyy/MM/dd")).print(parsed)
    Config.system.ymd = DateTimeFormat.forPattern("yyyy-MM-dd 00:00:00").print(parsed)
    Config.system.ymdh = DateTimeFormat.forPattern("yyyy-MM-dd HH:00:00").print(parsed)            
  }

}
