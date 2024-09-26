package graph

import io.prophecy.libs._
import config.Context
import udfs.UDFs._
import udfs.ColumnFunctions._
import udfs.PipelineInitCode._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Script_Config {
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
    // for XR_BUSINESS_DATE, XR_BUSINESS_HOUR, XR_ADI_MEMBER_INFO:
    // XR_BUSINESS_DATE | XR_BUSINESS_HOUR | XR_ADI_MEMBER_INFO
    // -----------------+------------------+--------------------
    // 20200331         | 05               | hdfs:/data_team/team_user_space/blee/prophecy/filters/agg_dw_impressions_member_info/2020/03/31/05
    // for path to CSV file : 2024/06/04/03
    
    val parsed : DateTime = (if (isHourlyPartition)
            DateTimeFormat.forPattern("yyyy-MM-dd HH:00:00")
        else DateTimeFormat.forPattern("yyyy-MM-dd 00:00:00"))
            .parseDateTime(partitionDateOrHourArg)
    
    Config.XR_BUSINESS_DATE = (DateTimeFormat.forPattern("yyyyMMdd")).print(parsed)
    Config.XR_BUSINESS_HOUR = (DateTimeFormat.forPattern("HH")).print(parsed)
    Config.XR_ADI_MEMBER_INFO = Config.XR_ADI_MEMBER_INFO + DateTimeFormat.forPattern("/yyyy/MM/dd/HH").print(parsed)
    
    // println("XXXXX XR_BUSINESS_DATE: " + Config.XR_BUSINESS_DATE)
    // println("XXXXX XR_BUSINESS_HOUR: " + Config.XR_BUSINESS_HOUR)
    // println("XXXXX XR_ADI_MEMBER_INFO: " + Config.XR_ADI_MEMBER_INFO)
    // Console.println("XXXXX XR_BUSINESS_DATE: " + Config.XR_BUSINESS_DATE)
    // Console.println("XXXXX XR_BUSINESS_HOUR: " + Config.XR_BUSINESS_HOUR)
    // Console.println("XXXXX XR_ADI_MEMBER_INFO: " + Config.XR_ADI_MEMBER_INFO)
  }

}
