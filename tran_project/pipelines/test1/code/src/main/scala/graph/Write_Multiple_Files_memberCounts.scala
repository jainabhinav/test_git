package graph

import io.prophecy.libs._
import udfs.PipelineInitCode._
import udfs.UDFs._
import udfs.ColumnFunctions._
import config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Write_Multiple_Files_memberCounts {

  def apply(context: Context, in: DataFrame): Unit = {
    import org.apache.spark.sql.ProphecyDataFrame
    ProphecyDataFrame
      .extendedDataFrame(
        in.withColumn(
          "fileName",
          concat(lit(context.config.XR_ADI_MEMBER_INFO), lit("/memberCounts"))
        )
      )
      .breakAndWriteDataFrameForOutputFile(List("\"keyBin\"", "\"valBin\""),
                                           "fileName",
                                           "parquet",
                                           Some(",")
      )
  }

}
