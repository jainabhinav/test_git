package graph.Create_sup_lookup_files

import io.prophecy.libs._
import graph.Create_sup_lookup_files.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object sup_ip_range {

  def apply(context: Context, in: DataFrame): Unit = {
    val Config = context.config
    import _root_.io.prophecy.abinitio.dml.DMLSchema.parse
    import _root_.io.prophecy.libs.FFSchemaRecord
    import _root_.io.prophecy.libs.FixedFormatSchemaImplicits._
    import play.api.libs.json.Json
    try {
      val schema =
        Some("""type sfixed32_t = little endian integer(4);
type string_t = utf8 string(unsigned little endian integer(4));
type enum_t = little endian integer(4);
metadata type =
record
  sfixed32_t index;
  string_t start_ip;
  string_t end_ip;
  string_t name;
  enum_t ip_feature;
  int start_ip_number;
  int end_ip_number;
end ;""").map(s => parse(s).asInstanceOf[FFSchemaRecord])
      var writer = in.write.format("io.prophecy.libs.FixedFileFormat")
      writer = writer.mode("overwrite")
      schema
        .map(s => Json.stringify(Json.toJson(s)))
        .foreach(schema => writer = writer.option("schema", schema))
      writer.save(
        s"${Config.XR_LOOKUP_DATA}/data_team/lookups/trx_tl_ad_request_trans_path_core_processing_pb/sup_ip_range.${Config.XR_BUSINESS_DATE}${Config.XR_BUSINESS_HOUR}.dat"
      )
    } catch {
      case e: Error =>
        println(s"Error occurred while writing dataframe: $e")
        throw new Exception(e.getMessage)
      case e: Throwable =>
        println(s"Throwable occurred while writing dataframe: $e")
        throw new Exception(e.getMessage)
    }
  }

}
