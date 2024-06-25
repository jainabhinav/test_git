package graph.Create_sup_lookup_files

import io.prophecy.libs._
import graph.Create_sup_lookup_files.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object sup_placement_video_attributes_pb {

  def apply(context: Context, in: DataFrame): Unit = {
    val Config = context.config
    import _root_.io.prophecy.abinitio.dml.DMLSchema.parse
    import _root_.io.prophecy.libs.FFSchemaRecord
    import _root_.io.prophecy.libs.FixedFormatSchemaImplicits._
    import play.api.libs.json.Json
    try {
      val schema = Some(
        """type fixed32_t = unsigned little endian integer(4);
type fixed64_t = unsigned little endian integer(8);
type sfixed32_t = little endian integer(4);
type sfixed64_t = little endian integer(8);
type real32_t = little endian ieee real(4);
type real64_t = little endian ieee real(8);
type bool_t = unsigned integer(1);
type string_t = utf8 string(unsigned little endian integer(4));
type bytes_t = void(unsigned little endian integer(4));
type enum_t = little endian integer(4);
type length_t = unsigned little endian integer(4);
type sup_placement_video_attributes =
record
  sfixed32_t id;
  sfixed32_t supports_skippable = NULL;
  sfixed32_t max_duration_secs = NULL;
  sfixed32_t max_ad_duration_secs = NULL;
  sfixed32_t maximum_number_ads = NULL;
  sfixed32_t start_delay_secs = NULL;
  enum_t playback_method = NULL;
  enum_t video_context = NULL;
  sfixed32_t is_mediated = NULL;
  sfixed32_t skip_offset = NULL;
end;
constant
record
  string(int) name;
  record
    string(int) name;
    int number;
    unsigned integer(1) type_code;
    int message_type;
    unsigned integer(1) optional;
  end[int] field_infos;
end[int] sup_placement_video_attributes_message_types =
[vector
  [record
    name "sup_placement_video_attributes"
    field_infos [vector
                  [record name "id" number 1 type_code 4 message_type -1 optional 0],
                  [record name "supports_skippable" number 2 type_code 4 message_type -1 optional 1],
                  [record name "max_duration_secs" number 3 type_code 4 message_type -1 optional 1],
                  [record name "max_ad_duration_secs" number 4 type_code 4 message_type -1 optional 1],
                  [record name "maximum_number_ads" number 5 type_code 4 message_type -1 optional 1],
                  [record name "start_delay_secs" number 6 type_code 4 message_type -1 optional 1],
                  [record name "playback_method" number 7 type_code 4 message_type -1 optional 1],
                  [record name "video_context" number 8 type_code 4 message_type -1 optional 1],
                  [record name "is_mediated" number 9 type_code 4 message_type -1 optional 1],
                  [record name "skip_offset" number 10 type_code 4 message_type -1 optional 1]]]];
metadata type = sup_placement_video_attributes ;"""
      ).map(s => parse(s).asInstanceOf[FFSchemaRecord])
      var writer = in.write.format("io.prophecy.libs.FixedFileFormat")
      writer = writer.mode("overwrite")
      schema
        .map(s => Json.stringify(Json.toJson(s)))
        .foreach(schema => writer = writer.option("schema", schema))
      writer.save(
        s"${Config.XR_LOOKUP_DATA}/data_team/lookups/trx_tl_ad_request_trans_path_core_processing_pb/sup_placement_video_attributes_pb.${Config.XR_BUSINESS_DATE}${Config.XR_BUSINESS_HOUR}.dat"
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
