package graph.Create_sup_lookup_files

import io.prophecy.libs._
import graph.Create_sup_lookup_files.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Read_Proto_Range_sup_bidder_campaign_pb_sup_bidder_campaign {

  def apply(context: Context): DataFrame = {
    val spark = context.spark
    import _root_.io.prophecy.abinitio.dml.DMLSchema.parse
    import _root_.io.prophecy.libs.FFSchemaRecord
    import _root_.io.prophecy.libs.FixedFormatSchemaImplicits._
    import play.api.libs.json.Json
    var df: DataFrame = spark.emptyDataFrame
    try {
      var reader = spark.read
        .option(
          "schema",
          Some("""type fixed32_t = unsigned little endian integer(4);
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
type sup_bidder_campaign =
record
  sfixed32_t campaign_id = NULL;
  sfixed32_t campaign_type_id = NULL;
  sfixed32_t campaign_group_id = NULL;
  sfixed32_t campaign_group_type_id = NULL;
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
end[int] sup_bidder_campaign_message_types =
[vector
  [record
    name "sup_bidder_campaign"
    field_infos [vector
                  [record name "campaign_id" number 1 type_code 4 message_type -1 optional 1],
                  [record name "campaign_type_id" number 2 type_code 4 message_type -1 optional 1],
                  [record name "campaign_group_id" number 3 type_code 4 message_type -1 optional 1],
                  [record name "campaign_group_type_id" number 4 type_code 4 message_type -1 optional 1]]]];
metadata type = sup_bidder_campaign;
metadata type = "sup_bidder_campaign" ;""")
            .map(s => parse(s).asInstanceOf[FFSchemaRecord])
            .map(s => Json.stringify(Json.toJson(s)))
            .getOrElse("")
        )
        .format("io.prophecy.libs.FixedFileFormat")
      df = reader.load("NA")
    } catch {
      case e: Error =>
        println(s"Error occurred while reading dataframe: $e")
        throw new Exception(e.getMessage)
      case e: Throwable =>
        println(s"Throwable occurred while reading dataframe: $e")
        throw new Exception(e.getMessage)
    }
    df
  }

}