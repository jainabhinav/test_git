package graph

import io.prophecy.libs._
import config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Read_Proto_Range_log_impbus_view_pb_log_impbus_view {

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
type log_view_video =
record
  real64_t view_audio_duration_eq_100pct = NULL;
  real64_t view_creative_duration = NULL;
end;
type log_anonymized_user_info =
record
  bytes_t user_id = NULL;
end;
type log_impbus_view =
record
  fixed64_t date_time;
  fixed64_t auction_id_64;
  fixed64_t user_id_64 = NULL;
  enum_t view_result = NULL;
  sfixed32_t ttl = NULL;
  string_t view_data = NULL;
  sfixed32_t viewdef_definition_id = NULL;
  sfixed32_t viewdef_view_result = NULL;
  sfixed32_t view_not_measurable_type = NULL;
  sfixed32_t view_not_visible_type = NULL;
  sfixed32_t view_frame_type = NULL;
  sfixed32_t view_script_version = NULL;
  string_t view_tag_version = NULL;
  sfixed32_t view_screen_width = NULL;
  sfixed32_t view_screen_height = NULL;
  string_t view_js_browser = NULL;
  string_t view_js_platform = NULL;
  sfixed32_t view_banner_left = NULL;
  sfixed32_t view_banner_top = NULL;
  sfixed32_t view_banner_width = NULL;
  sfixed32_t view_banner_height = NULL;
  real64_t view_tracking_duration = NULL;
  real64_t view_page_duration = NULL;
  real64_t view_usage_duration = NULL;
  real64_t view_surface = NULL;
  string_t view_js_message = NULL;
  sfixed32_t view_player_width = NULL;
  sfixed32_t view_player_height = NULL;
  real64_t view_iab_duration = NULL;
  sfixed32_t view_iab_inview_count = NULL;
  real64_t view_duration_gt_0pct = NULL;
  real64_t view_duration_gt_25pct = NULL;
  real64_t view_duration_gt_50pct = NULL;
  real64_t view_duration_gt_75pct = NULL;
  real64_t view_duration_eq_100pct = NULL;
  fixed64_t auction_timestamp;
  sfixed32_t view_has_banner_left = NULL;
  sfixed32_t view_has_banner_top = NULL;
  sfixed32_t view_mouse_position_final_x = NULL;
  sfixed32_t view_mouse_position_final_y = NULL;
  sfixed32_t view_has_mouse_position_final = NULL;
  sfixed32_t view_mouse_position_initial_x = NULL;
  sfixed32_t view_mouse_position_initial_y = NULL;
  sfixed32_t view_has_mouse_position_initial = NULL;
  sfixed32_t view_mouse_position_page_x = NULL;
  sfixed32_t view_mouse_position_page_y = NULL;
  sfixed32_t view_has_mouse_position_page = NULL;
  sfixed32_t view_mouse_position_timeout_x = NULL;
  sfixed32_t view_mouse_position_timeout_y = NULL;
  sfixed32_t view_has_mouse_position_timeout = NULL;
  fixed64_t view_session_id = NULL;
  log_view_video view_video = NULL;
  log_anonymized_user_info anonymized_user_info = NULL;
  bool_t is_deferred = NULL;
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
end[int] log_impbus_view_message_types =
[vector
  [record
    name "log_view_video"
    field_infos [vector
                  [record name "view_audio_duration_eq_100pct" number 1 type_code 1 message_type -1 optional 1],
                  [record name "view_creative_duration" number 2 type_code 1 message_type -1 optional 1]]],
  [record
    name "log_anonymized_user_info"
    field_infos [vector
                  [record name "user_id" number 1 type_code 8 message_type -1 optional 1]]],
  [record
    name "log_impbus_view"
    field_infos [vector
                  [record name "date_time" number 1 type_code 1 message_type -1 optional 0],
                  [record name "auction_id_64" number 2 type_code 1 message_type -1 optional 0],
                  [record name "user_id_64" number 3 type_code 1 message_type -1 optional 1],
                  [record name "view_result" number 4 type_code 4 message_type -1 optional 1],
                  [record name "ttl" number 5 type_code 4 message_type -1 optional 1],
                  [record name "view_data" number 6 type_code 8 message_type -1 optional 1],
                  [record name "viewdef_definition_id" number 7 type_code 4 message_type -1 optional 1],
                  [record name "viewdef_view_result" number 8 type_code 4 message_type -1 optional 1],
                  [record name "view_not_measurable_type" number 9 type_code 4 message_type -1 optional 1],
                  [record name "view_not_visible_type" number 10 type_code 4 message_type -1 optional 1],
                  [record name "view_frame_type" number 11 type_code 4 message_type -1 optional 1],
                  [record name "view_script_version" number 12 type_code 4 message_type -1 optional 1],
                  [record name "view_tag_version" number 13 type_code 8 message_type -1 optional 1],
                  [record name "view_screen_width" number 14 type_code 4 message_type -1 optional 1],
                  [record name "view_screen_height" number 15 type_code 4 message_type -1 optional 1],
                  [record name "view_js_browser" number 16 type_code 8 message_type -1 optional 1],
                  [record name "view_js_platform" number 17 type_code 8 message_type -1 optional 1],
                  [record name "view_banner_left" number 18 type_code 4 message_type -1 optional 1],
                  [record name "view_banner_top" number 19 type_code 4 message_type -1 optional 1],
                  [record name "view_banner_width" number 20 type_code 4 message_type -1 optional 1],
                  [record name "view_banner_height" number 21 type_code 4 message_type -1 optional 1],
                  [record name "view_tracking_duration" number 22 type_code 1 message_type -1 optional 1],
                  [record name "view_page_duration" number 23 type_code 1 message_type -1 optional 1],
                  [record name "view_usage_duration" number 24 type_code 1 message_type -1 optional 1],
                  [record name "view_surface" number 25 type_code 1 message_type -1 optional 1],
                  [record name "view_js_message" number 26 type_code 8 message_type -1 optional 1],
                  [record name "view_player_width" number 27 type_code 4 message_type -1 optional 1],
                  [record name "view_player_height" number 28 type_code 4 message_type -1 optional 1],
                  [record name "view_iab_duration" number 29 type_code 1 message_type -1 optional 1],
                  [record name "view_iab_inview_count" number 30 type_code 4 message_type -1 optional 1],
                  [record name "view_duration_gt_0pct" number 31 type_code 1 message_type -1 optional 1],
                  [record name "view_duration_gt_25pct" number 32 type_code 1 message_type -1 optional 1],
                  [record name "view_duration_gt_50pct" number 33 type_code 1 message_type -1 optional 1],
                  [record name "view_duration_gt_75pct" number 34 type_code 1 message_type -1 optional 1],
                  [record name "view_duration_eq_100pct" number 35 type_code 1 message_type -1 optional 1],
                  [record name "auction_timestamp" number 36 type_code 1 message_type -1 optional 0],
                  [record name "view_has_banner_left" number 37 type_code 4 message_type -1 optional 1],
                  [record name "view_has_banner_top" number 38 type_code 4 message_type -1 optional 1],
                  [record name "view_mouse_position_final_x" number 39 type_code 4 message_type -1 optional 1],
                  [record name "view_mouse_position_final_y" number 40 type_code 4 message_type -1 optional 1],
                  [record name "view_has_mouse_position_final" number 41 type_code 4 message_type -1 optional 1],
                  [record name "view_mouse_position_initial_x" number 42 type_code 4 message_type -1 optional 1],
                  [record name "view_mouse_position_initial_y" number 43 type_code 4 message_type -1 optional 1],
                  [record name "view_has_mouse_position_initial" number 44 type_code 4 message_type -1 optional 1],
                  [record name "view_mouse_position_page_x" number 45 type_code 4 message_type -1 optional 1],
                  [record name "view_mouse_position_page_y" number 46 type_code 4 message_type -1 optional 1],
                  [record name "view_has_mouse_position_page" number 47 type_code 4 message_type -1 optional 1],
                  [record name "view_mouse_position_timeout_x" number 48 type_code 4 message_type -1 optional 1],
                  [record name "view_mouse_position_timeout_y" number 49 type_code 4 message_type -1 optional 1],
                  [record name "view_has_mouse_position_timeout" number 50 type_code 4 message_type -1 optional 1],
                  [record name "view_session_id" number 51 type_code 1 message_type -1 optional 1],
                  [record name "view_video" number 54 type_code 9 message_type 0 optional 1],
                  [record name "anonymized_user_info" number 55 type_code 9 message_type 1 optional 1],
                  [record name "is_deferred" number 56 type_code 7 message_type -1 optional 1]]]];
metadata type = log_impbus_view;
metadata type = "log_impbus_view" ;""")
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
