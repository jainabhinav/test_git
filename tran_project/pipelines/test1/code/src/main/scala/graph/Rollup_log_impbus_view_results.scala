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

object Rollup_log_impbus_view_results {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(
      col("_result").getField("date_time").as("date_time"),
      col("_result").getField("auction_id_64").as("auction_id_64"),
      col("_result").getField("user_id_64").as("user_id_64"),
      col("_result").getField("view_result").as("view_result"),
      col("_result").getField("ttl").as("ttl"),
      col("_result").getField("view_data").as("view_data"),
      col("_result")
        .getField("viewdef_definition_id")
        .as("viewdef_definition_id"),
      col("_result").getField("viewdef_view_result").as("viewdef_view_result"),
      col("_result")
        .getField("view_not_measurable_type")
        .as("view_not_measurable_type"),
      col("_result")
        .getField("view_not_visible_type")
        .as("view_not_visible_type"),
      col("_result").getField("view_frame_type").as("view_frame_type"),
      col("_result").getField("view_script_version").as("view_script_version"),
      col("_result").getField("view_tag_version").as("view_tag_version"),
      col("_result").getField("view_screen_width").as("view_screen_width"),
      col("_result").getField("view_screen_height").as("view_screen_height"),
      col("_result").getField("view_js_browser").as("view_js_browser"),
      col("_result").getField("view_js_platform").as("view_js_platform"),
      col("_result").getField("view_banner_left").as("view_banner_left"),
      col("_result").getField("view_banner_top").as("view_banner_top"),
      col("_result").getField("view_banner_width").as("view_banner_width"),
      col("_result").getField("view_banner_height").as("view_banner_height"),
      col("_result")
        .getField("view_tracking_duration")
        .as("view_tracking_duration"),
      col("_result").getField("view_page_duration").as("view_page_duration"),
      col("_result").getField("view_usage_duration").as("view_usage_duration"),
      col("_result").getField("view_surface").as("view_surface"),
      col("_result").getField("view_js_message").as("view_js_message"),
      col("_result").getField("view_player_width").as("view_player_width"),
      col("_result").getField("view_player_height").as("view_player_height"),
      col("_result").getField("view_iab_duration").as("view_iab_duration"),
      col("_result")
        .getField("view_iab_inview_count")
        .as("view_iab_inview_count"),
      col("_result")
        .getField("view_duration_gt_0pct")
        .as("view_duration_gt_0pct"),
      col("_result")
        .getField("view_duration_gt_25pct")
        .as("view_duration_gt_25pct"),
      col("_result")
        .getField("view_duration_gt_50pct")
        .as("view_duration_gt_50pct"),
      col("_result")
        .getField("view_duration_gt_75pct")
        .as("view_duration_gt_75pct"),
      col("_result")
        .getField("view_duration_eq_100pct")
        .as("view_duration_eq_100pct"),
      col("_result").getField("auction_timestamp").as("auction_timestamp"),
      col("_result")
        .getField("view_has_banner_left")
        .as("view_has_banner_left"),
      col("_result").getField("view_has_banner_top").as("view_has_banner_top"),
      col("_result")
        .getField("view_mouse_position_final_x")
        .as("view_mouse_position_final_x"),
      col("_result")
        .getField("view_mouse_position_final_y")
        .as("view_mouse_position_final_y"),
      col("_result")
        .getField("view_has_mouse_position_final")
        .as("view_has_mouse_position_final"),
      col("_result")
        .getField("view_mouse_position_initial_x")
        .as("view_mouse_position_initial_x"),
      col("_result")
        .getField("view_mouse_position_initial_y")
        .as("view_mouse_position_initial_y"),
      col("_result")
        .getField("view_has_mouse_position_initial")
        .as("view_has_mouse_position_initial"),
      col("_result")
        .getField("view_mouse_position_page_x")
        .as("view_mouse_position_page_x"),
      col("_result")
        .getField("view_mouse_position_page_y")
        .as("view_mouse_position_page_y"),
      col("_result")
        .getField("view_has_mouse_position_page")
        .as("view_has_mouse_position_page"),
      col("_result")
        .getField("view_mouse_position_timeout_x")
        .as("view_mouse_position_timeout_x"),
      col("_result")
        .getField("view_mouse_position_timeout_y")
        .as("view_mouse_position_timeout_y"),
      col("_result")
        .getField("view_has_mouse_position_timeout")
        .as("view_has_mouse_position_timeout"),
      col("_result").getField("view_session_id").as("view_session_id"),
      col("_result").getField("view_video").as("view_video"),
      col("_result")
        .getField("anonymized_user_info")
        .as("anonymized_user_info"),
      col("_result").getField("is_deferred").as("is_deferred")
    )

}
