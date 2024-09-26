package udfs

import _root_.io.prophecy.abinitio.ScalaFunctions._
import _root_.io.prophecy.libs._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

object UDFs extends Serializable {

  def registerUDFs(spark: SparkSession) = {
    spark.udf.register("temp18771_UDF",           temp18771_UDF)
    spark.udf.register("temp3018_UDF",            temp3018_UDF)
    spark.udf.register("personal_identifier_udf", personal_identifier_udf)
    spark.udf.register("rollup1627_UDF",          rollup1627_UDF)
    spark.udf.register("rollup1615_UDF",          rollup1615_UDF)
    spark.udf.register("rollup1621_UDF",          rollup1621_UDF)
    spark.udf.register("temp1151664_UDF",         temp1151664_UDF)
    spark.udf.register("temp2545540_UDF",         temp2545540_UDF)
    spark.udf.register("temp3256870_UDF",         temp3256870_UDF)
    spark.udf.register("temp634656_UDF",          temp634656_UDF)
    spark.udf.register("temp1344271_UDF",         temp1344271_UDF)
    spark.udf.register("temp2053714_UDF",         temp2053714_UDF)
    spark.udf.register("temp2762257_UDF",         temp2762257_UDF)
    spark.udf.register("temp3471027_UDF",         temp3471027_UDF)
    spark.udf.register("temp4180587_UDF",         temp4180587_UDF)
    spark.udf.register("temp6282792_UDF",         temp6282792_UDF)
    spark.udf.register("temp6995147_UDF",         temp6995147_UDF)
    spark.udf.register("temp10430067_UDF",        temp10430067_UDF)
    spark.udf.register(
      "f_create_agg_dw_impressions_virtual_log_dw_bid_10949800",
      f_create_agg_dw_impressions_virtual_log_dw_bid_10949800
    )
    spark.udf.register("temp1248466_UDF",      temp1248466_UDF)
    spark.udf.register("temp2704269_UDF",      temp2704269_UDF)
    spark.udf.register("temp3446552_UDF",      temp3446552_UDF)
    spark.udf.register("temp692552_UDF",       temp692552_UDF)
    spark.udf.register("temp1433148_UDF",      temp1433148_UDF)
    spark.udf.register("temp2173548_UDF",      temp2173548_UDF)
    spark.udf.register("temp2912975_UDF",      temp2912975_UDF)
    spark.udf.register("temp3652655_UDF",      temp3652655_UDF)
    spark.udf.register("temp4393171_UDF",      temp4393171_UDF)
    spark.udf.register("rollup_146_UDF_inner", rollup_146_UDF_inner)
    spark.udf.register("rollup_150_UDF_inner", rollup_150_UDF_inner)
    spark.udf.register("temp6618434_UDF",      temp6618434_UDF)
    spark.udf.register("temp7361735_UDF",      temp7361735_UDF)
    spark.udf.register("temp10950958_UDF",     temp10950958_UDF)
    spark.udf.register("temp1248596_UDF",      temp1248596_UDF)
    spark.udf.register("temp2704511_UDF",      temp2704511_UDF)
    spark.udf.register("temp3446850_UDF",      temp3446850_UDF)
    spark.udf.register("temp10951013_UDF",     temp10951013_UDF)
    spark.udf.register("temp7091_UDF",         temp7091_UDF)
    spark.udf.register("processUDF_8439",      processUDF_8439)
    spark.udf.register("allFieldsNull",        allFieldsNull)
    registerAllUDFs(spark)
  }

  def temp18771_UDF = {
    udf(
      (_current_date: Integer, _current_hour: Integer) => {
        var current_hour = _current_hour
        var current_date = _current_date
        while (compareTo(current_hour, 0) < 0) {
          current_date = current_date - 1
          current_hour = current_hour + 24
        }
        while (compareTo(current_hour, 24) >= 0) {
          current_date = current_date + 1
          current_hour = current_hour - 24
        }
        Row(convertToInt(current_hour), convertToInt(current_date))
      },
      StructType(
        List(StructField("current_hour", IntegerType, false),
             StructField("current_date", IntegerType, false)
        )
      )
    )
  }

  def temp3018_UDF = {
    udf(
      (_current_date: Integer, _current_hour: Integer) => {
        var current_hour = _current_hour
        var current_date = _current_date
        while (compareTo(current_hour, 0) < 0) {
          current_date = current_date - 1
          current_hour = current_hour + 24
        }
        while (compareTo(current_hour, 24) >= 0) {
          current_date = current_date + 1
          current_hour = current_hour - 24
        }
        Row(convertToInt(current_hour), convertToInt(current_date))
      },
      StructType(
        List(StructField("current_hour", IntegerType, false),
             StructField("current_date", IntegerType, false)
        )
      )
    )
  }

  def personal_identifier_udf = {
    udf(
      (personal_identifiers_in: Seq[Row]) =>
        personal_identifiers_in.map { x =>
          (x.getInt(0), null)
        }.toArray,
      ArrayType(StructType(
                  List(StructField("identity_type",  IntegerType, false),
                       StructField("identity_value", StringType,  false)
                  )
                ),
                false
      )
    )
  }

  def rollup1627_UDF = {
    udf(
      (
        _viewdef_view_result:        Seq[Integer],
        _viewdef_definition_id:      Seq[Integer],
        _temp_viewdef_view_result:   Integer,
        _temp_viewdef_definition_id: Integer
      ) => {
        var viewdef_view_result        = _viewdef_view_result
        var viewdef_definition_id      = _viewdef_definition_id
        var temp_viewdef_definition_id = _temp_viewdef_definition_id
        var temp_viewdef_view_result   = _temp_viewdef_view_result
        var i                          = 0
        while (i < viewdef_view_result.length) {
          temp_viewdef_definition_id =
            if (
              compareTo(if (temp_viewdef_definition_id == null)
                          0
                        else
                          temp_viewdef_definition_id,
                        if (viewdef_definition_id(i) == null)
                          0
                        else
                          viewdef_definition_id(i)
              ) > 0
            ) {
              if (temp_viewdef_definition_id == null)
                0
              else
                temp_viewdef_definition_id
            } else {
              if (
                compareTo(if (temp_viewdef_definition_id == null)
                            0
                          else
                            temp_viewdef_definition_id,
                          if (viewdef_definition_id(i) == null)
                            0
                          else
                            viewdef_definition_id(i)
                ) < 0
              ) {
                if (viewdef_definition_id(i) == null)
                  0
                else
                  viewdef_definition_id(i)
              } else {
                if (
                  compareTo(if (temp_viewdef_definition_id == null)
                              0
                            else
                              temp_viewdef_definition_id,
                            0
                  ) > 0
                ) {
                  if (temp_viewdef_definition_id == null)
                    0
                  else
                    temp_viewdef_definition_id
                } else
                  0
              }
            }
          temp_viewdef_view_result =
            if (
              compareTo(if (temp_viewdef_definition_id == null)
                          0
                        else
                          temp_viewdef_definition_id,
                        if (viewdef_definition_id(i) == null)
                          0
                        else
                          viewdef_definition_id(i)
              ) > 0
            ) {
              if (temp_viewdef_view_result == null)
                0
              else
                temp_viewdef_view_result
            } else {
              if (
                compareTo(if (temp_viewdef_definition_id == null)
                            0
                          else
                            temp_viewdef_definition_id,
                          if (viewdef_definition_id(i) == null)
                            0
                          else
                            viewdef_definition_id(i)
                ) < 0
              ) {
                if (viewdef_view_result(i) == null)
                  0
                else
                  viewdef_view_result(i)
              } else {
                if (
                  compareTo(if (temp_viewdef_definition_id == null)
                              0
                            else
                              temp_viewdef_definition_id,
                            0
                  ) > 0
                ) {
                  if (
                    (if (temp_viewdef_view_result == null)
                       0
                     else
                       temp_viewdef_view_result) == (if (
                                                       viewdef_view_result(
                                                         i
                                                       ) == null
                                                     )
                                                       0
                                                     else
                                                       viewdef_view_result(
                                                         i
                                                       )) || (if (
                                                                viewdef_view_result(
                                                                  i
                                                                ) == null
                                                              )
                                                                0
                                                              else
                                                                viewdef_view_result(
                                                                  i
                                                                )) == 0 || (if (
                                                                              temp_viewdef_view_result == null
                                                                            )
                                                                              0
                                                                            else
                                                                              temp_viewdef_view_result) == 1
                  ) {
                    if (temp_viewdef_view_result == null)
                      0
                    else
                      temp_viewdef_view_result
                  } else {
                    if (
                      (if (temp_viewdef_view_result == null)
                         0
                       else
                         temp_viewdef_view_result) == 0 || (if (
                                                              viewdef_view_result(
                                                                i
                                                              ) == null
                                                            )
                                                              0
                                                            else
                                                              viewdef_view_result(
                                                                i
                                                              )) == 1
                    ) {
                      if (viewdef_view_result(i) == null)
                        0
                      else
                        viewdef_view_result(i)
                    } else
                      3
                  }
                } else
                  0
              }
            }
          i = i + 1
        }
        temp_viewdef_view_result
      },
      IntegerType
    )
  }

  def rollup1615_UDF = {
    udf(
      (_view_result: Seq[Integer], _temp_view_result: Integer) => {
        var view_result      = _view_result
        var temp_view_result = _temp_view_result
        var i                = 0
        while (i < view_result.length) {
          temp_view_result =
            if (
              (if (temp_view_result == null)
                 0
               else
                 temp_view_result) == (if (view_result(i) == null)
                                         0
                                       else
                                         view_result(i)) || (if (
                                                               view_result(
                                                                 i
                                                               ) == null
                                                             )
                                                               0
                                                             else
                                                               view_result(
                                                                 i
                                                               )) == 0 || (if (
                                                                             temp_view_result == null
                                                                           )
                                                                             0
                                                                           else
                                                                             temp_view_result) == 1
            ) {
              if (temp_view_result == null)
                0
              else
                temp_view_result
            } else {
              if (
                (if (temp_view_result == null)
                   0
                 else
                   temp_view_result) == 0 || (if (view_result(i) == null)
                                                0
                                              else
                                                view_result(i)) == 1
              ) {
                if (view_result(i) == null)
                  0
                else
                  view_result(i)
              } else
                3
            }
          i = i + 1
        }
        temp_view_result
      },
      IntegerType
    )
  }

  def rollup1621_UDF = {
    udf(
      (
        _viewdef_definition_id:      Seq[Integer],
        _temp_viewdef_definition_id: Integer
      ) => {
        var viewdef_definition_id      = _viewdef_definition_id
        var temp_viewdef_definition_id = _temp_viewdef_definition_id
        var i                          = 0
        while (i < viewdef_definition_id.length) {
          temp_viewdef_definition_id =
            if (
              compareTo(if (temp_viewdef_definition_id == null)
                          0
                        else
                          temp_viewdef_definition_id,
                        if (viewdef_definition_id(i) == null)
                          0
                        else
                          viewdef_definition_id(i)
              ) > 0
            ) {
              if (temp_viewdef_definition_id == null)
                0
              else
                temp_viewdef_definition_id
            } else {
              if (
                compareTo(if (temp_viewdef_definition_id == null)
                            0
                          else
                            temp_viewdef_definition_id,
                          if (viewdef_definition_id(i) == null)
                            0
                          else
                            viewdef_definition_id(i)
                ) < 0
              ) {
                if (viewdef_definition_id(i) == null)
                  0
                else
                  viewdef_definition_id(i)
              } else {
                if (
                  compareTo(if (temp_viewdef_definition_id == null)
                              0
                            else
                              temp_viewdef_definition_id,
                            0
                  ) > 0
                ) {
                  if (temp_viewdef_definition_id == null)
                    0
                  else
                    temp_viewdef_definition_id
                } else
                  0
              }
            }
          i = i + 1
        }
        temp_viewdef_definition_id
      },
      IntegerType
    )
  }

  def temp1151664_UDF = {
    udf(
      (
        _l_term_id:       Integer,
        _term_id:         Integer,
        _l_pricing_terms: Seq[Row],
        _i:               Integer
      ) => {
        var term_id         = _term_id
        var i               = _i
        var l_pricing_term  = Row(null, null, null, null, null, null)
        var l_pricing_terms = _l_pricing_terms.toArray
        var l_term_id       = _l_term_id
        while (compareTo(i, l_pricing_terms.length) < 0) {
          l_term_id = 0
          if (
            !_isnull(l_pricing_terms(convertToInt(i)).getAs[Integer]("term_id"))
          )
            l_term_id = convertToInt(
              l_pricing_terms(convertToInt(i)).getAs[Integer]("term_id")
            )
          if (l_term_id == term_id)
            l_pricing_term = l_pricing_terms(convertToInt(i))
          i = i + convertToInt(1)
        }
        Row(
          if (!_isnull(l_pricing_term))
            Row(
              convertToInt(l_pricing_term.get(0)),
              l_pricing_term.get(1),
              l_pricing_term.get(2),
              l_pricing_term.get(3),
              l_pricing_term.get(4),
              convertToInt(l_pricing_term.get(5))
            )
          else
            null,
          convertToInt(i),
          convertToInt(l_term_id)
        )
      },
      StructType(
        List(
          StructField(
            "l_pricing_term",
            StructType(
              List(
                StructField("term_id",                 IntegerType, true),
                StructField("amount",                  DoubleType,  true),
                StructField("rate",                    DoubleType,  true),
                StructField("is_deduction",            BooleanType, true),
                StructField("is_media_cost_dependent", BooleanType, true),
                StructField("data_member_id",          IntegerType, true)
              )
            ),
            false
          ),
          StructField("i",         IntegerType, false),
          StructField("l_term_id", IntegerType, false)
        )
      )
    )
  }

  def temp2545540_UDF = {
    udf(
      (
        _in_pricing_terms: Seq[Row],
        _l_pricing_terms:  Seq[Row],
        _i:                Integer,
        _magnitude:        Double
      ) => {
        var l_pricing_terms  = _l_pricing_terms.toArray
        var i                = _i
        var in_pricing_terms = _in_pricing_terms.toArray
        var magnitude        = _magnitude
        while (compareTo(i, l_pricing_terms.length) < 0) {
          if (
            !_isnull(in_pricing_terms(convertToInt(i)).getAs[Double]("amount"))
          )
            l_pricing_terms(i) = Row(
              l_pricing_terms(convertToInt(i)).getAs[Integer]("term_id"),
              l_pricing_terms(convertToInt(i))
                .getAs[Double]("amount") / magnitude,
              l_pricing_terms(convertToInt(i)).getAs[Double]("rate"),
              l_pricing_terms(convertToInt(i)).getAs[Boolean]("is_deduction"),
              l_pricing_terms(convertToInt(i))
                .getAs[Boolean]("is_media_cost_dependent"),
              l_pricing_terms(convertToInt(i)).getAs[Integer]("data_member_id")
            )
          i = i + convertToInt(1)
        }
        Row(
          l_pricing_terms.map { x =>
            if (!_isnull(x))
              Row(convertToInt(x.get(0)),
                  x.get(1),
                  x.get(2),
                  x.get(3),
                  x.get(4),
                  convertToInt(x.get(5))
              )
            else
              null
          }.toArray,
          convertToInt(i)
        )
      },
      StructType(
        List(
          StructField(
            "l_pricing_terms",
            ArrayType(
              StructType(
                List(
                  StructField("term_id",                 IntegerType, true),
                  StructField("amount",                  DoubleType,  true),
                  StructField("rate",                    DoubleType,  true),
                  StructField("is_deduction",            BooleanType, true),
                  StructField("is_media_cost_dependent", BooleanType, true),
                  StructField("data_member_id",          IntegerType, true)
                )
              )
            ),
            false
          ),
          StructField("i", IntegerType, false)
        )
      )
    )
  }

  def temp3256870_UDF = {
    udf(
      (_l_filtered_terms: Seq[Row], _unfiltered_terms: Seq[Row], _i: Integer) =>
        {
          var l_current_term   = Row(null, null, null, null, null, null)
          var l_filtered_terms = _l_filtered_terms.toArray
          var i                = _i
          var unfiltered_terms = _unfiltered_terms.toArray
          while (compareTo(i, unfiltered_terms.length) < 0) {
            l_current_term = unfiltered_terms(convertToInt(i))
            if (
              unfiltered_terms(convertToInt(i))
                .getAs[Boolean]("is_deduction") == convertToBoolean(0)
            )
              l_filtered_terms =
                Array.concat(l_filtered_terms,
                             Array.fill(1)(unfiltered_terms(convertToInt(i)))
                )
            i = i + convertToInt(1)
          }
          Row(
            if (!_isnull(l_current_term))
              Row(
                convertToInt(l_current_term.get(0)),
                l_current_term.get(1),
                l_current_term.get(2),
                l_current_term.get(3),
                l_current_term.get(4),
                convertToInt(l_current_term.get(5))
              )
            else
              null,
            l_filtered_terms.map { x =>
              if (!_isnull(x))
                Row(convertToInt(x.get(0)),
                    x.get(1),
                    x.get(2),
                    x.get(3),
                    x.get(4),
                    convertToInt(x.get(5))
                )
              else
                null
            }.toArray,
            convertToInt(i)
          )
        },
      StructType(
        List(
          StructField(
            "l_current_term",
            StructType(
              List(
                StructField("term_id",                 IntegerType, true),
                StructField("amount",                  DoubleType,  true),
                StructField("rate",                    DoubleType,  true),
                StructField("is_deduction",            BooleanType, true),
                StructField("is_media_cost_dependent", BooleanType, true),
                StructField("data_member_id",          IntegerType, true)
              )
            ),
            false
          ),
          StructField(
            "l_filtered_terms",
            ArrayType(
              StructType(
                List(
                  StructField("term_id",                 IntegerType, true),
                  StructField("amount",                  DoubleType,  true),
                  StructField("rate",                    DoubleType,  true),
                  StructField("is_deduction",            BooleanType, true),
                  StructField("is_media_cost_dependent", BooleanType, true),
                  StructField("data_member_id",          IntegerType, true)
                )
              )
            ),
            false
          ),
          StructField("i", IntegerType, false)
        )
      )
    )
  }

  def temp634656_UDF = {
    udf(
      (
        _i:                Integer,
        _l_curator_margin: Double,
        _l_is_deduction:   Integer,
        _l_pricing_terms:  Seq[Row],
        _l_term_id:        Integer
      ) => {
        var i                = _i
        var l_curator_margin = _l_curator_margin
        var l_is_deduction   = _l_is_deduction
        var l_pricing_terms  = _l_pricing_terms.toArray
        var l_term_id        = _l_term_id
        while (compareTo(i, l_pricing_terms.length) < 0) {
          l_term_id = 0
          l_is_deduction = 0
          if (
            !_isnull(l_pricing_terms(convertToInt(i)).getAs[Integer]("term_id"))
          )
            l_term_id = convertToInt(
              l_pricing_terms(convertToInt(i)).getAs[Integer]("term_id")
            )
          if (
            !_isnull(
              l_pricing_terms(convertToInt(i)).getAs[Boolean]("is_deduction")
            )
          )
            l_is_deduction = convertToInt(
              l_pricing_terms(convertToInt(i)).getAs[Boolean]("is_deduction")
            )
          if (l_term_id == 95 && l_is_deduction == 1) {
            if (
              !_isnull(l_pricing_terms(convertToInt(i)).getAs[Double]("amount"))
            )
              l_curator_margin = l_curator_margin + l_pricing_terms(
                convertToInt(i)
              ).getAs[Double]("amount")
          }
          i = i + convertToInt(1)
        }
        Row(convertToInt(l_is_deduction),
            convertToInt(i),
            convertToInt(l_term_id),
            l_curator_margin
        )
      },
      StructType(
        List(
          StructField("l_is_deduction",   IntegerType, false),
          StructField("i",                IntegerType, false),
          StructField("l_term_id",        IntegerType, false),
          StructField("l_curator_margin", DoubleType,  false)
        )
      )
    )
  }

  def temp1344271_UDF = {
    udf(
      (
        _l_total_tech_fees: Double,
        _i:                 Integer,
        _l_is_deduction:    Integer,
        _l_pricing_terms:   Seq[Row],
        _l_term_id:         Integer
      ) => {
        var l_total_tech_fees = _l_total_tech_fees
        var i                 = _i
        var l_is_deduction    = _l_is_deduction
        var l_pricing_terms   = _l_pricing_terms.toArray
        var l_term_id         = _l_term_id
        while (compareTo(i, l_pricing_terms.length) < 0) {
          l_term_id = 0
          l_is_deduction = 0
          if (
            !_isnull(l_pricing_terms(convertToInt(i)).getAs[Integer]("term_id"))
          )
            l_term_id = convertToInt(
              l_pricing_terms(convertToInt(i)).getAs[Integer]("term_id")
            )
          if (
            !_isnull(
              l_pricing_terms(convertToInt(i)).getAs[Boolean]("is_deduction")
            )
          )
            l_is_deduction = convertToInt(
              l_pricing_terms(convertToInt(i)).getAs[Boolean]("is_deduction")
            )
          if (
            Array(99, 89, 88, 85, 86).contains(l_term_id) && l_is_deduction == 1
          ) {
            if (
              !_isnull(l_pricing_terms(convertToInt(i)).getAs[Double]("amount"))
            )
              l_total_tech_fees = l_total_tech_fees + l_pricing_terms(
                convertToInt(i)
              ).getAs[Double]("amount")
          }
          i = i + convertToInt(1)
        }
        Row(l_total_tech_fees,
            convertToInt(l_is_deduction),
            convertToInt(i),
            convertToInt(l_term_id)
        )
      },
      StructType(
        List(
          StructField("l_total_tech_fees", DoubleType,  false),
          StructField("l_is_deduction",    IntegerType, false),
          StructField("i",                 IntegerType, false),
          StructField("l_term_id",         IntegerType, false)
        )
      )
    )
  }

  def temp2053714_UDF = {
    udf(
      (
        _i:               Integer,
        _l_seller_fees:   Double,
        _l_is_deduction:  Integer,
        _l_pricing_terms: Seq[Row],
        _l_term_id:       Integer
      ) => {
        var i               = _i
        var l_seller_fees   = _l_seller_fees
        var l_is_deduction  = _l_is_deduction
        var l_pricing_terms = _l_pricing_terms.toArray
        var l_term_id       = _l_term_id
        while (compareTo(i, l_pricing_terms.length) < 0) {
          l_term_id = 0
          l_is_deduction = 0
          if (
            !_isnull(l_pricing_terms(convertToInt(i)).getAs[Integer]("term_id"))
          )
            l_term_id = convertToInt(
              l_pricing_terms(convertToInt(i)).getAs[Integer]("term_id")
            )
          if (
            !_isnull(
              l_pricing_terms(convertToInt(i)).getAs[Boolean]("is_deduction")
            )
          )
            l_is_deduction = convertToInt(
              l_pricing_terms(convertToInt(i)).getAs[Boolean]("is_deduction")
            )
          if (l_term_id != 90 && l_is_deduction == 1) {
            if (
              !_isnull(l_pricing_terms(convertToInt(i)).getAs[Double]("amount"))
            )
              l_seller_fees = l_seller_fees + l_pricing_terms(convertToInt(i))
                .getAs[Double]("amount")
          }
          i = i + convertToInt(1)
        }
        Row(convertToInt(l_is_deduction),
            convertToInt(i),
            convertToInt(l_term_id),
            l_seller_fees
        )
      },
      StructType(
        List(
          StructField("l_is_deduction", IntegerType, false),
          StructField("i",              IntegerType, false),
          StructField("l_term_id",      IntegerType, false),
          StructField("l_seller_fees",  DoubleType,  false)
        )
      )
    )
  }

  def temp2762257_UDF = {
    udf(
      (
        _l_data_costs:                Seq[Row],
        _l_member_sales_tax_rate_pct: Double,
        _i:                           Integer,
        _l_data_cost:                 Row,
        _data_costs:                  Seq[Row]
      ) => {
        var l_data_costs                = _l_data_costs.toArray
        var l_member_sales_tax_rate_pct = _l_member_sales_tax_rate_pct
        var i                           = _i
        var l_data_cost                 = _l_data_cost
        var data_costs                  = _data_costs.toArray
        while (compareTo(i, data_costs.length) < 0) {
          l_data_cost = data_costs(convertToInt(i))
          if (!_isnull(data_costs(convertToInt(i))))
            l_data_cost =
              updateIndexInRow(l_data_cost,
                               1,
                               l_data_cost.getAs[Double](
                                 "cost"
                               ) * (1.0d + l_member_sales_tax_rate_pct / 100.0d)
              )
          l_data_costs = Array.concat(l_data_costs, Array.fill(1)(l_data_cost))
          i = i + convertToInt(1)
        }
        Row(
          l_data_costs.map { x =>
            if (!_isnull(x))
              Row(convertToInt(x.get(0)),
                  x.get(1),
                  x.getAs[Seq[Integer]](2).toArray,
                  x.get(3)
              )
            else
              null
          }.toArray,
          convertToInt(i),
          if (!_isnull(l_data_cost))
            Row(convertToInt(l_data_cost.get(0)),
                l_data_cost.get(1),
                l_data_cost.getAs[Seq[Integer]](2).toArray,
                l_data_cost.get(3)
            )
          else
            null
        )
      },
      StructType(
        List(
          StructField(
            "l_data_costs",
            ArrayType(
              StructType(
                List(
                  StructField("data_member_id", IntegerType,            true),
                  StructField("cost",           DoubleType,             true),
                  StructField("used_segments",  ArrayType(IntegerType), false),
                  StructField("cost_pct",       DoubleType,             true)
                )
              )
            ),
            false
          ),
          StructField("i", IntegerType, false),
          StructField(
            "l_data_cost",
            StructType(
              List(
                StructField("data_member_id", IntegerType,            true),
                StructField("cost",           DoubleType,             true),
                StructField("used_segments",  ArrayType(IntegerType), false),
                StructField("cost_pct",       DoubleType,             true)
              )
            ),
            false
          )
        )
      )
    )
  }

  def temp3471027_UDF = {
    udf(
      (
        _l_term_id:       Integer,
        _l_pricing_terms: Seq[Row],
        _i:               Integer,
        _l_is_deduction:  Integer
      ) => {
        var i                                        = _i
        var l_is_deduction                           = _l_is_deduction
        var l_pricing_terms                          = _l_pricing_terms.toArray
        var l_term_id                                = _l_term_id
        var l_is_curator_margin_media_cost_dependent = 0
        while (compareTo(i, l_pricing_terms.length) < 0) {
          l_term_id = 0
          l_is_deduction = 0
          if (
            !_isnull(l_pricing_terms(convertToInt(i)).getAs[Integer]("term_id"))
          )
            l_term_id = convertToInt(
              l_pricing_terms(convertToInt(i)).getAs[Integer]("term_id")
            )
          if (
            !_isnull(
              l_pricing_terms(convertToInt(i)).getAs[Boolean]("is_deduction")
            )
          )
            l_is_deduction = convertToInt(
              l_pricing_terms(convertToInt(i)).getAs[Boolean]("is_deduction")
            )
          if (l_term_id == 95 && l_is_deduction == 1) {
            if (
              !_isnull(
                l_pricing_terms(convertToInt(i))
                  .getAs[Boolean]("is_media_cost_dependent")
              )
            )
              l_is_curator_margin_media_cost_dependent = convertToInt(
                l_pricing_terms(convertToInt(i))
                  .getAs[Boolean]("is_media_cost_dependent")
              )
          }
          i = i + convertToInt(1)
        }
        Row(convertToInt(l_is_curator_margin_media_cost_dependent),
            convertToInt(l_is_deduction),
            convertToInt(i),
            convertToInt(l_term_id)
        )
      },
      StructType(
        List(
          StructField("l_is_curator_margin_media_cost_dependent",
                      IntegerType,
                      false
          ),
          StructField("l_is_deduction", IntegerType, false),
          StructField("i",              IntegerType, false),
          StructField("l_term_id",      IntegerType, false)
        )
      )
    )
  }

  def temp4180587_UDF = {
    udf(
      (
        _l_term_id:       Integer,
        _l_pricing_terms: Seq[Row],
        _i:               Integer,
        _l_is_deduction:  Integer
      ) => {
        var i                     = _i
        var l_curator_margin_type = 0
        var l_is_deduction        = _l_is_deduction
        var l_pricing_terms       = _l_pricing_terms.toArray
        var l_term_id             = _l_term_id
        while (compareTo(i, l_pricing_terms.length) < 0) {
          l_term_id = 0
          l_is_deduction = 0
          if (
            !_isnull(l_pricing_terms(convertToInt(i)).getAs[Integer]("term_id"))
          )
            l_term_id = convertToInt(
              l_pricing_terms(convertToInt(i)).getAs[Integer]("term_id")
            )
          if (
            !_isnull(
              l_pricing_terms(convertToInt(i)).getAs[Boolean]("is_deduction")
            )
          )
            l_is_deduction = convertToInt(
              l_pricing_terms(convertToInt(i)).getAs[Boolean]("is_deduction")
            )
          if (l_term_id == 95 && l_is_deduction == 1) {
            if (
              !_isnull(
                l_pricing_terms(convertToInt(i))
                  .getAs[Boolean]("is_media_cost_dependent")
              )
            ) {
              if (
                l_pricing_terms(convertToInt(i)).getAs[Boolean](
                  "is_media_cost_dependent"
                ) == convertToBoolean(1)
              )
                l_curator_margin_type = 1
              else l_curator_margin_type = 2
            }
          }
          i = i + convertToInt(1)
        }
        Row(convertToInt(l_is_deduction),
            convertToInt(i),
            convertToInt(l_term_id),
            convertToInt(l_curator_margin_type)
        )
      },
      StructType(
        List(
          StructField("l_is_deduction",        IntegerType, false),
          StructField("i",                     IntegerType, false),
          StructField("l_term_id",             IntegerType, false),
          StructField("l_curator_margin_type", IntegerType, false)
        )
      )
    )
  }

  def temp6282792_UDF = {
    udf(
      (_in_pricing_terms: Seq[Row], _l_pricing_terms: Seq[Row], _i: Integer) =>
        {
          var l_pricing_terms  = _l_pricing_terms.toArray
          var i                = _i
          var in_pricing_terms = _in_pricing_terms.toArray
          while (compareTo(i, l_pricing_terms.length) < 0) {
            if (
              !_isnull(
                in_pricing_terms(convertToInt(i)).getAs[Double]("amount")
              ) && !_isnull(
                in_pricing_terms(convertToInt(i))
                  .getAs[Boolean]("is_media_cost_dependent")
              ) && in_pricing_terms(convertToInt(i)).getAs[Boolean](
                "is_media_cost_dependent"
              ) == convertToBoolean(1)
            )
              l_pricing_terms(i) = Row(
                l_pricing_terms(convertToInt(i)).getAs[Integer]("term_id"),
                0,
                l_pricing_terms(convertToInt(i)).getAs[Double]("rate"),
                l_pricing_terms(convertToInt(i)).getAs[Boolean]("is_deduction"),
                l_pricing_terms(convertToInt(i))
                  .getAs[Boolean]("is_media_cost_dependent"),
                l_pricing_terms(convertToInt(i))
                  .getAs[Integer]("data_member_id")
              )
            i = i + convertToInt(1)
          }
          Row(
            l_pricing_terms.map { x =>
              if (!_isnull(x))
                Row(convertToInt(x.get(0)),
                    x.get(1),
                    x.get(2),
                    x.get(3),
                    x.get(4),
                    convertToInt(x.get(5))
                )
              else
                null
            }.toArray,
            convertToInt(i)
          )
        },
      StructType(
        List(
          StructField(
            "l_pricing_terms",
            ArrayType(
              StructType(
                List(
                  StructField("term_id",                 IntegerType, true),
                  StructField("amount",                  DoubleType,  true),
                  StructField("rate",                    DoubleType,  true),
                  StructField("is_deduction",            BooleanType, true),
                  StructField("is_media_cost_dependent", BooleanType, true),
                  StructField("data_member_id",          IntegerType, true)
                )
              )
            ),
            false
          ),
          StructField("i", IntegerType, false)
        )
      )
    )
  }

  def temp6995147_UDF = {
    udf(
      (
        _id_type:   String,
        _pi_list:   Seq[Row],
        _i:         Integer,
        _l_id_type: Integer
      ) => {
        var pi        = Row(convertToInt(0), null)
        var l_id_type = _l_id_type
        var id_type   = _id_type
        var i         = _i
        var pi_list   = _pi_list.toArray
        while (compareTo(i, pi_list.length) < 0) {
          l_id_type = 0
          if (
            !_isnull(pi_list(convertToInt(i)).getAs[Integer]("identity_type"))
          )
            l_id_type = convertToInt(
              pi_list(convertToInt(i)).getAs[Integer]("identity_type")
            )
          if (l_id_type.toString == id_type)
            pi = pi_list(convertToInt(i))
          i = i + convertToInt(1)
        }
        Row(if (!_isnull(pi))
              Row(convertToInt(pi.get(0)), pi.get(1).toString)
            else
              null,
            convertToInt(l_id_type),
            convertToInt(i)
        )
      },
      StructType(
        List(
          StructField("pi",
                      StructType(
                        List(StructField("identity_type",  IntegerType, false),
                             StructField("identity_value", StringType,  true)
                        )
                      ),
                      false
          ),
          StructField("l_id_type", IntegerType, false),
          StructField("i",         IntegerType, false)
        )
      )
    )
  }

  def temp10430067_UDF = {
    def f_keep_data_charge(
      cost_pct:     Double,
      agg_type:     Int,
      payment_type: Int
    ) = {
      var l_agg_type = convertToInt(
        if (
          (try agg_type
          catch {
            case error: Throwable => null
          }) == null
        )
          0
        else
          agg_type
      )
      var l_payment_type = convertToInt(
        if (
          (try payment_type
          catch {
            case error: Throwable => null
          }) == null
        )
          0
        else
          payment_type
      )
      var l_keep_data_charge = 0
      l_keep_data_charge = convertToInt(
        if (
          (if (
             (try cost_pct
             catch {
               case error: Throwable => null
             }) == null
           )
             0
           else
             cost_pct) > 0
        ) {
          if (
            f_payment_type_matches(l_agg_type,
                                   l_payment_type
            ) == 1 || f_forward_for_next_stage(l_agg_type, l_payment_type) == 1
          )
            1
          else
            l_keep_data_charge
        } else {
          if (
            (if (
               (try agg_type
               catch {
                 case error: Throwable => null
               }) == null
             )
               0
             else
               agg_type) == 0 || (if (
                                    (try agg_type
                                    catch {
                                      case error: Throwable => null
                                    }) == null
                                  )
                                    0
                                  else
                                    agg_type) == 4
          )
            1
          else
            l_keep_data_charge
        }
      )
      l_keep_data_charge
    }
    def f_forward_for_next_stage(agg_type: Int, payment_type: Int) = {
      var l_payment_type = convertToInt(
        if (
          (try payment_type
          catch {
            case error: Throwable => null
          }) == null
        )
          0
        else
          payment_type
      )
      var l_forward_for_next_stage = 0
      l_forward_for_next_stage = convertToInt(
        if (
          (if (
             (try agg_type
             catch {
               case error: Throwable => null
             }) == null
           )
             0
           else
             agg_type) == 1
        ) {
          if (l_payment_type == 2)
            1
          else
            0
        } else {
          if (
            (if (
               (try agg_type
               catch {
                 case error: Throwable => null
               }) == null
             )
               0
             else
               agg_type) == 3
          )
            0
          else {
            if (
              (if (
                 (try agg_type
                 catch {
                   case error: Throwable => null
                 }) == null
               )
                 0
               else
                 agg_type) == 5
            )
              0
            else
              1
          }
        }
      )
      l_forward_for_next_stage
    }
    def f_payment_type_matches(agg_type: Int, payment_type: Int) = {
      var l_payment_type_matches = 0
      l_payment_type_matches = convertToInt(
        if (
          f_is_matching_payment_and_agg_type(if (
                                               (try agg_type
                                               catch {
                                                 case error: Throwable => null
                                               }) == null
                                             )
                                               0
                                             else
                                               agg_type,
                                             if (
                                               (try payment_type
                                               catch {
                                                 case error: Throwable => null
                                               }) == null
                                             )
                                               0
                                             else
                                               payment_type
          ) == 1 || (if (
                       (try agg_type
                       catch {
                         case error: Throwable => null
                       }) == null
                     )
                       0
                     else
                       agg_type) == 5 && (if (
                                            (try payment_type
                                            catch {
                                              case error: Throwable => null
                                            }) == null
                                          )
                                            0
                                          else
                                            payment_type) == 6 || (if (
                                                                     (try agg_type
                                                                     catch {
                                                                       case error: Throwable =>
                                                                         null
                                                                     }) == null
                                                                   )
                                                                     0
                                                                   else
                                                                     agg_type) == 0 && ((if (
                                                                                           (try payment_type
                                                                                           catch {
                                                                                             case error: Throwable => null
                                                                                           }) == null
                                                                                         )
                                                                                           0
                                                                                         else
                                                                                           payment_type) != 1 && (if (
                                                                                                                    (try payment_type
                                                                                                                    catch {
                                                                                                                      case error: Throwable => null
                                                                                                                    }) == null
                                                                                                                  )
                                                                                                                    0
                                                                                                                  else
                                                                                                                    payment_type) != 2 && (if (
                                                                                                                                             (try payment_type
                                                                                                                                             catch {
                                                                                                                                               case error: Throwable => null
                                                                                                                                             }) == null
                                                                                                                                           )
                                                                                                                                             0
                                                                                                                                           else
                                                                                                                                             payment_type) != 5 && (if (
                                                                                                                                                                      (try payment_type
                                                                                                                                                                      catch {
                                                                                                                                                                        case error: Throwable => null
                                                                                                                                                                      }) == null
                                                                                                                                                                    )
                                                                                                                                                                      0
                                                                                                                                                                    else
                                                                                                                                                                      payment_type) != 6)
        )
          1
        else
          l_payment_type_matches
      )
      l_payment_type_matches
    }
    def f_is_matching_payment_and_agg_type(agg_type: Int, payment_type: Int) = {
      var l_agg_type = convertToInt(
        if (
          (try agg_type
          catch {
            case error: Throwable => null
          }) == null
        )
          0
        else
          agg_type
      )
      var l_payment_type = convertToInt(
        if (
          (try payment_type
          catch {
            case error: Throwable => null
          }) == null
        )
          0
        else
          payment_type
      )
      var l_is_matching_payment_and_agg_type = 0
      l_is_matching_payment_and_agg_type = convertToInt(
        if (
          (if (
             (try agg_type
             catch {
               case error: Throwable => null
             }) == null
           )
             0
           else
             agg_type) != 1 && (if (
                                  (try agg_type
                                  catch {
                                    case error: Throwable => null
                                  }) == null
                                )
                                  0
                                else
                                  agg_type) != 2
        ) {
          if (l_agg_type == 3) {
            if (l_payment_type == 2)
              1
            else
              l_is_matching_payment_and_agg_type
          } else {
            if (l_agg_type == 4) {
              if (l_payment_type == 5)
                1
              else
                l_is_matching_payment_and_agg_type
            } else
              l_is_matching_payment_and_agg_type
          }
        } else {
          if (
            (if (
               (try payment_type
               catch {
                 case error: Throwable => null
               }) == null
             )
               0
             else
               payment_type) == 1
          )
            1
          else
            l_is_matching_payment_and_agg_type
        }
      )
      l_is_matching_payment_and_agg_type
    }
    udf(
      (
        _l_data_costs:                Seq[Row],
        _l_payment_type:              Integer,
        _l_member_sales_tax_rate_pct: Double,
        _i:                           Integer,
        _l_media_cost_cpm:            Double,
        _l_agg_type:                  Integer,
        _l_data_cost:                 Row,
        _data_costs:                  Seq[Row]
      ) => {
        var l_data_costs                = _l_data_costs.toArray
        var l_payment_type              = _l_payment_type
        var l_member_sales_tax_rate_pct = _l_member_sales_tax_rate_pct
        var i                           = _i
        var l_media_cost_cpm            = _l_media_cost_cpm
        var l_agg_type                  = _l_agg_type
        var l_data_cost                 = _l_data_cost
        var data_costs                  = _data_costs.toArray
        while (compareTo(i, data_costs.length) < 0) {
          l_data_cost = data_costs(convertToInt(i))
          if (
            !_isnull(
              data_costs(convertToInt(i)).getAs[Double]("cost")
            ) && !_isnull(data_costs(convertToInt(i)).getAs[Double]("cost_pct"))
          ) {
            if (
              f_keep_data_charge(
                data_costs(convertToInt(i)).getAs[Double]("cost_pct"),
                l_agg_type,
                l_payment_type
              ) == 1
            ) {
              if (
                compareTo(data_costs(convertToInt(i)).getAs[Double]("cost_pct"),
                          0
                ) > 0 && f_payment_type_matches(l_agg_type, l_payment_type) == 1
              )
                l_data_cost = updateIndexInRow(
                  l_data_cost,
                  1,
                  l_media_cost_cpm * (l_data_cost.getAs[Double](
                    "cost_pct"
                  ) / 100.0d) * (1.0d + l_member_sales_tax_rate_pct / 100.0d)
                )
              if (l_agg_type == 0)
                l_data_cost = updateIndexInRow(
                  l_data_cost,
                  1,
                  l_data_cost.getAs[Double](
                    "cost"
                  ) * (1.0d + l_member_sales_tax_rate_pct / 100.0d)
                )
              l_data_costs =
                Array.concat(l_data_costs, Array.fill(1)(l_data_cost))
            }
          }
          i = i + convertToInt(1)
        }
        Row(
          l_data_costs.map { x =>
            if (!_isnull(x))
              Row(convertToInt(x.get(0)),
                  x.get(1),
                  x.getAs[Seq[Integer]](2).toArray,
                  x.get(3)
              )
            else
              null
          }.toArray,
          convertToInt(i),
          if (!_isnull(l_data_cost))
            Row(convertToInt(l_data_cost.get(0)),
                l_data_cost.get(1),
                l_data_cost.getAs[Seq[Integer]](2).toArray,
                l_data_cost.get(3)
            )
          else
            null
        )
      },
      StructType(
        List(
          StructField(
            "l_data_costs",
            ArrayType(
              StructType(
                List(
                  StructField("data_member_id", IntegerType,            true),
                  StructField("cost",           DoubleType,             true),
                  StructField("used_segments",  ArrayType(IntegerType), false),
                  StructField("cost_pct",       DoubleType,             true)
                )
              )
            ),
            false
          ),
          StructField("i", IntegerType, false),
          StructField(
            "l_data_cost",
            StructType(
              List(
                StructField("data_member_id", IntegerType,            true),
                StructField("cost",           DoubleType,             true),
                StructField("used_segments",  ArrayType(IntegerType), false),
                StructField("cost_pct",       DoubleType,             true)
              )
            ),
            false
          )
        )
      )
    )
  }

  def f_create_agg_dw_impressions_virtual_log_dw_bid_10949800 = {
    udf(
      {
        (
          _virtual_log_dw_bid:                 Row,
          _f_is_buy_side:                      Int,
          log_dw_bid:                          Row,
          f_is_error_imp:                      Int,
          is_dw:                               Int,
          imp_type:                            Int,
          f_is_default_or_error_imp:           Int,
          log_impbus_impressions_payment_type: Int,
          seller_trx_event_id:                 Int
        ) =>
          var virtual_log_dw_bid = _virtual_log_dw_bid
          if (_f_is_buy_side == 0) {
            if (
              log_dw_bid == null && (f_is_error_imp == 1 || is_dw == 1 && (imp_type == 2 || imp_type == 4))
            ) {
              virtual_log_dw_bid = updateIndexInRow(virtual_log_dw_bid, 1,  1)
              virtual_log_dw_bid = updateIndexInRow(virtual_log_dw_bid, 0,  1)
              virtual_log_dw_bid = updateIndexInRow(virtual_log_dw_bid, 48, 1)
              virtual_log_dw_bid =
                updateIndexInRow(virtual_log_dw_bid, 59, "--")
              virtual_log_dw_bid =
                updateIndexInRow(virtual_log_dw_bid,                    46, "\\\\N")
              virtual_log_dw_bid = updateIndexInRow(virtual_log_dw_bid, 62, 0)
              virtual_log_dw_bid = updateIndexInRow(virtual_log_dw_bid, 10, -2)
              virtual_log_dw_bid = updateIndexInRow(virtual_log_dw_bid, 11, -2)
              virtual_log_dw_bid = updateIndexInRow(virtual_log_dw_bid, 51, -2)
              virtual_log_dw_bid = updateIndexInRow(virtual_log_dw_bid, 52, -2)
              virtual_log_dw_bid = updateIndexInRow(virtual_log_dw_bid, 8,  -2)
              virtual_log_dw_bid = updateIndexInRow(virtual_log_dw_bid, 9,  -2)
              virtual_log_dw_bid = updateIndexInRow(virtual_log_dw_bid, 53, -2)
              virtual_log_dw_bid = updateIndexInRow(virtual_log_dw_bid, 54, -2)
              virtual_log_dw_bid =
                updateIndexInRow(virtual_log_dw_bid, 18, "---")
              virtual_log_dw_bid =
                updateIndexInRow(virtual_log_dw_bid,                    24, "USD")
              virtual_log_dw_bid = updateIndexInRow(virtual_log_dw_bid, 25, 1)
              virtual_log_dw_bid = updateIndexInRow(virtual_log_dw_bid, 55, "u")
              virtual_log_dw_bid = updateIndexInRow(virtual_log_dw_bid, 23, 1)
              virtual_log_dw_bid = updateIndexInRow(virtual_log_dw_bid, 20, 2)
              virtual_log_dw_bid = updateIndexInRow(virtual_log_dw_bid, 27, -1)
              virtual_log_dw_bid = updateIndexInRow(virtual_log_dw_bid, 28, -1)
            } else {
              if (f_is_default_or_error_imp == 1) {
                if (log_dw_bid != null) virtual_log_dw_bid = log_dw_bid
                if (f_is_default_or_error_imp == 1) {
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 10, -2)
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 11, -2)
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 51, -2)
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 52, -2)
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 8, -2)
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 9, -2)
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 53, -2)
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 54, -2)
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 4, 0)
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 6, 0)
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 5, 0)
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 7, 0)
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 23, 0)
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 12, 0)
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 72, 0)
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 73, 0)
                }
                if (f_is_error_imp == 1) {
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 24, "USD")
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 25, 1.0d)
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 55, "u")
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 56, 0)
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 19, 0)
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 20, 2)
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 21, 0)
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 22, 0)
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 27, -1)
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 28, -1)
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 18, "---")
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 7, 0)
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 26, 0)
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 43, 0)
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 16, 0)
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 23, 1)
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 31, 0)
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 32, 0)
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 14, 0)
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 17, 0)
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 2, 0)
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 33, 0)
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 34, 0)
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 29, 0)
                }
              } else {
                if (imp_type == 9 || imp_type == 2 || imp_type == 5)
                  virtual_log_dw_bid = log_dw_bid
                else {
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 1, 1)
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 0, 1)
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 48, 1)
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 10, -1)
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 11, -1)
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 51, -1)
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 52, -1)
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 8, -1)
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 9, -1)
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 53, -1)
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 54, -1)
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 57, 0)
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 58, 0)
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 59, "---")
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 24, "USD")
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 25, 1)
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 18, "---")
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 66, 2)
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 41, 0)
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid,
                                     37,
                                     log_impbus_impressions_payment_type
                    )
                  virtual_log_dw_bid = updateIndexInRow(virtual_log_dw_bid,
                                                        62,
                                                        seller_trx_event_id
                  )
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 28, -2)
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 27, -2)
                }
              }
            }
          }
          virtual_log_dw_bid
      },
      StructType(
        Array(
          StructField("date_time",                LongType,    true),
          StructField("auction_id_64",            LongType,    true),
          StructField("price",                    DoubleType,  true),
          StructField("member_id",                IntegerType, true),
          StructField("advertiser_id",            IntegerType, true),
          StructField("campaign_group_id",        IntegerType, true),
          StructField("campaign_id",              IntegerType, true),
          StructField("creative_id",              IntegerType, true),
          StructField("creative_freq",            IntegerType, true),
          StructField("creative_rec",             IntegerType, true),
          StructField("advertiser_freq",          IntegerType, true),
          StructField("advertiser_rec",           IntegerType, true),
          StructField("is_remarketing",           IntegerType, true),
          StructField("user_group_id",            IntegerType, true),
          StructField("media_buy_cost",           DoubleType,  true),
          StructField("is_default",               IntegerType, true),
          StructField("pub_rule_id",              IntegerType, true),
          StructField("media_buy_rev_share_pct",  DoubleType,  true),
          StructField("pricing_type",             StringType,  true),
          StructField("can_convert",              IntegerType, true),
          StructField("is_control",               IntegerType, true),
          StructField("control_pct",              DoubleType,  true),
          StructField("control_creative_id",      IntegerType, true),
          StructField("cadence_modifier",         DoubleType,  true),
          StructField("advertiser_currency",      StringType,  true),
          StructField("advertiser_exchange_rate", DoubleType,  true),
          StructField("insertion_order_id",       IntegerType, true),
          StructField("predict_type",             IntegerType, true),
          StructField("predict_type_goal",        IntegerType, true),
          StructField("revenue_value_dollars",    DoubleType,  true),
          StructField("revenue_value_adv_curr",   DoubleType,  true),
          StructField("commission_cpm",           DoubleType,  true),
          StructField("commission_revshare",      DoubleType,  true),
          StructField("serving_fees_cpm",         DoubleType,  true),
          StructField("serving_fees_revshare",    DoubleType,  true),
          StructField("publisher_currency",       StringType,  true),
          StructField("publisher_exchange_rate",  DoubleType,  true),
          StructField("payment_type",             IntegerType, true),
          StructField("payment_value",            DoubleType,  true),
          StructField("creative_group_freq",      IntegerType, true),
          StructField("creative_group_rec",       IntegerType, true),
          StructField("revenue_type",             IntegerType, true),
          StructField("apply_cost_on_default",    IntegerType, true),
          StructField("instance_id",              IntegerType, true),
          StructField("vp_expose_age",            IntegerType, true),
          StructField("vp_expose_gender",         IntegerType, true),
          StructField("targeted_segments",        StringType,  true),
          StructField("ttl",                      IntegerType, true),
          StructField("auction_timestamp",        LongType,    true),
          StructField(
            "data_costs",
            ArrayType(
              StructType(
                Array(
                  StructField("data_member_id", IntegerType, true),
                  StructField("cost",           DoubleType,  true),
                  StructField("used_segments",
                              ArrayType(IntegerType, true),
                              true
                  ),
                  StructField("cost_pct", DoubleType, true)
                )
              ),
              true
            ),
            true
          ),
          StructField("targeted_segment_list",
                      ArrayType(IntegerType, true),
                      true
          ),
          StructField("campaign_group_freq",        IntegerType, true),
          StructField("campaign_group_rec",         IntegerType, true),
          StructField("insertion_order_freq",       IntegerType, true),
          StructField("insertion_order_rec",        IntegerType, true),
          StructField("buyer_gender",               StringType,  true),
          StructField("buyer_age",                  IntegerType, true),
          StructField("custom_model_id",            IntegerType, true),
          StructField("custom_model_last_modified", LongType,    true),
          StructField("custom_model_output_code",   StringType,  true),
          StructField("bid_priority",               IntegerType, true),
          StructField("explore_disposition",        IntegerType, true),
          StructField("revenue_auction_event_type", IntegerType, true),
          StructField(
            "campaign_group_models",
            ArrayType(
              StructType(
                Array(
                  StructField("model_type", IntegerType, true),
                  StructField("model_id",   IntegerType, true),
                  StructField("leaf_code",  StringType,  true),
                  StructField("origin",     IntegerType, true),
                  StructField("experiment", IntegerType, true),
                  StructField("value",      FloatType,   true)
                )
              ),
              true
            ),
            true
          ),
          StructField("impression_transaction_type", IntegerType, true),
          StructField("is_deferred",                 IntegerType, true),
          StructField("log_type",                    IntegerType, true),
          StructField("crossdevice_group_anon",
                      StructType(
                        Array(StructField("graph_id", IntegerType, true),
                              StructField("group_id", BinaryType,  true)
                        )
                      ),
                      true
          ),
          StructField("fx_rate_snapshot_id", IntegerType, true),
          StructField(
            "crossdevice_graph_cost",
            StructType(
              Array(StructField("graph_provider_member_id", IntegerType, true),
                    StructField("cost_cpm_usd",             DoubleType,  true)
              )
            ),
            true
          ),
          StructField("revenue_event_type_id", IntegerType, true),
          StructField(
            "targeted_segment_details",
            ArrayType(StructType(
                        Array(StructField("segment_id",    IntegerType, true),
                              StructField("last_seen_min", IntegerType, true)
                        )
                      ),
                      true
            ),
            true
          ),
          StructField("insertion_order_budget_interval_id", IntegerType, true),
          StructField("campaign_group_budget_interval_id",  IntegerType, true),
          StructField("cold_start_price_type",              IntegerType, true),
          StructField("discovery_state",                    IntegerType, true),
          StructField(
            "revenue_info",
            StructType(
              Array(
                StructField("total_partner_fees_microcents", LongType,   true),
                StructField("booked_revenue_dollars",        DoubleType, true),
                StructField("booked_revenue_adv_curr",       DoubleType, true),
                StructField("total_data_costs_microcents",   LongType,   true),
                StructField("total_profit_microcents",       LongType,   true),
                StructField("total_segment_data_costs_microcents",
                            LongType,
                            true
                ),
                StructField("total_feature_costs_microcents", LongType, true)
              )
            ),
            true
          ),
          StructField("use_revenue_info",              BooleanType, true),
          StructField("sales_tax_rate_pct",            DoubleType,  true),
          StructField("targeted_crossdevice_graph_id", IntegerType, true),
          StructField("product_feed_id",               IntegerType, true),
          StructField("item_selection_strategy_id",    IntegerType, true),
          StructField("discovery_prediction",          DoubleType,  true),
          StructField("bidding_host_id",               IntegerType, true),
          StructField("split_id",                      IntegerType, true),
          StructField(
            "excluded_targeted_segment_details",
            ArrayType(
              StructType(Array(StructField("segment_id", IntegerType, true))),
              true
            ),
            true
          ),
          StructField("predicted_kpi_event_rate",          DoubleType,  true),
          StructField("has_crossdevice_reach_extension",   BooleanType, true),
          StructField("advertiser_expected_value_ecpm_ac", DoubleType,  true),
          StructField("bpp_multiplier",                    DoubleType,  true),
          StructField("bpp_offset",                        DoubleType,  true),
          StructField("bid_modifier",                      DoubleType,  true),
          StructField("payment_value_microcents",          LongType,    true),
          StructField(
            "crossdevice_graph_membership",
            ArrayType(StructType(
                        Array(StructField("graph_id", IntegerType, true),
                              StructField("group_id", BinaryType,  true)
                        )
                      ),
                      true
            ),
            true
          ),
          StructField(
            "valuation_landscape",
            ArrayType(
              StructType(
                Array(
                  StructField("kpi_event_id",               IntegerType, true),
                  StructField("ev_kpi_event_ac",            DoubleType,  true),
                  StructField("p_kpi_event",                DoubleType,  true),
                  StructField("bpo_aggressiveness_factor",  DoubleType,  true),
                  StructField("min_margin_pct",             DoubleType,  true),
                  StructField("max_revenue_or_bid_value",   DoubleType,  true),
                  StructField("min_revenue_or_bid_value",   DoubleType,  true),
                  StructField("cold_start_price_ac",        DoubleType,  true),
                  StructField("dynamic_bid_max_revenue_ac", DoubleType,  true),
                  StructField("p_revenue_event",            DoubleType,  true),
                  StructField("total_fees_deducted_ac",     DoubleType,  true)
                )
              ),
              true
            ),
            true
          ),
          StructField("line_item_currency",             StringType,  true),
          StructField("measurement_fee_cpm_usd",        DoubleType,  true),
          StructField("measurement_provider_id",        IntegerType, true),
          StructField("measurement_provider_member_id", IntegerType, true),
          StructField("offline_attribution_provider_member_id",
                      IntegerType,
                      true
          ),
          StructField("offline_attribution_cost_usd_cpm", DoubleType, true),
          StructField(
            "targeted_segment_details_by_id_type",
            ArrayType(
              StructType(
                Array(
                  StructField("identity_type", IntegerType, true),
                  StructField(
                    "targeted_segment_details",
                    ArrayType(
                      StructType(
                        Array(StructField("segment_id",    IntegerType, true),
                              StructField("last_seen_min", IntegerType, true)
                        )
                      ),
                      true
                    ),
                    true
                  )
                )
              ),
              true
            ),
            true
          ),
          StructField(
            "offline_attribution",
            ArrayType(
              StructType(
                Array(StructField("provider_member_id", IntegerType, true),
                      StructField("cost_usd_cpm",       DoubleType,  true)
                )
              ),
              true
            ),
            true
          ),
          StructField("frequency_cap_type_internal", IntegerType, true),
          StructField("modeled_cap_did_override_line_item_daily_cap",
                      BooleanType,
                      true
          ),
          StructField("modeled_cap_user_sample_rate", DoubleType, true),
          StructField("bid_rate",                     DoubleType, true),
          StructField("district_postal_code_lists",
                      ArrayType(IntegerType, true),
                      true
          ),
          StructField("pre_bpp_price",        DoubleType,  true),
          StructField("feature_tests_bitmap", IntegerType, true)
        )
      )
    )
  }

  def temp1248466_UDF = {
    udf(
      (
        _l_term_id:       Integer,
        _term_id:         Integer,
        _l_pricing_terms: Seq[Row],
        _i:               Integer
      ) => {
        var term_id         = _term_id
        var i               = _i
        var l_pricing_term  = Row(null, null, null, null, null, null)
        var l_pricing_terms = _l_pricing_terms.toArray
        var l_term_id       = _l_term_id
        while (compareTo(i, l_pricing_terms.length) < 0) {
          l_term_id = 0
          if (
            !_isnull(l_pricing_terms(convertToInt(i)).getAs[Integer]("term_id"))
          )
            l_term_id = convertToInt(
              l_pricing_terms(convertToInt(i)).getAs[Integer]("term_id")
            )
          if (l_term_id == term_id)
            l_pricing_term = l_pricing_terms(convertToInt(i))
          i = i + convertToInt(1)
        }
        Row(
          if (!_isnull(l_pricing_term))
            Row(
              convertToInt(l_pricing_term.getAs[Integer](0)),
              l_pricing_term.getAs[Double](1),
              l_pricing_term.getAs[Double](2),
              l_pricing_term.getAs[Boolean](3),
              l_pricing_term.getAs[Boolean](4),
              convertToInt(l_pricing_term.getAs[Integer](5))
            )
          else null,
          convertToInt(i),
          convertToInt(l_term_id)
        )
      },
      StructType(
        List(
          StructField(
            "l_pricing_term",
            StructType(
              List(
                StructField("term_id",                 IntegerType, true),
                StructField("amount",                  DoubleType,  true),
                StructField("rate",                    DoubleType,  true),
                StructField("is_deduction",            BooleanType, true),
                StructField("is_media_cost_dependent", BooleanType, true),
                StructField("data_member_id",          IntegerType, true)
              )
            ),
            false
          ),
          StructField("i",         IntegerType, false),
          StructField("l_term_id", IntegerType, false)
        )
      )
    )
  }

  def temp2704269_UDF = {
    udf(
      (
        _in_pricing_terms: Seq[Row],
        _l_pricing_terms:  Seq[Row],
        _i:                Integer,
        _magnitude:        Double
      ) => {
        var l_pricing_terms  = _l_pricing_terms.toArray
        var i                = _i
        var in_pricing_terms = _in_pricing_terms.toArray
        var magnitude        = _magnitude
        while (compareTo(i, l_pricing_terms.length) < 0) {
          if (
            !_isnull(in_pricing_terms(convertToInt(i)).getAs[Double]("amount"))
          )
            l_pricing_terms(i) = Row(
              l_pricing_terms(convertToInt(i)).getAs[Integer]("term_id"),
              l_pricing_terms(convertToInt(i))
                .getAs[Double]("amount") / magnitude,
              l_pricing_terms(convertToInt(i)).getAs[Double]("rate"),
              l_pricing_terms(convertToInt(i)).getAs[Boolean]("is_deduction"),
              l_pricing_terms(convertToInt(i))
                .getAs[Boolean]("is_media_cost_dependent"),
              l_pricing_terms(convertToInt(i)).getAs[Integer]("data_member_id")
            )
          i = i + convertToInt(1)
        }
        Row(
          l_pricing_terms.map { x =>
            if (!_isnull(x))
              Row(convertToInt(x.getAs[Integer](0)),
                  x.getAs[Double](1),
                  x.getAs[Double](2),
                  x.getAs[Boolean](3),
                  x.getAs[Boolean](4),
                  convertToInt(x.getAs[Integer](5))
              )
            else null
          }.toArray,
          convertToInt(i)
        )
      },
      StructType(
        List(
          StructField(
            "l_pricing_terms",
            ArrayType(
              StructType(
                List(
                  StructField("term_id",                 IntegerType, true),
                  StructField("amount",                  DoubleType,  true),
                  StructField("rate",                    DoubleType,  true),
                  StructField("is_deduction",            BooleanType, true),
                  StructField("is_media_cost_dependent", BooleanType, true),
                  StructField("data_member_id",          IntegerType, true)
                )
              )
            ),
            false
          ),
          StructField("i", IntegerType, false)
        )
      )
    )
  }

  def temp3446552_UDF = {
    udf(
      (_l_filtered_terms: Seq[Row], _unfiltered_terms: Seq[Row], _i: Integer) =>
        {
          var l_current_term   = Row(null, null, null, null, null, null)
          var l_filtered_terms = _l_filtered_terms.toArray
          var i                = _i
          var unfiltered_terms = _unfiltered_terms.toArray
          while (compareTo(i, unfiltered_terms.length) < 0) {
            l_current_term = unfiltered_terms(convertToInt(i))
            if (
              unfiltered_terms(convertToInt(i))
                .getAs[Boolean]("is_deduction") == convertToBoolean(0)
            )
              l_filtered_terms =
                Array.concat(l_filtered_terms,
                             Array.fill(1)(unfiltered_terms(convertToInt(i)))
                )
            i = i + convertToInt(1)
          }
          Row(
            if (!_isnull(l_current_term))
              Row(
                convertToInt(l_current_term.getAs[Integer](0)),
                l_current_term.getAs[Double](1),
                l_current_term.getAs[Double](2),
                l_current_term.getAs[Boolean](3),
                l_current_term.getAs[Boolean](4),
                convertToInt(l_current_term.getAs[Integer](5))
              )
            else null,
            l_filtered_terms.map { x =>
              if (!_isnull(x))
                Row(convertToInt(x.getAs[Integer](0)),
                    x.getAs[Double](1),
                    x.getAs[Double](2),
                    x.getAs[Boolean](3),
                    x.getAs[Boolean](4),
                    convertToInt(x.getAs[Integer](5))
                )
              else null
            }.toArray,
            convertToInt(i)
          )
        },
      StructType(
        List(
          StructField(
            "l_current_term",
            StructType(
              List(
                StructField("term_id",                 IntegerType, true),
                StructField("amount",                  DoubleType,  true),
                StructField("rate",                    DoubleType,  true),
                StructField("is_deduction",            BooleanType, true),
                StructField("is_media_cost_dependent", BooleanType, true),
                StructField("data_member_id",          IntegerType, true)
              )
            ),
            false
          ),
          StructField(
            "l_filtered_terms",
            ArrayType(
              StructType(
                List(
                  StructField("term_id",                 IntegerType, true),
                  StructField("amount",                  DoubleType,  true),
                  StructField("rate",                    DoubleType,  true),
                  StructField("is_deduction",            BooleanType, true),
                  StructField("is_media_cost_dependent", BooleanType, true),
                  StructField("data_member_id",          IntegerType, true)
                )
              )
            ),
            false
          ),
          StructField("i", IntegerType, false)
        )
      )
    )
  }

  def temp692552_UDF = {
    udf(
      (
        _i:                Integer,
        _l_curator_margin: Double,
        _l_is_deduction:   Integer,
        _l_pricing_terms:  Seq[Row],
        _l_term_id:        Integer
      ) => {
        var i                = _i
        var l_curator_margin = _l_curator_margin
        var l_is_deduction   = _l_is_deduction
        var l_pricing_terms  = _l_pricing_terms.toArray
        var l_term_id        = _l_term_id
        while (compareTo(i, l_pricing_terms.length) < 0) {
          l_term_id = 0
          l_is_deduction = 0
          if (
            !_isnull(l_pricing_terms(convertToInt(i)).getAs[Integer]("term_id"))
          )
            l_term_id = convertToInt(
              l_pricing_terms(convertToInt(i)).getAs[Integer]("term_id")
            )
          if (
            !_isnull(
              l_pricing_terms(convertToInt(i)).getAs[Boolean]("is_deduction")
            )
          )
            l_is_deduction = convertToInt(
              l_pricing_terms(convertToInt(i)).getAs[Boolean]("is_deduction")
            )
          if (l_term_id == 95 && l_is_deduction == 1)
            if (
              !_isnull(l_pricing_terms(convertToInt(i)).getAs[Double]("amount"))
            )
              l_curator_margin = l_curator_margin + l_pricing_terms(
                convertToInt(i)
              ).getAs[Double]("amount")
          i = i + convertToInt(1)
        }
        Row(convertToInt(l_is_deduction),
            convertToInt(i),
            convertToInt(l_term_id),
            l_curator_margin
        )
      },
      StructType(
        List(
          StructField("l_is_deduction",   IntegerType, false),
          StructField("i",                IntegerType, false),
          StructField("l_term_id",        IntegerType, false),
          StructField("l_curator_margin", DoubleType,  false)
        )
      )
    )
  }

  def temp1433148_UDF = {
    udf(
      (
        _l_total_tech_fees: Double,
        _i:                 Integer,
        _l_is_deduction:    Integer,
        _l_pricing_terms:   Seq[Row],
        _l_term_id:         Integer
      ) => {
        var l_total_tech_fees = _l_total_tech_fees
        var i                 = _i
        var l_is_deduction    = _l_is_deduction
        var l_pricing_terms   = _l_pricing_terms.toArray
        var l_term_id         = _l_term_id
        while (compareTo(i, l_pricing_terms.length) < 0) {
          l_term_id = 0
          l_is_deduction = 0
          if (
            !_isnull(l_pricing_terms(convertToInt(i)).getAs[Integer]("term_id"))
          )
            l_term_id = convertToInt(
              l_pricing_terms(convertToInt(i)).getAs[Integer]("term_id")
            )
          if (
            !_isnull(
              l_pricing_terms(convertToInt(i)).getAs[Boolean]("is_deduction")
            )
          )
            l_is_deduction = convertToInt(
              l_pricing_terms(convertToInt(i)).getAs[Boolean]("is_deduction")
            )
          if (
            Array(99, 89, 88, 85, 86).contains(l_term_id) && l_is_deduction == 1
          )
            if (
              !_isnull(l_pricing_terms(convertToInt(i)).getAs[Double]("amount"))
            )
              l_total_tech_fees = l_total_tech_fees + l_pricing_terms(
                convertToInt(i)
              ).getAs[Double]("amount")
          i = i + convertToInt(1)
        }
        Row(l_total_tech_fees,
            convertToInt(l_is_deduction),
            convertToInt(i),
            convertToInt(l_term_id)
        )
      },
      StructType(
        List(
          StructField("l_total_tech_fees", DoubleType,  false),
          StructField("l_is_deduction",    IntegerType, false),
          StructField("i",                 IntegerType, false),
          StructField("l_term_id",         IntegerType, false)
        )
      )
    )
  }

  def temp2173548_UDF = {
    udf(
      (
        _i:               Integer,
        _l_seller_fees:   Double,
        _l_is_deduction:  Integer,
        _l_pricing_terms: Seq[Row],
        _l_term_id:       Integer
      ) => {
        var i               = _i
        var l_seller_fees   = _l_seller_fees
        var l_is_deduction  = _l_is_deduction
        var l_pricing_terms = _l_pricing_terms.toArray
        var l_term_id       = _l_term_id
        while (compareTo(i, l_pricing_terms.length) < 0) {
          l_term_id = 0
          l_is_deduction = 0
          if (
            !_isnull(l_pricing_terms(convertToInt(i)).getAs[Integer]("term_id"))
          )
            l_term_id = convertToInt(
              l_pricing_terms(convertToInt(i)).getAs[Integer]("term_id")
            )
          if (
            !_isnull(
              l_pricing_terms(convertToInt(i)).getAs[Boolean]("is_deduction")
            )
          )
            l_is_deduction = convertToInt(
              l_pricing_terms(convertToInt(i)).getAs[Boolean]("is_deduction")
            )
          if (l_term_id != 90 && l_is_deduction == 1)
            if (
              !_isnull(l_pricing_terms(convertToInt(i)).getAs[Double]("amount"))
            )
              l_seller_fees = l_seller_fees + l_pricing_terms(convertToInt(i))
                .getAs[Double]("amount")
          i = i + convertToInt(1)
        }
        var r = Row(convertToInt(l_is_deduction),
                    convertToInt(i),
                    convertToInt(l_term_id),
                    l_seller_fees
        )
        r
      },
      StructType(
        List(
          StructField("l_is_deduction", IntegerType, false),
          StructField("i",              IntegerType, false),
          StructField("l_term_id",      IntegerType, false),
          StructField("l_seller_fees",  DoubleType,  false)
        )
      )
    )
  }

  def temp2912975_UDF = {
    udf(
      (
        _l_data_costs:                Seq[Row],
        _l_member_sales_tax_rate_pct: Double,
        _i:                           Integer,
        _l_data_cost:                 Row,
        _data_costs:                  Seq[Row]
      ) => {
        var l_data_costs                = _l_data_costs.toArray
        var l_member_sales_tax_rate_pct = _l_member_sales_tax_rate_pct
        var i                           = _i
        var l_data_cost                 = _l_data_cost
        var data_costs                  = _data_costs.toArray
        while (compareTo(i, data_costs.length) < 0) {
          l_data_cost = data_costs(convertToInt(i))
          if (!_isnull(data_costs(convertToInt(i))))
            l_data_cost =
              updateIndexInRow(l_data_cost,
                               1,
                               l_data_cost.getAs[Double](
                                 "cost"
                               ) * (1.0d + l_member_sales_tax_rate_pct / 100.0d)
              )
          l_data_costs = Array.concat(l_data_costs, Array.fill(1)(l_data_cost))
          i = i + convertToInt(1)
        }
        Row(
          l_data_costs.map { x =>
            if (!_isnull(x))
              Row(convertToInt(x.getAs[Integer](0)),
                  x.getAs[Double](1),
                  x.getAs[Seq[Integer]](2).toArray,
                  x.getAs[Double](3)
              )
            else null
          }.toArray,
          convertToInt(i),
          if (!_isnull(l_data_cost))
            Row(convertToInt(l_data_cost.getAs[Integer](0)),
                l_data_cost.getAs[Double](1),
                l_data_cost.getAs[Seq[Integer]](2).toArray,
                l_data_cost.getAs[Double](3)
            )
          else null
        )
      },
      StructType(
        List(
          StructField(
            "l_data_costs",
            ArrayType(
              StructType(
                List(
                  StructField("data_member_id", IntegerType,            true),
                  StructField("cost",           DoubleType,             true),
                  StructField("used_segments",  ArrayType(IntegerType), false),
                  StructField("cost_pct",       DoubleType,             true)
                )
              )
            ),
            false
          ),
          StructField("i", IntegerType, false),
          StructField(
            "l_data_cost",
            StructType(
              List(
                StructField("data_member_id", IntegerType,            true),
                StructField("cost",           DoubleType,             true),
                StructField("used_segments",  ArrayType(IntegerType), false),
                StructField("cost_pct",       DoubleType,             true)
              )
            ),
            false
          )
        )
      )
    )
  }

  def temp3652655_UDF = {
    udf(
      (
        _l_term_id:       Integer,
        _l_pricing_terms: Seq[Row],
        _i:               Integer,
        _l_is_deduction:  Integer
      ) => {
        var i                                        = _i
        var l_is_deduction                           = _l_is_deduction
        var l_pricing_terms                          = _l_pricing_terms.toArray
        var l_term_id                                = _l_term_id
        var l_is_curator_margin_media_cost_dependent = 0
        while (compareTo(i, l_pricing_terms.length) < 0) {
          l_term_id = 0
          l_is_deduction = 0
          if (
            !_isnull(l_pricing_terms(convertToInt(i)).getAs[Integer]("term_id"))
          )
            l_term_id = convertToInt(
              l_pricing_terms(convertToInt(i)).getAs[Integer]("term_id")
            )
          if (
            !_isnull(
              l_pricing_terms(convertToInt(i)).getAs[Boolean]("is_deduction")
            )
          )
            l_is_deduction = convertToInt(
              l_pricing_terms(convertToInt(i)).getAs[Boolean]("is_deduction")
            )
          if (l_term_id == 95 && l_is_deduction == 1)
            if (
              !_isnull(
                l_pricing_terms(convertToInt(i))
                  .getAs[Boolean]("is_media_cost_dependent")
              )
            )
              l_is_curator_margin_media_cost_dependent = convertToInt(
                l_pricing_terms(convertToInt(i))
                  .getAs[Boolean]("is_media_cost_dependent")
              )
          i = i + convertToInt(1)
        }
        Row(convertToInt(l_is_curator_margin_media_cost_dependent),
            convertToInt(l_is_deduction),
            convertToInt(i),
            convertToInt(l_term_id)
        )
      },
      StructType(
        List(
          StructField("l_is_curator_margin_media_cost_dependent",
                      IntegerType,
                      false
          ),
          StructField("l_is_deduction", IntegerType, false),
          StructField("i",              IntegerType, false),
          StructField("l_term_id",      IntegerType, false)
        )
      )
    )
  }

  def temp4393171_UDF = {
    udf(
      (
        _l_term_id:       Integer,
        _l_pricing_terms: Seq[Row],
        _i:               Integer,
        _l_is_deduction:  Integer
      ) => {
        var i                     = _i
        var l_curator_margin_type = 0
        var l_is_deduction        = _l_is_deduction
        var l_pricing_terms       = _l_pricing_terms.toArray
        var l_term_id             = _l_term_id
        while (compareTo(i, l_pricing_terms.length) < 0) {
          l_term_id = 0
          l_is_deduction = 0
          if (
            !_isnull(l_pricing_terms(convertToInt(i)).getAs[Integer]("term_id"))
          )
            l_term_id = convertToInt(
              l_pricing_terms(convertToInt(i)).getAs[Integer]("term_id")
            )
          if (
            !_isnull(
              l_pricing_terms(convertToInt(i)).getAs[Boolean]("is_deduction")
            )
          )
            l_is_deduction = convertToInt(
              l_pricing_terms(convertToInt(i)).getAs[Boolean]("is_deduction")
            )
          if (l_term_id == 95 && l_is_deduction == 1) {
            if (
              !_isnull(
                l_pricing_terms(convertToInt(i))
                  .getAs[Boolean]("is_media_cost_dependent")
              )
            )
              if (
                l_pricing_terms(convertToInt(i)).getAs[Boolean](
                  "is_media_cost_dependent"
                ) == convertToBoolean(1)
              ) l_curator_margin_type = 1
              else l_curator_margin_type = 2
          }
          i = i + convertToInt(1)
        }
        Row(convertToInt(l_is_deduction),
            convertToInt(i),
            convertToInt(l_term_id),
            convertToInt(l_curator_margin_type)
        )
      },
      StructType(
        List(
          StructField("l_is_deduction",        IntegerType, false),
          StructField("i",                     IntegerType, false),
          StructField("l_term_id",             IntegerType, false),
          StructField("l_curator_margin_type", IntegerType, false)
        )
      )
    )
  }

  def rollup_146_UDF_inner = {
    def convertToDouble(input: Any): Double = {
      try {
        input match {
          case x: Boolean =>
            if (x) 1 else 0
          case x: Int =>
            x.toDouble
          case x: Integer =>
            x.toDouble
          case x: Long =>
            x.toDouble
          case x: BigDecimal =>
            x.toDouble
          case x: Double =>
            x
          case x: Float =>
            x.toDouble
          case "null" =>
            Double.MinValue
          case x: String =>
            x.toDouble
          case x @ _ =>
            x.toString.toDouble
        }
      } catch {
        case _: Throwable =>
          Double.MinValue
      }
    }
    udf(
      (_input: Seq[Row]) => {
        var input = _input.toArray
        var out = Row(
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          Row(
            null,
            null,
            null,
            null,
            null.asInstanceOf[String],
            null,
            null.asInstanceOf[String],
            null,
            null,
            null.asInstanceOf[String],
            null.asInstanceOf[String],
            null.asInstanceOf[String],
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null.asInstanceOf[String],
            null.asInstanceOf[String],
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null.asInstanceOf[String],
            null.asInstanceOf[String],
            null,
            null,
            null,
            null,
            null.asInstanceOf[String],
            null,
            null.asInstanceOf[String],
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null.asInstanceOf[String],
            null.asInstanceOf[String],
            null.asInstanceOf[String],
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null.asInstanceOf[String],
            null,
            null,
            null,
            null,
            null.asInstanceOf[String],
            null,
            null,
            null,
            null,
            null.asInstanceOf[String],
            null,
            null,
            null.asInstanceOf[String],
            null.asInstanceOf[String],
            null,
            null.asInstanceOf[String],
            null.asInstanceOf[String],
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            Row(null, null),
            Row(null, null),
            Row(null, null, null, null, null, null),
            Row(null.asInstanceOf[String]),
            null,
            null,
            null,
            Row(null, null),
            null,
            null.asInstanceOf[String],
            null,
            null.asInstanceOf[String],
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            Row(null),
            null,
            null,
            null.asInstanceOf[String],
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null.asInstanceOf[String],
            Row(null, null, null.asInstanceOf[String]),
            null,
            null,
            null,
            null,
            null,
            null.asInstanceOf[String],
            null.asInstanceOf[String],
            null,
            null.asInstanceOf[String],
            null,
            null,
            null,
            null.asInstanceOf[String],
            null
          ),
          null,
          Row(
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null.asInstanceOf[String],
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null.asInstanceOf[String],
            Row(null, null),
            Row(null, null),
            null,
            null,
            null,
            null.asInstanceOf[String],
            null,
            null,
            null,
            null.asInstanceOf[String],
            null.asInstanceOf[String],
            Row(null, null, null.asInstanceOf[String]),
            null,
            null,
            null
          ),
          Row(
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null.asInstanceOf[String],
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null.asInstanceOf[String],
            Row(null, null),
            Row(null, null),
            null,
            null,
            null,
            null.asInstanceOf[String],
            null,
            null,
            null,
            null.asInstanceOf[String],
            null.asInstanceOf[String],
            Row(null, null, null.asInstanceOf[String]),
            null,
            null,
            null
          ),
          null,
          Row(
            null,
            null,
            Row(null, null, null, null, null, null, null, null),
            Row(null, null, null, null, null, null, null, null),
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            Row(null, null, null),
            null,
            null,
            Row(null,
                null,
                null,
                Row(null, null, null, null, null, null, null, null),
                Row(null, null, null, null, null, null, null, null),
                null,
                null
            ),
            null,
            null,
            null
          ),
          Row(
            null,
            null,
            Row(null, null, null, null, null, null, null, null),
            Row(null, null, null, null, null, null, null, null),
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            Row(null, null, null),
            null,
            null,
            Row(null,
                null,
                null,
                Row(null, null, null, null, null, null, null, null),
                Row(null, null, null, null, null, null, null, null),
                null,
                null
            ),
            null,
            null,
            null
          )
        )
        var i = 0
        while (compareTo(i, input.length) < 0) {
          var in   = input(convertToInt(i))
          var temp = out
          if (i == 0) {
            out =
              updateIndexInRow(out,
                               0,
                               convertToLong(in.getAs[Long]("auction_id_64"))
              )
            out = updateIndexInRow(out,
                                   1,
                                   convertToLong(in.getAs[Long]("date_time"))
            )
            out =
              updateIndexInRow(out, 3, convertToInt(in.getAs[Integer]("is_dw")))
            out = updateIndexInRow(
              out,
              5,
              convertToInt(in.getAs[Integer]("buyer_member_id"))
            )
            out = updateIndexInRow(out,
                                   14,
                                   convertToInt(in.getAs[Integer]("imp_type"))
            )
            out = updateIndexInRow(out, 20, 0)
          }
          temp = out
          var log_impbus_preempt_last = Row(
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null.asInstanceOf[String],
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null.asInstanceOf[String],
            Row(null, null),
            Row(null, null),
            null,
            null,
            null,
            null.asInstanceOf[String],
            null,
            null,
            null,
            null.asInstanceOf[String],
            null.asInstanceOf[String],
            Row(null, null, null.asInstanceOf[String]),
            null,
            null,
            null
          )
          var log_impbus_preempt_first = Row(
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null.asInstanceOf[String],
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null.asInstanceOf[String],
            Row(null, null),
            Row(null, null),
            null,
            null,
            null,
            null.asInstanceOf[String],
            null,
            null,
            null,
            null.asInstanceOf[String],
            null.asInstanceOf[String],
            Row(null, null, null.asInstanceOf[String]),
            null,
            null,
            null
          )
          if (
            !temp.getAs[Row](21).isNullAt(44) && !_isnull(
              convertToLong(in.getAs[Long]("accept_timestamp"))
            ) && compareTo(convertToLong(in.getAs[Long]("accept_timestamp")),
                           temp.getAs[Row](21).getAs[Long](44)
            ) < 0
          ) {
            log_impbus_preempt_last = in
            log_impbus_preempt_first = temp.getAs[Row](21)
          } else if (temp.getAs[Row](21).isNullAt(44)) {
            log_impbus_preempt_last = in
            log_impbus_preempt_first = temp.getAs[Row](21)
          } else {
            log_impbus_preempt_last = temp.getAs[Row](21)
            log_impbus_preempt_first = in
          }
          out = updateIndexInRow(out, 0, log_impbus_preempt_last.getAs[Long](1))
          out = updateIndexInRow(out, 1, log_impbus_preempt_last.getAs[Long](0))
          out =
            updateIndexInRow(out, 3, log_impbus_preempt_last.getAs[Integer](26))
          out =
            updateIndexInRow(out, 5, log_impbus_preempt_last.getAs[Integer](9))
          out = updateIndexInRow(out,
                                 14,
                                 log_impbus_preempt_last.getAs[Integer](25)
          )
          out = updateIndexInRow(out, 20, temp.getAs[Integer](20) + 1)
          out = updateIndexInRow(out, 21, log_impbus_preempt_last)
          out = updateIndexInRow(out, 22, log_impbus_preempt_first)
          i = i + convertToInt(1)
        }
        out
      },
      StructType(
        List(
          StructField("auction_id_64",            LongType,    true),
          StructField("date_time",                LongType,    true),
          StructField("is_delivered",             IntegerType, true),
          StructField("is_dw",                    IntegerType, true),
          StructField("seller_member_id",         IntegerType, true),
          StructField("buyer_member_id",          IntegerType, true),
          StructField("member_id",                IntegerType, true),
          StructField("publisher_id",             IntegerType, true),
          StructField("site_id",                  IntegerType, true),
          StructField("tag_id",                   IntegerType, true),
          StructField("advertiser_id",            IntegerType, true),
          StructField("campaign_group_id",        IntegerType, true),
          StructField("campaign_id",              IntegerType, true),
          StructField("insertion_order_id",       IntegerType, true),
          StructField("imp_type",                 IntegerType, true),
          StructField("is_transactable",          BooleanType, true),
          StructField("is_transacted_previously", BooleanType, true),
          StructField("is_deferred_impression",   BooleanType, true),
          StructField("has_null_bid",             BooleanType, true),
          StructField(
            "log_impbus_impressions",
            StructType(
              List(
                StructField("date_time",                   LongType,    true),
                StructField("auction_id_64",               LongType,    true),
                StructField("user_id_64",                  LongType,    true),
                StructField("tag_id",                      IntegerType, true),
                StructField("ip_address",                  StringType,  true),
                StructField("venue_id",                    IntegerType, true),
                StructField("site_domain",                 StringType,  true),
                StructField("width",                       IntegerType, true),
                StructField("height",                      IntegerType, true),
                StructField("geo_country",                 StringType,  true),
                StructField("geo_region",                  StringType,  true),
                StructField("gender",                      StringType,  true),
                StructField("age",                         IntegerType, true),
                StructField("bidder_id",                   IntegerType, true),
                StructField("seller_member_id",            IntegerType, true),
                StructField("buyer_member_id",             IntegerType, true),
                StructField("creative_id",                 IntegerType, true),
                StructField("imp_blacklist_or_fraud",      IntegerType, true),
                StructField("imp_bid_on",                  IntegerType, true),
                StructField("buyer_bid",                   DoubleType,  true),
                StructField("buyer_spend",                 DoubleType,  true),
                StructField("seller_revenue",              DoubleType,  true),
                StructField("num_of_bids",                 IntegerType, true),
                StructField("ecp",                         DoubleType,  true),
                StructField("reserve_price",               DoubleType,  true),
                StructField("inv_code",                    StringType,  true),
                StructField("call_type",                   StringType,  true),
                StructField("inventory_source_id",         IntegerType, true),
                StructField("cookie_age",                  IntegerType, true),
                StructField("brand_id",                    IntegerType, true),
                StructField("cleared_direct",              IntegerType, true),
                StructField("forex_allowance",             DoubleType,  true),
                StructField("fold_position",               IntegerType, true),
                StructField("external_inv_id",             IntegerType, true),
                StructField("imp_type",                    IntegerType, true),
                StructField("is_delivered",                IntegerType, true),
                StructField("is_dw",                       IntegerType, true),
                StructField("publisher_id",                IntegerType, true),
                StructField("site_id",                     IntegerType, true),
                StructField("content_category_id",         IntegerType, true),
                StructField("datacenter_id",               IntegerType, true),
                StructField("eap",                         DoubleType,  true),
                StructField("user_tz_offset",              IntegerType, true),
                StructField("user_group_id",               IntegerType, true),
                StructField("pub_rule_id",                 IntegerType, true),
                StructField("media_type",                  IntegerType, true),
                StructField("operating_system",            IntegerType, true),
                StructField("browser",                     IntegerType, true),
                StructField("language",                    IntegerType, true),
                StructField("application_id",              StringType,  true),
                StructField("user_locale",                 StringType,  true),
                StructField("inventory_url_id",            IntegerType, true),
                StructField("audit_type",                  IntegerType, true),
                StructField("shadow_price",                DoubleType,  true),
                StructField("impbus_id",                   IntegerType, true),
                StructField("buyer_currency",              StringType,  true),
                StructField("buyer_exc hhhange_rate",      DoubleType,  true),
                StructField("seller_currency",             StringType,  true),
                StructField("seller_exchange_rate",        DoubleType,  true),
                StructField("vp_expose_domains",           IntegerType, true),
                StructField("vp_expose_categories",        IntegerType, true),
                StructField("vp_expose_pubs",              IntegerType, true),
                StructField("vp_expose_tag",               IntegerType, true),
                StructField("is_exclusive",                IntegerType, true),
                StructField("bidder_instance_id",          IntegerType, true),
                StructField("visibility_profile_id",       IntegerType, true),
                StructField("truncate_ip",                 IntegerType, true),
                StructField("device_id",                   IntegerType, true),
                StructField("carrier_id",                  IntegerType, true),
                StructField("creative_audit_status",       IntegerType, true),
                StructField("is_creative_hosted",          IntegerType, true),
                StructField("city",                        IntegerType, true),
                StructField("latitude",                    StringType,  true),
                StructField("longitude",                   StringType,  true),
                StructField("device_unique_id",            StringType,  true),
                StructField("supply_type",                 IntegerType, true),
                StructField("is_toolbar",                  IntegerType, true),
                StructField("deal_id",                     IntegerType, true),
                StructField("vp_bitmap",                   LongType,    true),
                StructField("ttl",                         IntegerType, true),
                StructField("view_detection_enabled",      IntegerType, true),
                StructField("ozone_id",                    IntegerType, true),
                StructField("is_performance",              IntegerType, true),
                StructField("sdk_version",                 StringType,  true),
                StructField("inventory_session_frequency", IntegerType, true),
                StructField("bid_price_type",              IntegerType, true),
                StructField("device_type",                 IntegerType, true),
                StructField("dma",                         IntegerType, true),
                StructField("postal",                      StringType,  true),
                StructField("package_id",                  IntegerType, true),
                StructField("spend_protection",            IntegerType, true),
                StructField("is_secure",                   IntegerType, true),
                StructField("estimated_view_rate",         DoubleType,  true),
                StructField("external_request_id",         StringType,  true),
                StructField("viewdef_definition_id_buyer_member",
                            IntegerType,
                            true
                ),
                StructField("spend_protection_pixel_id",      IntegerType, true),
                StructField("external_uid",                   StringType,  true),
                StructField("request_uuid",                   StringType,  true),
                StructField("mobile_app_instance_id",         IntegerType, true),
                StructField("traffic_source_code",            StringType,  true),
                StructField("stitch_group_id",                StringType,  true),
                StructField("deal_type",                      IntegerType, true),
                StructField("ym_floor_id",                    IntegerType, true),
                StructField("ym_bias_id",                     IntegerType, true),
                StructField("estimated_view_rate_over_total", DoubleType,  true),
                StructField("device_make_id",                 IntegerType, true),
                StructField("operating_system_family_id",     IntegerType, true),
                StructField("tag_sizes",
                            ArrayType(
                              StructType(
                                List(StructField("width",  IntegerType, true),
                                     StructField("height", IntegerType, true)
                                )
                              )
                            ),
                            true
                ),
                StructField(
                  "seller_transaction_def",
                  StructType(
                    List(StructField("transaction_event", IntegerType, true),
                         StructField("transaction_event_type_id",
                                     IntegerType,
                                     true
                         )
                    )
                  ),
                  true
                ),
                StructField(
                  "buyer_transaction_def",
                  StructType(
                    List(StructField("transaction_event", IntegerType, true),
                         StructField("transaction_event_type_id",
                                     IntegerType,
                                     true
                         )
                    )
                  ),
                  true
                ),
                StructField(
                  "predicted_video_view_info",
                  StructType(
                    List(
                      StructField("iab_view_rate_over_measured",
                                  DoubleType,
                                  true
                      ),
                      StructField("iab_view_rate_over_total", DoubleType, true),
                      StructField("predicted_100pv50pd_video_view_rate",
                                  DoubleType,
                                  true
                      ),
                      StructField(
                        "predicted_100pv50pd_video_view_rate_over_total",
                        DoubleType,
                        true
                      ),
                      StructField("video_completion_rate",  DoubleType,  true),
                      StructField("view_prediction_source", IntegerType, true)
                    )
                  ),
                  true
                ),
                StructField(
                  "auction_url",
                  StructType(List(StructField("site_url", StringType, true))),
                  true
                ),
                StructField("allowed_media_types",
                            ArrayType(IntegerType),
                            true
                ),
                StructField("is_imp_rejecter_applied", BooleanType, true),
                StructField("imp_rejecter_do_auction", BooleanType, true),
                StructField("geo_location",
                            StructType(
                              List(StructField("latitude",  FloatType, true),
                                   StructField("longitude", FloatType, true)
                              )
                            ),
                            true
                ),
                StructField("seller_bid_currency_conversion_rate",
                            DoubleType,
                            true
                ),
                StructField("seller_bid_currency_code", StringType,  true),
                StructField("is_prebid",                BooleanType, true),
                StructField("default_referrer_url",     StringType,  true),
                StructField(
                  "engagement_rates",
                  ArrayType(
                    StructType(
                      List(
                        StructField("engagement_rate_type", IntegerType, true),
                        StructField("rate",                 DoubleType,  true),
                        StructField("engagement_rate_type_id",
                                    IntegerType,
                                    true
                        )
                      )
                    )
                  ),
                  true
                ),
                StructField("fx_rate_snapshot_id",     IntegerType, true),
                StructField("payment_type",            IntegerType, true),
                StructField("apply_cost_on_default",   IntegerType, true),
                StructField("media_buy_cost",          DoubleType,  true),
                StructField("media_buy_rev_share_pct", DoubleType,  true),
                StructField("auction_duration_ms",     IntegerType, true),
                StructField("expected_events",         IntegerType, true),
                StructField(
                  "anonymized_user_info",
                  StructType(List(StructField("user_id", BinaryType, true))),
                  true
                ),
                StructField("region_id",                 IntegerType, true),
                StructField("media_company_id",          IntegerType, true),
                StructField("gdpr_consent_cookie",       StringType,  true),
                StructField("subject_to_gdpr",           BooleanType, true),
                StructField("browser_code_id",           IntegerType, true),
                StructField("is_prebid_server_included", IntegerType, true),
                StructField("seat_id",                   IntegerType, true),
                StructField("uid_source",                IntegerType, true),
                StructField("is_whiteops_scanned",       BooleanType, true),
                StructField("pred_info",                 IntegerType, true),
                StructField("crossdevice_groups",
                            ArrayType(
                              StructType(
                                List(StructField("graph_id", IntegerType, true),
                                     StructField("group_id", LongType,    true)
                                )
                              )
                            ),
                            true
                ),
                StructField("is_amp",               BooleanType, true),
                StructField("hb_source",            IntegerType, true),
                StructField("external_campaign_id", StringType,  true),
                StructField(
                  "log_product_ads",
                  StructType(
                    List(
                      StructField("product_feed_id", IntegerType, true),
                      StructField("item_selection_strategy_id",
                                  IntegerType,
                                  true
                      ),
                      StructField("product_uuid", StringType, true)
                    )
                  ),
                  true
                ),
                StructField("ss_native_assembly_enabled", BooleanType, true),
                StructField("emp",                        DoubleType,  true),
                StructField(
                  "personal_identifiers",
                  ArrayType(
                    StructType(
                      List(StructField("identity_type",  IntegerType, true),
                           StructField("identity_value", StringType,  true)
                      )
                    )
                  ),
                  true
                ),
                StructField(
                  "personal_identifiers_experimental",
                  ArrayType(
                    StructType(
                      List(StructField("identity_type",  IntegerType, true),
                           StructField("identity_value", StringType,  true)
                      )
                    )
                  ),
                  true
                ),
                StructField("postal_code_ext_id",        IntegerType, true),
                StructField("hashed_ip",                 StringType,  true),
                StructField("external_deal_code",        StringType,  true),
                StructField("creative_duration",         IntegerType, true),
                StructField("openrtb_req_subdomain",     StringType,  true),
                StructField("creative_media_subtype_id", IntegerType, true),
                StructField("is_private_auction",        BooleanType, true),
                StructField("private_auction_eligible",  BooleanType, true),
                StructField("client_request_id",         StringType,  true),
                StructField("chrome_traffic_label",      IntegerType, true)
              )
            ),
            true
          ),
          StructField("log_impbus_preempt_count", IntegerType, true),
          StructField(
            "log_impbus_preempt",
            StructType(
              List(
                StructField("date_time",              LongType,    true),
                StructField("auction_id_64",          LongType,    true),
                StructField("imp_transacted",         IntegerType, true),
                StructField("buyer_spend",            DoubleType,  true),
                StructField("seller_revenue",         DoubleType,  true),
                StructField("bidder_fees",            DoubleType,  true),
                StructField("instance_id",            IntegerType, true),
                StructField("fold_position",          IntegerType, true),
                StructField("seller_deduction",       DoubleType,  true),
                StructField("buyer_member_id",        IntegerType, true),
                StructField("creative_id",            IntegerType, true),
                StructField("cleared_direct",         IntegerType, true),
                StructField("buyer_currency",         StringType,  true),
                StructField("buyer_exchange_rate",    DoubleType,  true),
                StructField("width",                  IntegerType, true),
                StructField("height",                 IntegerType, true),
                StructField("brand_id",               IntegerType, true),
                StructField("creative_audit_status",  IntegerType, true),
                StructField("is_creative_hosted",     IntegerType, true),
                StructField("vp_expose_domains",      IntegerType, true),
                StructField("vp_expose_categories",   IntegerType, true),
                StructField("vp_expose_pubs",         IntegerType, true),
                StructField("vp_expose_tag",          IntegerType, true),
                StructField("bidder_id",              IntegerType, true),
                StructField("deal_id",                IntegerType, true),
                StructField("imp_type",               IntegerType, true),
                StructField("is_dw",                  IntegerType, true),
                StructField("vp_bitmap",              LongType,    true),
                StructField("ttl",                    IntegerType, true),
                StructField("view_detection_enabled", IntegerType, true),
                StructField("media_type",             IntegerType, true),
                StructField("auction_timestamp",      LongType,    true),
                StructField("spend_protection",       IntegerType, true),
                StructField("viewdef_definition_id_buyer_member",
                            IntegerType,
                            true
                ),
                StructField("deal_type",                 IntegerType, true),
                StructField("ym_floor_id",               IntegerType, true),
                StructField("ym_bias_id",                IntegerType, true),
                StructField("bid_price_type",            IntegerType, true),
                StructField("spend_protection_pixel_id", IntegerType, true),
                StructField("ip_address",                StringType,  true),
                StructField(
                  "buyer_transaction_def",
                  StructType(
                    List(StructField("transaction_event", IntegerType, true),
                         StructField("transaction_event_type_id",
                                     IntegerType,
                                     true
                         )
                    )
                  ),
                  true
                ),
                StructField(
                  "seller_transaction_def",
                  StructType(
                    List(StructField("transaction_event", IntegerType, true),
                         StructField("transaction_event_type_id",
                                     IntegerType,
                                     true
                         )
                    )
                  ),
                  true
                ),
                StructField("buyer_bid",            DoubleType,  true),
                StructField("expected_events",      IntegerType, true),
                StructField("accept_timestamp",     LongType,    true),
                StructField("external_creative_id", StringType,  true),
                StructField("seat_id",              IntegerType, true),
                StructField("is_prebid_server",     BooleanType, true),
                StructField("curated_deal_id",      IntegerType, true),
                StructField("external_campaign_id", StringType,  true),
                StructField("trust_id",             StringType,  true),
                StructField(
                  "log_product_ads",
                  StructType(
                    List(
                      StructField("product_feed_id", IntegerType, true),
                      StructField("item_selection_strategy_id",
                                  IntegerType,
                                  true
                      ),
                      StructField("product_uuid", StringType, true)
                    )
                  ),
                  true
                ),
                StructField("external_bidrequest_id",     LongType,    true),
                StructField("external_bidrequest_imp_id", LongType,    true),
                StructField("creative_media_subtype_id",  IntegerType, true)
              )
            ),
            true
          ),
          StructField(
            "log_impbus_preempt_dup",
            StructType(
              List(
                StructField("date_time",              LongType,    true),
                StructField("auction_id_64",          LongType,    true),
                StructField("imp_transacted",         IntegerType, true),
                StructField("buyer_spend",            DoubleType,  true),
                StructField("seller_revenue",         DoubleType,  true),
                StructField("bidder_fees",            DoubleType,  true),
                StructField("instance_id",            IntegerType, true),
                StructField("fold_position",          IntegerType, true),
                StructField("seller_deduction",       DoubleType,  true),
                StructField("buyer_member_id",        IntegerType, true),
                StructField("creative_id",            IntegerType, true),
                StructField("cleared_direct",         IntegerType, true),
                StructField("buyer_currency",         StringType,  true),
                StructField("buyer_exchange_rate",    DoubleType,  true),
                StructField("width",                  IntegerType, true),
                StructField("height",                 IntegerType, true),
                StructField("brand_id",               IntegerType, true),
                StructField("creative_audit_status",  IntegerType, true),
                StructField("is_creative_hosted",     IntegerType, true),
                StructField("vp_expose_domains",      IntegerType, true),
                StructField("vp_expose_categories",   IntegerType, true),
                StructField("vp_expose_pubs",         IntegerType, true),
                StructField("vp_expose_tag",          IntegerType, true),
                StructField("bidder_id",              IntegerType, true),
                StructField("deal_id",                IntegerType, true),
                StructField("imp_type",               IntegerType, true),
                StructField("is_dw",                  IntegerType, true),
                StructField("vp_bitmap",              LongType,    true),
                StructField("ttl",                    IntegerType, true),
                StructField("view_detection_enabled", IntegerType, true),
                StructField("media_type",             IntegerType, true),
                StructField("auction_timestamp",      LongType,    true),
                StructField("spend_protection",       IntegerType, true),
                StructField("viewdef_definition_id_buyer_member",
                            IntegerType,
                            true
                ),
                StructField("deal_type",                 IntegerType, true),
                StructField("ym_floor_id",               IntegerType, true),
                StructField("ym_bias_id",                IntegerType, true),
                StructField("bid_price_type",            IntegerType, true),
                StructField("spend_protection_pixel_id", IntegerType, true),
                StructField("ip_address",                StringType,  true),
                StructField(
                  "buyer_transaction_def",
                  StructType(
                    List(StructField("transaction_event", IntegerType, true),
                         StructField("transaction_event_type_id",
                                     IntegerType,
                                     true
                         )
                    )
                  ),
                  true
                ),
                StructField(
                  "seller_transaction_def",
                  StructType(
                    List(StructField("transaction_event", IntegerType, true),
                         StructField("transaction_event_type_id",
                                     IntegerType,
                                     true
                         )
                    )
                  ),
                  true
                ),
                StructField("buyer_bid",            DoubleType,  true),
                StructField("expected_events",      IntegerType, true),
                StructField("accept_timestamp",     LongType,    true),
                StructField("external_creative_id", StringType,  true),
                StructField("seat_id",              IntegerType, true),
                StructField("is_prebid_server",     BooleanType, true),
                StructField("curated_deal_id",      IntegerType, true),
                StructField("external_campaign_id", StringType,  true),
                StructField("trust_id",             StringType,  true),
                StructField(
                  "log_product_ads",
                  StructType(
                    List(
                      StructField("product_feed_id", IntegerType, true),
                      StructField("item_selection_strategy_id",
                                  IntegerType,
                                  true
                      ),
                      StructField("product_uuid", StringType, true)
                    )
                  ),
                  true
                ),
                StructField("external_bidrequest_id",     LongType,    true),
                StructField("external_bidrequest_imp_id", LongType,    true),
                StructField("creative_media_subtype_id",  IntegerType, true)
              )
            ),
            true
          ),
          StructField("log_impbus_impressions_pricing_count",
                      IntegerType,
                      true
          ),
          StructField(
            "log_impbus_impressions_pricing",
            StructType(
              List(
                StructField("date_time",     LongType, true),
                StructField("auction_id_64", LongType, true),
                StructField(
                  "buyer_charges",
                  StructType(
                    List(
                      StructField("rate_card_id", IntegerType, true),
                      StructField("member_id",    IntegerType, true),
                      StructField("is_dw",        BooleanType, true),
                      StructField(
                        "pricing_terms",
                        ArrayType(
                          StructType(
                            List(
                              StructField("term_id",      IntegerType, true),
                              StructField("amount",       DoubleType,  true),
                              StructField("rate",         DoubleType,  true),
                              StructField("is_deduction", BooleanType, true),
                              StructField("is_media_cost_dependent",
                                          BooleanType,
                                          true
                              ),
                              StructField("data_member_id", IntegerType, true)
                            )
                          )
                        ),
                        true
                      ),
                      StructField("fx_margin_rate_id",      IntegerType, true),
                      StructField("marketplace_owner_id",   IntegerType, true),
                      StructField("virtual_marketplace_id", IntegerType, true),
                      StructField("amino_enabled",          BooleanType, true)
                    )
                  ),
                  true
                ),
                StructField(
                  "seller_charges",
                  StructType(
                    List(
                      StructField("rate_card_id", IntegerType, true),
                      StructField("member_id",    IntegerType, true),
                      StructField("is_dw",        BooleanType, true),
                      StructField(
                        "pricing_terms",
                        ArrayType(
                          StructType(
                            List(
                              StructField("term_id",      IntegerType, true),
                              StructField("amount",       DoubleType,  true),
                              StructField("rate",         DoubleType,  true),
                              StructField("is_deduction", BooleanType, true),
                              StructField("is_media_cost_dependent",
                                          BooleanType,
                                          true
                              ),
                              StructField("data_member_id", IntegerType, true)
                            )
                          )
                        ),
                        true
                      ),
                      StructField("fx_margin_rate_id",      IntegerType, true),
                      StructField("marketplace_owner_id",   IntegerType, true),
                      StructField("virtual_marketplace_id", IntegerType, true),
                      StructField("amino_enabled",          BooleanType, true)
                    )
                  ),
                  true
                ),
                StructField("buyer_spend",                 DoubleType,  true),
                StructField("seller_revenue",              DoubleType,  true),
                StructField("rate_card_auction_type",      IntegerType, true),
                StructField("rate_card_media_type",        IntegerType, true),
                StructField("direct_clear",                BooleanType, true),
                StructField("auction_timestamp",           LongType,    true),
                StructField("instance_id",                 IntegerType, true),
                StructField("two_phase_reduction_applied", BooleanType, true),
                StructField("trade_agreement_id",          IntegerType, true),
                StructField("log_timestamp",               LongType,    true),
                StructField(
                  "trade_agreement_info",
                  StructType(
                    List(
                      StructField("applied_term_id",   IntegerType, true),
                      StructField("applied_term_type", IntegerType, true),
                      StructField("targeted_term_ids",
                                  ArrayType(IntegerType),
                                  true
                      )
                    )
                  ),
                  true
                ),
                StructField("is_buy_it_now",   BooleanType, true),
                StructField("net_buyer_spend", DoubleType,  true),
                StructField(
                  "impression_event_pricing",
                  StructType(
                    List(
                      StructField("gross_payment_value_microcents",
                                  LongType,
                                  true
                      ),
                      StructField("net_payment_value_microcents",
                                  LongType,
                                  true
                      ),
                      StructField("seller_revenue_microcents", LongType, true),
                      StructField(
                        "buyer_charges",
                        StructType(
                          List(
                            StructField("rate_card_id", IntegerType, true),
                            StructField("member_id",    IntegerType, true),
                            StructField("is_dw",        BooleanType, true),
                            StructField(
                              "pricing_terms",
                              ArrayType(
                                StructType(
                                  List(
                                    StructField("term_id", IntegerType, true),
                                    StructField("amount",  DoubleType,  true),
                                    StructField("rate",    DoubleType,  true),
                                    StructField("is_deduction",
                                                BooleanType,
                                                true
                                    ),
                                    StructField("is_media_cost_dependent",
                                                BooleanType,
                                                true
                                    ),
                                    StructField("data_member_id",
                                                IntegerType,
                                                true
                                    )
                                  )
                                )
                              ),
                              true
                            ),
                            StructField("fx_margin_rate_id", IntegerType, true),
                            StructField("marketplace_owner_id",
                                        IntegerType,
                                        true
                            ),
                            StructField("virtual_marketplace_id",
                                        IntegerType,
                                        true
                            ),
                            StructField("amino_enabled", BooleanType, true)
                          )
                        ),
                        true
                      ),
                      StructField(
                        "seller_charges",
                        StructType(
                          List(
                            StructField("rate_card_id", IntegerType, true),
                            StructField("member_id",    IntegerType, true),
                            StructField("is_dw",        BooleanType, true),
                            StructField(
                              "pricing_terms",
                              ArrayType(
                                StructType(
                                  List(
                                    StructField("term_id", IntegerType, true),
                                    StructField("amount",  DoubleType,  true),
                                    StructField("rate",    DoubleType,  true),
                                    StructField("is_deduction",
                                                BooleanType,
                                                true
                                    ),
                                    StructField("is_media_cost_dependent",
                                                BooleanType,
                                                true
                                    ),
                                    StructField("data_member_id",
                                                IntegerType,
                                                true
                                    )
                                  )
                                )
                              ),
                              true
                            ),
                            StructField("fx_margin_rate_id", IntegerType, true),
                            StructField("marketplace_owner_id",
                                        IntegerType,
                                        true
                            ),
                            StructField("virtual_marketplace_id",
                                        IntegerType,
                                        true
                            ),
                            StructField("amino_enabled", BooleanType, true)
                          )
                        ),
                        true
                      ),
                      StructField("buyer_transacted",  BooleanType, true),
                      StructField("seller_transacted", BooleanType, true)
                    )
                  ),
                  true
                ),
                StructField("counterparty_ruleset_type", IntegerType, true),
                StructField("estimated_audience_imps",   FloatType,   true),
                StructField("audience_imps",             FloatType,   true)
              )
            ),
            true
          ),
          StructField(
            "log_impbus_impressions_pricing_dup",
            StructType(
              List(
                StructField("date_time",     LongType, true),
                StructField("auction_id_64", LongType, true),
                StructField(
                  "buyer_charges",
                  StructType(
                    List(
                      StructField("rate_card_id", IntegerType, true),
                      StructField("member_id",    IntegerType, true),
                      StructField("is_dw",        BooleanType, true),
                      StructField(
                        "pricing_terms",
                        ArrayType(
                          StructType(
                            List(
                              StructField("term_id",      IntegerType, true),
                              StructField("amount",       DoubleType,  true),
                              StructField("rate",         DoubleType,  true),
                              StructField("is_deduction", BooleanType, true),
                              StructField("is_media_cost_dependent",
                                          BooleanType,
                                          true
                              ),
                              StructField("data_member_id", IntegerType, true)
                            )
                          )
                        ),
                        true
                      ),
                      StructField("fx_margin_rate_id",      IntegerType, true),
                      StructField("marketplace_owner_id",   IntegerType, true),
                      StructField("virtual_marketplace_id", IntegerType, true),
                      StructField("amino_enabled",          BooleanType, true)
                    )
                  ),
                  true
                ),
                StructField(
                  "seller_charges",
                  StructType(
                    List(
                      StructField("rate_card_id", IntegerType, true),
                      StructField("member_id",    IntegerType, true),
                      StructField("is_dw",        BooleanType, true),
                      StructField(
                        "pricing_terms",
                        ArrayType(
                          StructType(
                            List(
                              StructField("term_id",      IntegerType, true),
                              StructField("amount",       DoubleType,  true),
                              StructField("rate",         DoubleType,  true),
                              StructField("is_deduction", BooleanType, true),
                              StructField("is_media_cost_dependent",
                                          BooleanType,
                                          true
                              ),
                              StructField("data_member_id", IntegerType, true)
                            )
                          )
                        ),
                        true
                      ),
                      StructField("fx_margin_rate_id",      IntegerType, true),
                      StructField("marketplace_owner_id",   IntegerType, true),
                      StructField("virtual_marketplace_id", IntegerType, true),
                      StructField("amino_enabled",          BooleanType, true)
                    )
                  ),
                  true
                ),
                StructField("buyer_spend",                 DoubleType,  true),
                StructField("seller_revenue",              DoubleType,  true),
                StructField("rate_card_auction_type",      IntegerType, true),
                StructField("rate_card_media_type",        IntegerType, true),
                StructField("direct_clear",                BooleanType, true),
                StructField("auction_timestamp",           LongType,    true),
                StructField("instance_id",                 IntegerType, true),
                StructField("two_phase_reduction_applied", BooleanType, true),
                StructField("trade_agreement_id",          IntegerType, true),
                StructField("log_timestamp",               LongType,    true),
                StructField(
                  "trade_agreement_info",
                  StructType(
                    List(
                      StructField("applied_term_id",   IntegerType, true),
                      StructField("applied_term_type", IntegerType, true),
                      StructField("targeted_term_ids",
                                  ArrayType(IntegerType),
                                  true
                      )
                    )
                  ),
                  true
                ),
                StructField("is_buy_it_now",   BooleanType, true),
                StructField("net_buyer_spend", DoubleType,  true),
                StructField(
                  "impression_event_pricing",
                  StructType(
                    List(
                      StructField("gross_payment_value_microcents",
                                  LongType,
                                  true
                      ),
                      StructField("net_payment_value_microcents",
                                  LongType,
                                  true
                      ),
                      StructField("seller_revenue_microcents", LongType, true),
                      StructField(
                        "buyer_charges",
                        StructType(
                          List(
                            StructField("rate_card_id", IntegerType, true),
                            StructField("member_id",    IntegerType, true),
                            StructField("is_dw",        BooleanType, true),
                            StructField(
                              "pricing_terms",
                              ArrayType(
                                StructType(
                                  List(
                                    StructField("term_id", IntegerType, true),
                                    StructField("amount",  DoubleType,  true),
                                    StructField("rate",    DoubleType,  true),
                                    StructField("is_deduction",
                                                BooleanType,
                                                true
                                    ),
                                    StructField("is_media_cost_dependent",
                                                BooleanType,
                                                true
                                    ),
                                    StructField("data_member_id",
                                                IntegerType,
                                                true
                                    )
                                  )
                                )
                              ),
                              true
                            ),
                            StructField("fx_margin_rate_id", IntegerType, true),
                            StructField("marketplace_owner_id",
                                        IntegerType,
                                        true
                            ),
                            StructField("virtual_marketplace_id",
                                        IntegerType,
                                        true
                            ),
                            StructField("amino_enabled", BooleanType, true)
                          )
                        ),
                        true
                      ),
                      StructField(
                        "seller_charges",
                        StructType(
                          List(
                            StructField("rate_card_id", IntegerType, true),
                            StructField("member_id",    IntegerType, true),
                            StructField("is_dw",        BooleanType, true),
                            StructField(
                              "pricing_terms",
                              ArrayType(
                                StructType(
                                  List(
                                    StructField("term_id", IntegerType, true),
                                    StructField("amount",  DoubleType,  true),
                                    StructField("rate",    DoubleType,  true),
                                    StructField("is_deduction",
                                                BooleanType,
                                                true
                                    ),
                                    StructField("is_media_cost_dependent",
                                                BooleanType,
                                                true
                                    ),
                                    StructField("data_member_id",
                                                IntegerType,
                                                true
                                    )
                                  )
                                )
                              ),
                              true
                            ),
                            StructField("fx_margin_rate_id", IntegerType, true),
                            StructField("marketplace_owner_id",
                                        IntegerType,
                                        true
                            ),
                            StructField("virtual_marketplace_id",
                                        IntegerType,
                                        true
                            ),
                            StructField("amino_enabled", BooleanType, true)
                          )
                        ),
                        true
                      ),
                      StructField("buyer_transacted",  BooleanType, true),
                      StructField("seller_transacted", BooleanType, true)
                    )
                  ),
                  true
                ),
                StructField("counterparty_ruleset_type", IntegerType, true),
                StructField("estimated_audience_imps",   FloatType,   true),
                StructField("audience_imps",             FloatType,   true)
              )
            ),
            true
          )
        )
      )
    )
  }

  def rollup_150_UDF_inner = {
    def convertToDouble(input: Any): Double = {
      try {
        input match {
          case x: Boolean =>
            if (x) 1 else 0
          case x: Int =>
            x.toDouble
          case x: Integer =>
            x.toDouble
          case x: Long =>
            x.toDouble
          case x: BigDecimal =>
            x.toDouble
          case x: Double =>
            x
          case x: Float =>
            x.toDouble
          case "null" =>
            Double.MinValue
          case x: String =>
            x.toDouble
          case x @ _ =>
            x.toString.toDouble
        }
      } catch {
        case _: Throwable =>
          Double.MinValue
      }
    }
    udf(
      (_input: Seq[Row]) => {
        var input = _input.toArray
        var out = Row(
          null,
          null,
          null,
          null,
          null,
          null.asInstanceOf[String],
          null,
          null,
          null,
          null,
          null,
          null,
          null.asInstanceOf[String],
          null,
          null,
          null.asInstanceOf[String],
          null.asInstanceOf[String],
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null.asInstanceOf[String],
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          Row(null, null),
          Row(null),
          null
        )
        var i = 0
        while (compareTo(i, input.length) < 0) {
          var in   = input(convertToInt(i))
          var temp = out
          if (i == 0) temp = in
          temp = out
          var temp_view_result           = 0
          var temp_viewdef_definition_id = 0
          var temp_viewdef_view_result   = 0
          var in_view_result             = 0
          var in_viewdef_definition_id   = 0
          var in_viewdef_view_result     = 0
          var view_result                = 0
          var viewdef_definition_id      = 0
          var viewdef_view_result        = 0
          temp_view_result =
            if (temp.getAs[Integer](3) == null) 0 else temp.getAs[Integer](3)
          temp_viewdef_definition_id =
            if (temp.getAs[Integer](6) == null) 0 else temp.getAs[Integer](6)
          temp_viewdef_view_result =
            if (temp.getAs[Integer](7) == null) 0 else temp.getAs[Integer](7)
          in_view_result =
            if (convertToInt(in.getAs[Integer]("view_result")) == null) 0
            else convertToInt(in.getAs[Integer]("view_result"))
          in_viewdef_definition_id =
            if (
              convertToInt(in.getAs[Integer]("viewdef_definition_id")) == null
            ) 0
            else convertToInt(in.getAs[Integer]("viewdef_definition_id"))
          in_viewdef_view_result =
            if (convertToInt(in.getAs[Integer]("viewdef_view_result")) == null)
              0
            else convertToInt(in.getAs[Integer]("viewdef_view_result"))
          if (
            temp_view_result == in_view_result || in_view_result == 0 || temp_view_result == 1
          ) view_result = temp_view_result
          else if (temp_view_result == 0 || in_view_result == 1)
            view_result = in_view_result
          else view_result = 3
          if (
            compareTo(temp_viewdef_definition_id, in_viewdef_definition_id) > 0
          ) {
            viewdef_definition_id = temp_viewdef_definition_id
            viewdef_view_result = temp_viewdef_view_result
          } else if (
            compareTo(temp_viewdef_definition_id, in_viewdef_definition_id) < 0
          ) {
            viewdef_definition_id = in_viewdef_definition_id
            viewdef_view_result = in_viewdef_view_result
          } else if (compareTo(temp_viewdef_definition_id, 0) > 0) {
            viewdef_definition_id = temp_viewdef_definition_id
            if (
              temp_viewdef_view_result == in_viewdef_view_result || in_viewdef_view_result == 0 || temp_viewdef_view_result == 1
            ) viewdef_view_result = temp_viewdef_view_result
            else if (
              temp_viewdef_view_result == 0 || in_viewdef_view_result == 1
            ) viewdef_view_result = in_viewdef_view_result
            else viewdef_view_result = 3
          } else {
            viewdef_definition_id = 0
            viewdef_view_result = 0
          }
          out =
            updateIndexInRow(out, 0, convertToLong(in.getAs[Long]("date_time")))
          out = updateIndexInRow(out,
                                 1,
                                 convertToLong(in.getAs[Long]("auction_id_64"))
          )
          out = updateIndexInRow(out,
                                 2,
                                 convertToLong(in.getAs[Long]("user_id_64"))
          )
          out = updateIndexInRow(out,
                                 3,
                                 convertToInt(in.getAs[Integer]("view_result"))
          )
          out = updateIndexInRow(out, 4, convertToInt(in.getAs[Integer]("ttl")))
          out = updateIndexInRow(out, 5, in.getAs[String]("view_data"))
          out = updateIndexInRow(
            out,
            6,
            convertToInt(in.getAs[Integer]("viewdef_definition_id"))
          )
          out = updateIndexInRow(
            out,
            7,
            convertToInt(in.getAs[Integer]("viewdef_view_result"))
          )
          out = updateIndexInRow(
            out,
            8,
            convertToInt(in.getAs[Integer]("view_not_measurable_type"))
          )
          out = updateIndexInRow(
            out,
            9,
            convertToInt(in.getAs[Integer]("view_not_visible_type"))
          )
          out =
            updateIndexInRow(out,
                             10,
                             convertToInt(in.getAs[Integer]("view_frame_type"))
            )
          out = updateIndexInRow(
            out,
            11,
            convertToInt(in.getAs[Integer]("view_script_version"))
          )
          out = updateIndexInRow(out, 12, in.getAs[String]("view_tag_version"))
          out = updateIndexInRow(
            out,
            13,
            convertToInt(in.getAs[Integer]("view_screen_width"))
          )
          out = updateIndexInRow(
            out,
            14,
            convertToInt(in.getAs[Integer]("view_screen_height"))
          )
          out = updateIndexInRow(out, 15, in.getAs[String]("view_js_browser"))
          out = updateIndexInRow(out, 16, in.getAs[String]("view_js_platform"))
          out =
            updateIndexInRow(out,
                             17,
                             convertToInt(in.getAs[Integer]("view_banner_left"))
            )
          out =
            updateIndexInRow(out,
                             18,
                             convertToInt(in.getAs[Integer]("view_banner_top"))
            )
          out = updateIndexInRow(
            out,
            19,
            convertToInt(in.getAs[Integer]("view_banner_width"))
          )
          out = updateIndexInRow(
            out,
            20,
            convertToInt(in.getAs[Integer]("view_banner_height"))
          )
          out = updateIndexInRow(out,
                                 21,
                                 in.getAs[Double]("view_tracking_duration")
          )
          out =
            updateIndexInRow(out, 22, in.getAs[Double]("view_page_duration"))
          out =
            updateIndexInRow(out,     23, in.getAs[Double]("view_usage_duration"))
          out = updateIndexInRow(out, 24, in.getAs[Double]("view_surface"))
          out = updateIndexInRow(out, 25, in.getAs[String]("view_js_message"))
          out = updateIndexInRow(
            out,
            26,
            convertToInt(in.getAs[Integer]("view_player_width"))
          )
          out = updateIndexInRow(
            out,
            27,
            convertToInt(in.getAs[Integer]("view_player_height"))
          )
          out = updateIndexInRow(out, 28, in.getAs[Double]("view_iab_duration"))
          out = updateIndexInRow(
            out,
            29,
            convertToInt(in.getAs[Integer]("view_iab_inview_count"))
          )
          out =
            updateIndexInRow(out, 30, in.getAs[Double]("view_duration_gt_0pct"))
          out = updateIndexInRow(out,
                                 31,
                                 in.getAs[Double]("view_duration_gt_25pct")
          )
          out = updateIndexInRow(out,
                                 32,
                                 in.getAs[Double]("view_duration_gt_50pct")
          )
          out = updateIndexInRow(out,
                                 33,
                                 in.getAs[Double]("view_duration_gt_75pct")
          )
          out = updateIndexInRow(out,
                                 34,
                                 in.getAs[Double]("view_duration_eq_100pct")
          )
          out =
            updateIndexInRow(out,
                             35,
                             convertToLong(in.getAs[Long]("auction_timestamp"))
            )
          out = updateIndexInRow(
            out,
            36,
            convertToInt(in.getAs[Integer]("view_has_banner_left"))
          )
          out = updateIndexInRow(
            out,
            37,
            convertToInt(in.getAs[Integer]("view_has_banner_top"))
          )
          out = updateIndexInRow(
            out,
            38,
            convertToInt(in.getAs[Integer]("view_mouse_position_final_x"))
          )
          out = updateIndexInRow(
            out,
            39,
            convertToInt(in.getAs[Integer]("view_mouse_position_final_y"))
          )
          out = updateIndexInRow(
            out,
            40,
            convertToInt(in.getAs[Integer]("view_has_mouse_position_final"))
          )
          out = updateIndexInRow(
            out,
            41,
            convertToInt(in.getAs[Integer]("view_mouse_position_initial_x"))
          )
          out = updateIndexInRow(
            out,
            42,
            convertToInt(in.getAs[Integer]("view_mouse_position_initial_y"))
          )
          out = updateIndexInRow(
            out,
            43,
            convertToInt(in.getAs[Integer]("view_has_mouse_position_initial"))
          )
          out = updateIndexInRow(
            out,
            44,
            convertToInt(in.getAs[Integer]("view_mouse_position_page_x"))
          )
          out = updateIndexInRow(
            out,
            45,
            convertToInt(in.getAs[Integer]("view_mouse_position_page_y"))
          )
          out = updateIndexInRow(
            out,
            46,
            convertToInt(in.getAs[Integer]("view_has_mouse_position_page"))
          )
          out = updateIndexInRow(
            out,
            47,
            convertToInt(in.getAs[Integer]("view_mouse_position_timeout_x"))
          )
          out = updateIndexInRow(
            out,
            48,
            convertToInt(in.getAs[Integer]("view_mouse_position_timeout_y"))
          )
          out = updateIndexInRow(
            out,
            49,
            convertToInt(in.getAs[Integer]("view_has_mouse_position_timeout"))
          )
          out =
            updateIndexInRow(out,
                             50,
                             convertToLong(in.getAs[Long]("view_session_id"))
            )
          out = updateIndexInRow(out, 51, in.getAs[Row]("view_video"))
          out = updateIndexInRow(out, 52, in.getAs[Row]("anonymized_user_info"))
          out = updateIndexInRow(out, 53, in.getAs[Boolean]("is_deferred"))
          i = i + convertToInt(1)
        }
        out
      },
      StructType(
        List(
          StructField("date_time",                       LongType,    true),
          StructField("auction_id_64",                   LongType,    true),
          StructField("user_id_64",                      LongType,    true),
          StructField("view_result",                     IntegerType, true),
          StructField("ttl",                             IntegerType, true),
          StructField("view_data",                       StringType,  true),
          StructField("viewdef_definition_id",           IntegerType, true),
          StructField("viewdef_view_result",             IntegerType, true),
          StructField("view_not_measurable_type",        IntegerType, true),
          StructField("view_not_visible_type",           IntegerType, true),
          StructField("view_frame_type",                 IntegerType, true),
          StructField("view_script_version",             IntegerType, true),
          StructField("view_tag_version",                StringType,  true),
          StructField("view_screen_width",               IntegerType, true),
          StructField("view_screen_height",              IntegerType, true),
          StructField("view_js_browser",                 StringType,  true),
          StructField("view_js_platform",                StringType,  true),
          StructField("view_banner_left",                IntegerType, true),
          StructField("view_banner_top",                 IntegerType, true),
          StructField("view_banner_width",               IntegerType, true),
          StructField("view_banner_height",              IntegerType, true),
          StructField("view_tracking_duration",          DoubleType,  true),
          StructField("view_page_duration",              DoubleType,  true),
          StructField("view_usage_duration",             DoubleType,  true),
          StructField("view_surface",                    DoubleType,  true),
          StructField("view_js_message",                 StringType,  true),
          StructField("view_player_width",               IntegerType, true),
          StructField("view_player_height",              IntegerType, true),
          StructField("view_iab_duration",               DoubleType,  true),
          StructField("view_iab_inview_count",           IntegerType, true),
          StructField("view_duration_gt_0pct",           DoubleType,  true),
          StructField("view_duration_gt_25pct",          DoubleType,  true),
          StructField("view_duration_gt_50pct",          DoubleType,  true),
          StructField("view_duration_gt_75pct",          DoubleType,  true),
          StructField("view_duration_eq_100pct",         DoubleType,  true),
          StructField("auction_timestamp",               LongType,    true),
          StructField("view_has_banner_left",            IntegerType, true),
          StructField("view_has_banner_top",             IntegerType, true),
          StructField("view_mouse_position_final_x",     IntegerType, true),
          StructField("view_mouse_position_final_y",     IntegerType, true),
          StructField("view_has_mouse_position_final",   IntegerType, true),
          StructField("view_mouse_position_initial_x",   IntegerType, true),
          StructField("view_mouse_position_initial_y",   IntegerType, true),
          StructField("view_has_mouse_position_initial", IntegerType, true),
          StructField("view_mouse_position_page_x",      IntegerType, true),
          StructField("view_mouse_position_page_y",      IntegerType, true),
          StructField("view_has_mouse_position_page",    IntegerType, true),
          StructField("view_mouse_position_timeout_x",   IntegerType, true),
          StructField("view_mouse_position_timeout_y",   IntegerType, true),
          StructField("view_has_mouse_position_timeout", IntegerType, true),
          StructField("view_session_id",                 LongType,    true),
          StructField(
            "view_video",
            StructType(
              List(
                StructField("view_audio_duration_eq_100pct", DoubleType, true),
                StructField("view_creative_duration",        DoubleType, true)
              )
            ),
            true
          ),
          StructField(
            "anonymized_user_info",
            StructType(List(StructField("user_id", BinaryType, true))),
            true
          ),
          StructField("is_deferred", BooleanType, true)
        )
      )
    )
  }

  def temp6618434_UDF = {
    udf(
      (_in_pricing_terms: Seq[Row], _l_pricing_terms: Seq[Row], _i: Integer) =>
        {
          var l_pricing_terms =
            if (_l_pricing_terms == null) null.asInstanceOf[Array[Row]]
            else _l_pricing_terms.toArray
          var i = _i
          var in_pricing_terms =
            if (_in_pricing_terms == null) null.asInstanceOf[Array[Row]]
            else _in_pricing_terms.toArray
          while (compareTo(i, l_pricing_terms.length) < 0) {
            if (
              !_isnull(
                in_pricing_terms(convertToInt(i)).getAs[Double]("amount")
              ) && !_isnull(
                in_pricing_terms(convertToInt(i))
                  .getAs[Boolean]("is_media_cost_dependent")
              ) && in_pricing_terms(convertToInt(i)).getAs[Boolean](
                "is_media_cost_dependent"
              ) == convertToBoolean(1)
            )
              l_pricing_terms(i) = Row(
                l_pricing_terms(convertToInt(i)).getAs[Integer]("term_id"),
                0,
                l_pricing_terms(convertToInt(i)).getAs[Double]("rate"),
                l_pricing_terms(convertToInt(i)).getAs[Boolean]("is_deduction"),
                l_pricing_terms(convertToInt(i))
                  .getAs[Boolean]("is_media_cost_dependent"),
                l_pricing_terms(convertToInt(i))
                  .getAs[Integer]("data_member_id")
              )
            i = i + convertToInt(1)
          }
          Row(
            l_pricing_terms.map { x =>
              if (!_isnull(x))
                Row(convertToInt(x.getAs[Integer](0)),
                    x.getAs[Double](1),
                    x.getAs[Double](2),
                    x.getAs[Boolean](3),
                    x.getAs[Boolean](4),
                    convertToInt(x.getAs[Integer](5))
                )
              else null
            }.toArray,
            convertToInt(i)
          )
        },
      StructType(
        List(
          StructField(
            "l_pricing_terms",
            ArrayType(
              StructType(
                List(
                  StructField("term_id",                 IntegerType, true),
                  StructField("amount",                  DoubleType,  true),
                  StructField("rate",                    DoubleType,  true),
                  StructField("is_deduction",            BooleanType, true),
                  StructField("is_media_cost_dependent", BooleanType, true),
                  StructField("data_member_id",          IntegerType, true)
                )
              )
            ),
            false
          ),
          StructField("i", IntegerType, false)
        )
      )
    )
  }

  def temp7361735_UDF = {
    udf(
      (
        _id_type:   Integer,
        _pi_list:   Seq[Row],
        _i:         Integer,
        _l_id_type: Integer
      ) => {
        var pi        = Row(convertToInt(0), null)
        var l_id_type = _l_id_type
        var id_type   = _id_type
        var i         = _i
        var pi_list   = _pi_list.toArray
        while (compareTo(i, pi_list.length) < 0) {
          l_id_type = 0
          if (
            !_isnull(pi_list(convertToInt(i)).getAs[Integer]("identity_type"))
          )
            l_id_type = convertToInt(
              pi_list(convertToInt(i)).getAs[Integer]("identity_type")
            )
          if (l_id_type == id_type) pi = pi_list(convertToInt(i))
          i = i + convertToInt(1)
        }
        Row(
          if (!_isnull(pi))
            Row(convertToInt(pi.getAs[Integer](0)),
                if (pi.isNullAt(1)) null else pi.getAs[String](1).toString
            )
          else null,
          convertToInt(l_id_type),
          convertToInt(i)
        )
      },
      StructType(
        List(
          StructField("pi",
                      StructType(
                        List(StructField("identity_type",  IntegerType, true),
                             StructField("identity_value", StringType,  true)
                        )
                      ),
                      true
          ),
          StructField("l_id_type", IntegerType, true),
          StructField("i",         IntegerType, true)
        )
      )
    )
  }

  def temp10950958_UDF = {
    import java.text.SimpleDateFormat
    def f_forward_for_next_stage(agg_type: Int, payment_type: Int) = {
      var l_payment_type = convertToInt(
        if (payment_type == null)
          0
        else
          payment_type
      )
      var l_forward_for_next_stage = 0
      l_forward_for_next_stage = convertToInt(
        if (
          (if (agg_type == null)
             0
           else
             agg_type) == 1
        ) {
          if (l_payment_type == 2)
            1
          else
            0
        } else {
          if (
            (if (agg_type == null)
               0
             else
               agg_type) == 3
          )
            0
          else {
            if (
              (if (agg_type == null)
                 0
               else
                 agg_type) == 5
            )
              0
            else
              1
          }
        }
      )
      l_forward_for_next_stage
    }
    def f_is_matching_payment_and_agg_type(agg_type: Int, payment_type: Int) = {
      var l_agg_type = convertToInt(
        if (agg_type == null)
          0
        else
          agg_type
      )
      var l_payment_type = convertToInt(
        if (payment_type == null)
          0
        else
          payment_type
      )
      var l_is_matching_payment_and_agg_type = 0
      l_is_matching_payment_and_agg_type = convertToInt(
        if (
          (if (agg_type == null)
             0
           else
             agg_type) != 1 && (if (agg_type == null)
                                  0
                                else
                                  agg_type) != 2
        ) {
          if (l_agg_type == 3) {
            if (l_payment_type == 2)
              1
            else
              l_is_matching_payment_and_agg_type
          } else {
            if (l_agg_type == 4) {
              if (l_payment_type == 5)
                1
              else
                l_is_matching_payment_and_agg_type
            } else
              l_is_matching_payment_and_agg_type
          }
        } else {
          if (
            (if (payment_type == null)
               0
             else
               payment_type) == 1
          )
            1
          else
            l_is_matching_payment_and_agg_type
        }
      )
      l_is_matching_payment_and_agg_type
    }
    def f_payment_type_matches(agg_type: Int, payment_type: Int) = {
      var l_payment_type_matches = 0
      l_payment_type_matches = convertToInt(
        if (
          f_is_matching_payment_and_agg_type(if (agg_type == null)
                                               0
                                             else
                                               agg_type,
                                             if (payment_type == null)
                                               0
                                             else
                                               payment_type
          ) == 1 || (if (agg_type == null)
                       0
                     else
                       agg_type) == 5 && (if (payment_type == null)
                                            0
                                          else
                                            payment_type) == 6 || (if (
                                                                     agg_type == null
                                                                   )
                                                                     0
                                                                   else
                                                                     agg_type) == 0 && ((if (payment_type == null)
                                                                                           0
                                                                                         else
                                                                                           payment_type) != 1 && (if (payment_type == null)
                                                                                                                    0
                                                                                                                  else
                                                                                                                    payment_type) != 2 && (if (payment_type == null)
                                                                                                                                             0
                                                                                                                                           else
                                                                                                                                             payment_type) != 5 && (if (payment_type == null)
                                                                                                                                                                      0
                                                                                                                                                                    else
                                                                                                                                                                      payment_type) != 6)
        )
          1
        else
          l_payment_type_matches
      )
      l_payment_type_matches
    }
    def f_keep_data_charge(
      cost_pct:     Double,
      agg_type:     Int,
      payment_type: Int
    ) = {
      var l_agg_type = convertToInt(
        if (agg_type == null)
          0
        else
          agg_type
      )
      var l_payment_type = convertToInt(
        if (payment_type == null)
          0
        else
          payment_type
      )
      var l_keep_data_charge = 0
      l_keep_data_charge = convertToInt(
        if (
          (if (cost_pct == null)
             0
           else
             cost_pct) > 0
        ) {
          if (
            f_payment_type_matches(l_agg_type,
                                   l_payment_type
            ) == 1 || f_forward_for_next_stage(l_agg_type, l_payment_type) == 1
          )
            1
          else
            l_keep_data_charge
        } else {
          if (
            (if (agg_type == null)
               0
             else
               agg_type) == 0 || (if (agg_type == null)
                                    0
                                  else
                                    agg_type) == 4
          )
            1
          else
            l_keep_data_charge
        }
      )
      l_keep_data_charge
    }
    udf(
      (
        _l_data_costs:                Seq[Row],
        _l_payment_type:              Integer,
        _l_member_sales_tax_rate_pct: Double,
        _i:                           Integer,
        _l_media_cost_cpm:            Double,
        _l_agg_type:                  Integer,
        _l_data_cost:                 Row,
        _data_costs:                  Seq[Row]
      ) => {
        var l_data_costs                = _l_data_costs.toArray
        var l_payment_type              = _l_payment_type
        var l_member_sales_tax_rate_pct = _l_member_sales_tax_rate_pct
        var i                           = _i
        var l_media_cost_cpm            = _l_media_cost_cpm
        var l_agg_type                  = _l_agg_type
        var l_data_cost                 = _l_data_cost
        var data_costs                  = _data_costs.toArray
        while (compareTo(i, data_costs.length) < 0) {
          l_data_cost = data_costs(convertToInt(i))
          if (
            !_isnull(
              data_costs(convertToInt(i)).getAs[Double]("cost")
            ) && !_isnull(data_costs(convertToInt(i)).getAs[Double]("cost_pct"))
          ) {
            if (
              f_keep_data_charge(
                data_costs(convertToInt(i)).getAs[Double]("cost_pct"),
                l_agg_type,
                l_payment_type
              ) == 1
            ) {
              if (
                compareTo(data_costs(convertToInt(i)).getAs[Double]("cost_pct"),
                          0
                ) > 0 && f_payment_type_matches(l_agg_type, l_payment_type) == 1
              )
                l_data_cost = updateIndexInRow(
                  l_data_cost,
                  1,
                  l_media_cost_cpm * (l_data_cost.getAs[Double](
                    "cost_pct"
                  ) / 100.0d) * (1.0d + l_member_sales_tax_rate_pct / 100.0d)
                )
              if (l_agg_type == 0)
                l_data_cost = updateIndexInRow(
                  l_data_cost,
                  1,
                  l_data_cost.getAs[Double](
                    "cost"
                  ) * (1.0d + l_member_sales_tax_rate_pct / 100.0d)
                )
              l_data_costs =
                Array.concat(l_data_costs, Array.fill(1)(l_data_cost))
            }
          }
          i = i + convertToInt(1)
        }
        Row(
          l_data_costs.map { x =>
            if (!_isnull(x))
              Row(convertToInt(x.getAs[Integer](0)),
                  x.getAs[Double](1),
                  x.getAs[Seq[Integer]](2).toArray,
                  x.getAs[Double](3)
              )
            else null
          }.toArray,
          convertToInt(i),
          if (!_isnull(l_data_cost))
            Row(convertToInt(l_data_cost.getAs[Integer](0)),
                l_data_cost.getAs[Double](1),
                l_data_cost.getAs[Seq[Integer]](2).toArray,
                l_data_cost.getAs[Double](3)
            )
          else null
        )
      },
      StructType(
        List(
          StructField(
            "l_data_costs",
            ArrayType(
              StructType(
                List(
                  StructField("data_member_id", IntegerType,            true),
                  StructField("cost",           DoubleType,             true),
                  StructField("used_segments",  ArrayType(IntegerType), false),
                  StructField("cost_pct",       DoubleType,             true)
                )
              )
            ),
            false
          ),
          StructField("i", IntegerType, false),
          StructField(
            "l_data_cost",
            StructType(
              List(
                StructField("data_member_id", IntegerType,            true),
                StructField("cost",           DoubleType,             true),
                StructField("used_segments",  ArrayType(IntegerType), false),
                StructField("cost_pct",       DoubleType,             true)
              )
            ),
            false
          )
        )
      )
    )
  }

  def temp1248596_UDF = {
    udf(
      (
        _l_term_id:       Integer,
        _term_id:         Integer,
        _l_pricing_terms: Seq[Row],
        _i:               Integer
      ) => {
        var term_id         = _term_id
        var i               = _i
        var l_pricing_term  = Row(null, null, null, null, null, null)
        var l_pricing_terms = _l_pricing_terms.toArray
        var l_term_id       = _l_term_id
        while (compareTo(i, l_pricing_terms.length) < 0) {
          l_term_id = 0
          if (
            !_isnull(l_pricing_terms(convertToInt(i)).getAs[Integer]("term_id"))
          )
            l_term_id = convertToInt(
              l_pricing_terms(convertToInt(i)).getAs[Integer]("term_id")
            )
          if (l_term_id == term_id)
            l_pricing_term = l_pricing_terms(convertToInt(i))
          i = i + convertToInt(1)
        }
        Row(
          if (!_isnull(l_pricing_term))
            Row(
              convertToInt(l_pricing_term.getAs[Integer](0)),
              l_pricing_term.getAs[Double](1),
              l_pricing_term.getAs[Double](2),
              l_pricing_term.getAs[Boolean](3),
              l_pricing_term.getAs[Boolean](4),
              convertToInt(l_pricing_term.getAs[Integer](5))
            )
          else null,
          convertToInt(i),
          convertToInt(l_term_id)
        )
      },
      StructType(
        List(
          StructField(
            "l_pricing_term",
            StructType(
              List(
                StructField("term_id",                 IntegerType, true),
                StructField("amount",                  DoubleType,  true),
                StructField("rate",                    DoubleType,  true),
                StructField("is_deduction",            BooleanType, true),
                StructField("is_media_cost_dependent", BooleanType, true),
                StructField("data_member_id",          IntegerType, true)
              )
            ),
            false
          ),
          StructField("i",         IntegerType, false),
          StructField("l_term_id", IntegerType, false)
        )
      )
    )
  }

  def temp2704511_UDF = {
    udf(
      (
        _in_pricing_terms: Seq[Row],
        _l_pricing_terms:  Seq[Row],
        _i:                Integer,
        _magnitude:        Double
      ) => {
        var i = _i
        var in_pricing_terms =
          if (_in_pricing_terms != null) _in_pricing_terms.toArray
          else null.asInstanceOf[Array[Row]]
        var l_pricing_terms = in_pricing_terms
        var magnitude       = _magnitude
        while (
          l_pricing_terms != null && compareTo(i, l_pricing_terms.length) < 0
        ) {
          if (
            !_isnull(in_pricing_terms(convertToInt(i)).getAs[Double]("amount"))
          )
            l_pricing_terms(i) = Row(
              l_pricing_terms(convertToInt(i)).getAs[Integer]("term_id"),
              l_pricing_terms(convertToInt(i))
                .getAs[Double]("amount") / magnitude,
              l_pricing_terms(convertToInt(i)).getAs[Double]("rate"),
              l_pricing_terms(convertToInt(i)).getAs[Boolean]("is_deduction"),
              l_pricing_terms(convertToInt(i))
                .getAs[Boolean]("is_media_cost_dependent"),
              l_pricing_terms(convertToInt(i)).getAs[Integer]("data_member_id")
            )
          i = i + convertToInt(1)
        }
        Row(
          if (l_pricing_terms == null) null
          else
            l_pricing_terms.map { x =>
              if (!_isnull(x))
                Row(convertToInt(x.getAs[Integer](0)),
                    x.getAs[Double](1),
                    x.getAs[Double](2),
                    x.getAs[Boolean](3),
                    x.getAs[Boolean](4),
                    convertToInt(x.getAs[Integer](5))
                )
              else null
            },
          convertToInt(i)
        )
      },
      StructType(
        List(
          StructField(
            "l_pricing_terms",
            ArrayType(
              StructType(
                List(
                  StructField("term_id",                 IntegerType, true),
                  StructField("amount",                  DoubleType,  true),
                  StructField("rate",                    DoubleType,  true),
                  StructField("is_deduction",            BooleanType, true),
                  StructField("is_media_cost_dependent", BooleanType, true),
                  StructField("data_member_id",          IntegerType, true)
                )
              )
            ),
            false
          ),
          StructField("i", IntegerType, false)
        )
      )
    )
  }

  def temp3446850_UDF = {
    udf(
      (_l_filtered_terms: Seq[Row], _unfiltered_terms: Seq[Row], _i: Integer) =>
        {
          var l_current_term   = Row(null, null, null, null, null, null)
          var l_filtered_terms = _l_filtered_terms.toArray
          var i                = _i
          var unfiltered_terms = _unfiltered_terms.toArray
          while (compareTo(i, unfiltered_terms.length) < 0) {
            l_current_term = unfiltered_terms(convertToInt(i))
            if (
              unfiltered_terms(convertToInt(i))
                .getAs[Boolean]("is_deduction") == convertToBoolean(0)
            )
              l_filtered_terms =
                Array.concat(l_filtered_terms,
                             Array.fill(1)(unfiltered_terms(convertToInt(i)))
                )
            i = i + convertToInt(1)
          }
          Row(
            if (!_isnull(l_current_term))
              Row(
                convertToInt(l_current_term.getAs[Integer](0)),
                l_current_term.getAs[Double](1),
                l_current_term.getAs[Double](2),
                l_current_term.getAs[Boolean](3),
                l_current_term.getAs[Boolean](4),
                convertToInt(l_current_term.getAs[Integer](5))
              )
            else null,
            l_filtered_terms.map { x =>
              if (!_isnull(x))
                Row(convertToInt(x.getAs[Integer](0)),
                    x.getAs[Double](1),
                    x.getAs[Double](2),
                    x.getAs[Boolean](3),
                    x.getAs[Boolean](4),
                    convertToInt(x.getAs[Integer](5))
                )
              else null
            }.toArray,
            convertToInt(i)
          )
        },
      StructType(
        List(
          StructField(
            "l_current_term",
            StructType(
              List(
                StructField("term_id",                 IntegerType, true),
                StructField("amount",                  DoubleType,  true),
                StructField("rate",                    DoubleType,  true),
                StructField("is_deduction",            BooleanType, true),
                StructField("is_media_cost_dependent", BooleanType, true),
                StructField("data_member_id",          IntegerType, true)
              )
            ),
            false
          ),
          StructField(
            "l_filtered_terms",
            ArrayType(
              StructType(
                List(
                  StructField("term_id",                 IntegerType, true),
                  StructField("amount",                  DoubleType,  true),
                  StructField("rate",                    DoubleType,  true),
                  StructField("is_deduction",            BooleanType, true),
                  StructField("is_media_cost_dependent", BooleanType, true),
                  StructField("data_member_id",          IntegerType, true)
                )
              )
            ),
            false
          ),
          StructField("i", IntegerType, false)
        )
      )
    )
  }

  def temp10951013_UDF = {
    import java.text.SimpleDateFormat
    def f_forward_for_next_stage(agg_type: Int, payment_type: Int) = {
      var l_payment_type = convertToInt(
        if (payment_type == null)
          0
        else
          payment_type
      )
      var l_forward_for_next_stage = 0
      l_forward_for_next_stage = convertToInt(
        if (
          (if (agg_type == null)
             0
           else
             agg_type) == 1
        ) {
          if (l_payment_type == 2)
            1
          else
            0
        } else {
          if (
            (if (agg_type == null)
               0
             else
               agg_type) == 3
          )
            0
          else {
            if (
              (if (agg_type == null)
                 0
               else
                 agg_type) == 5
            )
              0
            else
              1
          }
        }
      )
      l_forward_for_next_stage
    }
    def f_is_matching_payment_and_agg_type(agg_type: Int, payment_type: Int) = {
      var l_agg_type = convertToInt(
        if (agg_type == null)
          0
        else
          agg_type
      )
      var l_payment_type = convertToInt(
        if (payment_type == null)
          0
        else
          payment_type
      )
      var l_is_matching_payment_and_agg_type = 0
      l_is_matching_payment_and_agg_type = convertToInt(
        if (
          (if (agg_type == null)
             0
           else
             agg_type) != 1 && (if (agg_type == null)
                                  0
                                else
                                  agg_type) != 2
        ) {
          if (l_agg_type == 3) {
            if (l_payment_type == 2)
              1
            else
              l_is_matching_payment_and_agg_type
          } else {
            if (l_agg_type == 4) {
              if (l_payment_type == 5)
                1
              else
                l_is_matching_payment_and_agg_type
            } else
              l_is_matching_payment_and_agg_type
          }
        } else {
          if (
            (if (payment_type == null)
               0
             else
               payment_type) == 1
          )
            1
          else
            l_is_matching_payment_and_agg_type
        }
      )
      l_is_matching_payment_and_agg_type
    }
    def f_payment_type_matches(agg_type: Int, payment_type: Int) = {
      var l_payment_type_matches = 0
      l_payment_type_matches = convertToInt(
        if (
          f_is_matching_payment_and_agg_type(if (agg_type == null)
                                               0
                                             else
                                               agg_type,
                                             if (payment_type == null)
                                               0
                                             else
                                               payment_type
          ) == 1 || (if (agg_type == null)
                       0
                     else
                       agg_type) == 5 && (if (payment_type == null)
                                            0
                                          else
                                            payment_type) == 6 || (if (
                                                                     agg_type == null
                                                                   )
                                                                     0
                                                                   else
                                                                     agg_type) == 0 && ((if (payment_type == null)
                                                                                           0
                                                                                         else
                                                                                           payment_type) != 1 && (if (payment_type == null)
                                                                                                                    0
                                                                                                                  else
                                                                                                                    payment_type) != 2 && (if (payment_type == null)
                                                                                                                                             0
                                                                                                                                           else
                                                                                                                                             payment_type) != 5 && (if (payment_type == null)
                                                                                                                                                                      0
                                                                                                                                                                    else
                                                                                                                                                                      payment_type) != 6)
        )
          1
        else
          l_payment_type_matches
      )
      l_payment_type_matches
    }
    def f_keep_data_charge(
      cost_pct:     Double,
      agg_type:     Int,
      payment_type: Int
    ) = {
      var l_agg_type = convertToInt(
        if (agg_type == null)
          0
        else
          agg_type
      )
      var l_payment_type = convertToInt(
        if (payment_type == null)
          0
        else
          payment_type
      )
      var l_keep_data_charge = 0
      l_keep_data_charge = convertToInt(
        if (
          (if (cost_pct == null)
             0
           else
             cost_pct) > 0
        ) {
          if (
            f_payment_type_matches(l_agg_type,
                                   l_payment_type
            ) == 1 || f_forward_for_next_stage(l_agg_type, l_payment_type) == 1
          )
            1
          else
            l_keep_data_charge
        } else {
          if (
            (if (agg_type == null)
               0
             else
               agg_type) == 0 || (if (agg_type == null)
                                    0
                                  else
                                    agg_type) == 4
          )
            1
          else
            l_keep_data_charge
        }
      )
      l_keep_data_charge
    }
    udf(
      (
        _l_data_costs:                Seq[Row],
        _l_payment_type:              Integer,
        _l_member_sales_tax_rate_pct: Double,
        _i:                           Integer,
        _l_media_cost_cpm:            Double,
        _l_agg_type:                  Integer,
        _l_data_cost:                 Row,
        _data_costs:                  Seq[Row]
      ) => {
        var l_data_costs                = _l_data_costs.toArray
        var l_payment_type              = _l_payment_type
        var l_member_sales_tax_rate_pct = _l_member_sales_tax_rate_pct
        var i                           = _i
        var l_media_cost_cpm            = _l_media_cost_cpm
        var l_agg_type                  = _l_agg_type
        var l_data_cost                 = _l_data_cost
        var data_costs                  = _data_costs.toArray
        while (compareTo(i, data_costs.length) < 0) {
          l_data_cost = data_costs(convertToInt(i))
          if (
            !_isnull(
              data_costs(convertToInt(i)).getAs[Double]("cost")
            ) && !_isnull(data_costs(convertToInt(i)).getAs[Double]("cost_pct"))
          ) {
            if (
              f_keep_data_charge(
                data_costs(convertToInt(i)).getAs[Double]("cost_pct"),
                l_agg_type,
                l_payment_type
              ) == 1
            ) {
              if (
                compareTo(data_costs(convertToInt(i)).getAs[Double]("cost_pct"),
                          0
                ) > 0 && f_payment_type_matches(l_agg_type, l_payment_type) == 1
              )
                l_data_cost = updateIndexInRow(
                  l_data_cost,
                  1,
                  l_media_cost_cpm * (l_data_cost.getAs[Double](
                    "cost_pct"
                  ) / 100.0d) * (1.0d + l_member_sales_tax_rate_pct / 100.0d)
                )
              if (l_agg_type == 0)
                l_data_cost = updateIndexInRow(
                  l_data_cost,
                  1,
                  l_data_cost.getAs[Double](
                    "cost"
                  ) * (1.0d + l_member_sales_tax_rate_pct / 100.0d)
                )
              l_data_costs =
                Array.concat(l_data_costs, Array.fill(1)(l_data_cost))
            }
          }
          i = i + convertToInt(1)
        }
        Row(
          l_data_costs.map { x =>
            if (!_isnull(x))
              Row(convertToInt(x.getAs[Integer](0)),
                  x.getAs[Double](1),
                  x.getAs[Seq[Integer]](2).toArray,
                  x.getAs[Double](3)
              )
            else null
          }.toArray,
          convertToInt(i),
          if (!_isnull(l_data_cost))
            Row(convertToInt(l_data_cost.getAs[Integer](0)),
                l_data_cost.getAs[Double](1),
                l_data_cost.getAs[Seq[Integer]](2).toArray,
                l_data_cost.getAs[Double](3)
            )
          else null
        )
      },
      StructType(
        List(
          StructField(
            "l_data_costs",
            ArrayType(
              StructType(
                List(
                  StructField("data_member_id", IntegerType,            true),
                  StructField("cost",           DoubleType,             true),
                  StructField("used_segments",  ArrayType(IntegerType), false),
                  StructField("cost_pct",       DoubleType,             true)
                )
              )
            ),
            false
          ),
          StructField("i", IntegerType, false),
          StructField(
            "l_data_cost",
            StructType(
              List(
                StructField("data_member_id", IntegerType,            true),
                StructField("cost",           DoubleType,             true),
                StructField("used_segments",  ArrayType(IntegerType), false),
                StructField("cost_pct",       DoubleType,             true)
              )
            ),
            false
          )
        )
      )
    )
  }

  def temp7091_UDF = {
    udf(
      (
        _bumper_count: Integer,
        _index:        Integer,
        _i:            Integer,
        _placements:   Seq[Row]
      ) => {
        var placements   = _placements.toArray
        var is_ad_pod    = 0
        var i            = _i
        var bumper_count = _bumper_count
        var index        = _index
        while (
          compareTo(i,
                    placements(convertToInt(index))
                      .getAs[Seq[Row]]("ad_slots")
                      .toArray[Row]
                      .length
          ) < 0
        ) {
          if (
            !_isnull(
              placements(convertToInt(index))
                .getAs[Seq[Row]]("ad_slots")
                .toArray[Row]
                .array(convertToInt(i))
                .getAs[Integer]("slot_type")
            )
          ) {
            if (
              placements(convertToInt(index))
                .getAs[Seq[Row]]("ad_slots")
                .toArray[Row]
                .array(convertToInt(i))
                .getAs[Integer]("slot_type") != 0
            ) {
              bumper_count = bumper_count + convertToInt(1)
              is_ad_pod = 1
            }
          }
          if (
            !_isnull(
              placements(convertToInt(index))
                .getAs[Seq[Row]]("ad_slots")
                .toArray[Row]
                .array(convertToInt(i))
                .getAs[Integer]("ad_slot_position")
            )
          ) {
            if (
              compareTo(placements(convertToInt(index))
                          .getAs[Seq[Row]]("ad_slots")
                          .toArray[Row]
                          .array(convertToInt(i))
                          .getAs[Integer]("ad_slot_position"),
                        0
              ) > 0
            )
              is_ad_pod = 1
          }
          i = i + convertToInt(1)
        }
        Row(convertToInt(is_ad_pod),
            convertToInt(i),
            convertToInt(bumper_count)
        )
      },
      StructType(
        List(StructField("is_ad_pod",    IntegerType, false),
             StructField("i",            IntegerType, false),
             StructField("bumper_count", IntegerType, false)
        )
      )
    )
  }

  def processUDF_8439 = {
    udf(
      { (input: Seq[Row]) =>
        import _root_.io.prophecy.abinitio.ScalaFunctions._
        import _root_.io.prophecy.libs.AbinitioDMLs._
        val outputRows         = scala.collection.mutable.ArrayBuffer[Row]()
        var SEQFILE_CHUNK_SIZE = 0
        var ALLOW_EMPTY_INPUT  = 0
        def output_read_spec(
          filepath:  String,
          info:      file_information_type,
          in_record: Row
        ) = {
          var start_offset        = 0L
          var end_offset          = 0L
          var file_size_remaining = convertToLong(info.size)
          var address             = Row(null, null, null)
          var block_size = convertToInt(
            if (
              (try info.block_size
              catch {
                case error: Throwable => null
              }) == null
            ) 0
            else info.block_size
          )
          if (convertToBoolean(convertToInt(block_size) == 0))
            _print_error(
              ("block_size is 0 in Find Files and Read Blocks " + _string_representation(
                info
              ) + """
""").toString
            )
          var idx = 0
          while (convertToBoolean(compareTo(idx, info.host.length) < 0)) {
            var chunk_number = 0L
            start_offset = convertToLong(_math_min(idx * block_size, info.size))
            end_offset =
              convertToLong(_math_min((idx + 1) * block_size, info.size))
            if (
              convertToBoolean(
                compareTo(
                  convertToLong(_string_lrtrim(SEQFILE_CHUNK_SIZE.toString)),
                  0
                ) > 0
              )
            ) {
              while (
                convertToBoolean(compareTo(start_offset, end_offset) < 0)
              ) {
                var size_this_time =
                  if (
                    convertToBoolean(
                      compareTo(end_offset - start_offset,
                                SEQFILE_CHUNK_SIZE
                      ) < 0
                    )
                  ) end_offset - start_offset
                  else SEQFILE_CHUNK_SIZE
                address = Row(
                  if (convertToBoolean(compareTo(end_offset, start_offset) > 0))
                    start_offset
                  else 0,
                  size_this_time,
                  0
                )
                outputRows.append(
                  Row(
                    filepath.toString,
                    Row(convertToLong(address.get(0)),
                        convertToInt(address.get(1)),
                        convertToInt(address.get(2))
                    ),
                    info.host.array(convertToInt(idx)),
                    Row(in_record.get(0).toString),
                    convertToInt(chunk_number)
                  )
                )
                start_offset = convertToLong(start_offset + size_this_time)
                chunk_number = convertToLong(chunk_number + 1)
              }
            } else {
              address =
                Row(if (
                      convertToBoolean(compareTo(end_offset, start_offset) > 0)
                    ) start_offset
                    else 0,
                    end_offset - start_offset,
                    0
                )
              outputRows.append(
                Row(
                  filepath.toString,
                  Row(convertToLong(address.get(0)),
                      convertToInt(address.get(1)),
                      convertToInt(address.get(2))
                  ),
                  info.host.array(convertToInt(idx)),
                  Row(in_record.get(0).toString),
                  convertToInt(0)
                )
              )
            }
            idx = convertToInt(idx + 1)
          }
          0
        }
        def traverse_directory(
          current_directory: String,
          in_record:         Row
        ): Int = {
          var dirlist = _directory_listing(current_directory.toString, "[!._]*")
          if (
            convertToBoolean(
              convertToLong(_string_lrtrim(ALLOW_EMPTY_INPUT.toString)) == 0
            )
          )
            if (convertToBoolean(compareTo(dirlist.length, 1) < 0))
              _print_error((current_directory + " has no files.").toString)
          dirlist.zipWithIndex
            .map({
              case (_shortpath, shortpathIndex) =>
                var shortpath = _shortpath
                var fullpath  = current_directory + "/" + shortpath
                var info      = _file_information(fullpath.toString)
                if (
                  convertToBoolean(info.found) && convertToBoolean(
                    info.file_type.toString == "DIR "
                  )
                ) traverse_directory(fullpath, in_record)
                else if (convertToBoolean(_ends_with(shortpath.toString, "")))
                  output_read_spec(fullpath,                             info, in_record)
            })
            .toArray
          null
          0
        }
        def process(input: Seq[Row]) = {
          var readRecordInstance = RecordIterator.getInstance(input)
          var have_data          = 1
          while (convertToBoolean(have_data)) {
            var in_record = readRecordInstance.readRecord
            var starting_path =
              (if (
                 (try in_record
                   .getAs[String]("_AB_read_hdfs_files_starting_path_")
                 catch {
                   case error: Throwable => null
                 }) == null
               ) ""
               else
                 in_record.getAs[String](
                   "_AB_read_hdfs_files_starting_path_"
                 )).toString
            if (convertToBoolean(starting_path.toString == ""))
              have_data = convertToInt(0)
            else {
              var info = _file_information(starting_path.toString)
              if (
                convertToBoolean(info.found) && convertToBoolean(
                  info.file_type.toString == "DIR "
                )
              ) traverse_directory(starting_path, in_record)
              else if (
                convertToBoolean(info.found) && convertToBoolean(
                  info.file_type.toString == "HDFS"
                )
              ) output_read_spec(starting_path, info, in_record)
              else _print_error((starting_path + " not found.").toString)
            }
          }
          null
          0
        }
        process(input)
        outputRows
      },
      ArrayType(
        StructType(
          List(
            StructField("_AB_read_hdfs_files_filename", StringType, false),
            StructField(
              "_AB_read_hdfs_files_address",
              StructType(
                List(StructField("file_offset", LongType,    false),
                     StructField("compr_size",  IntegerType, false),
                     StructField("byte_offset", IntegerType, false)
                )
              ),
              false
            ),
            StructField("_AB_read_hdfs_files_hosts",
                        ArrayType(StringType),
                        false
            ),
            StructField("_AB_read_hdfs_files_input_record",
                        StructType(
                          List(
                            StructField("_AB_read_hdfs_files_starting_path_",
                                        StringType,
                                        false
                            )
                          )
                        ),
                        false
            ),
            StructField("sequence_number", IntegerType, false)
          )
        )
      )
    )
  }

  def allFieldsNull =
    udf { (row: Row) =>
      if (row == null) true
      else row.schema.fields.forall(f => row.isNullAt(row.fieldIndex(f.name)))
    }

}

object PipelineInitCode extends Serializable
