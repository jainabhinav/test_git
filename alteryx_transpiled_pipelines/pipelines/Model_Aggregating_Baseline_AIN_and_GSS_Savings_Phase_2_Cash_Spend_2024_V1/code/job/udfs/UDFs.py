from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.lookups import (
    createLookup,
    createRangeLookup,
    lookup,
    lookup_last,
    lookup_match,
    lookup_count,
    lookup_row,
    lookup_row_reverse,
    lookup_nth
)

from datetime import datetime, timedelta
import calendar
from dateutil.relativedelta import relativedelta

def convert_format(custom_format: str) -> str:
    format_mapping = {
        'yyyy': '%Y',  # 4-digit year
        'yy': '%y',  # 2-digit year
        'MM': '%m',  # Month as zero-padded number
        'dd': '%d',  # Day of the month as zero-padded number
        'HH': '%H',  # Hour (24-hour clock)
        'hh': '%I',  # Hour (12-hour clock)
        'mm': '%M',  # Minutes
        'SS': '%S',  # Seconds
        'a': '%p',  # AM/PM
        'month': '%B',  # Full month name
        'Month': '%b',  # Abbreviated month name
        'day': '%A',  # Full weekday name
        'Day': '%a'  # Abbreviated weekday name
    }

    for custom_token, strftime_code in format_mapping.items():
        custom_format = custom_format.replace(custom_token, strftime_code)

    return custom_format

def date_add_native(d1: datetime, amount: int) -> datetime:
    return d1 + timedelta(days = amount)

def add_months_native(date: datetime, amount: int) -> datetime:
    return date + relativedelta(months = amount)

def to_date_native(dateStr: str, format: str="yyyy-MM-dd") -> datetime:
    return datetime.strptime(dateStr, convert_format(format))

def to_timestamp_native(dateStr: str, format: str="yyyy-MM-dd") -> datetime:
    return datetime.strptime(dateStr, convert_format(format))

def date_format_native(date: datetime, format: str="yyyy-MM-dd") -> str:
    return date.strftime(convert_format(format))

def months_between_native(date1: datetime, date2: datetime) -> int:
    return (date2.year - date1.year) * 12 + date2.month - date1.month

def datediff_native(date1: datetime, date2: datetime) -> int:
    return (date1 - date2).days

def year_native(date: datetime) -> int:
    return date.year

def month_native(date: datetime) -> int:
    return date.month

def day_native(date: datetime) -> int:
    return date.day

def second_native(date: datetime) -> int:
    return date.second

def minute_native(date: datetime) -> int:
    return date.minute

def hour_native(date: datetime) -> int:
    return date.hour

def quarter_native(date: datetime) -> int:
    return (date.month - 1) // 3 + 1

def date_trunc_native(unit: str, date: datetime) -> datetime:
    if unit == "year":
        return datetime(date.year, 1, 1)
    elif unit == "month":
        return datetime(date.year, date.month, 1)
    else:
        raise ValueError(f"Unsupported truncation unit: {unit}")

def last_day_native(date: datetime) -> datetime:
    next_month = date.replace(day = 28) + timedelta(days = 4)

    return next_month - timedelta(days = next_month.day)

def current_timestamp_native() -> datetime:
    return datetime.now()

def current_date_native() -> datetime:
    return datetime.now().date()

def evaluate_static_expression(spark, expression):
    df = spark.createDataFrame([("dummy", )], ["dummy"])
    result_df = df.selectExpr(f"{expression} as result")
    result = result_df.collect()[0]['result']

    return result

def replace_expression_in_namespace(expression: str, namespace: str, old_value: str, new_value: str):
    split_list = re.split(r'(in\d+)', expression)
    ans = []
    i = 0

    while i < len(split_list):
        if split_list[i] == namespace and i < len(split_list) - 1:
            ans.append(namespace)
            ans.append(split_list[i + 1].replace(old_value, new_value))
            i = i + 2
        else:
            ans.append(split_list[i])
            i = i + 1

    return ''.join(ans)

def update_config(value: str, operations: list):
    current = value

    for op in operations:
        if op[0]:
            current = op[2]
        elif op[3] is not None and op[1] is not None:
            current = replace_expression_in_namespace(current, str(op[3]), str(op[1]), str(op[2]))
        else:
            current = current.replace(str(op[1]), str(op[2]))

    return current

def alteryxOutputColumns(all_columns, columns_to_drop):
    conflicting_columns = [col.replace("in0_", "").replace("in1_", "") for col in all_columns]
    conflicting_columns = set([x for x in conflicting_columns if conflicting_columns.count(x) > 1])
    output_columns = []

    for col_name in all_columns:
        original_column_name = col_name.replace("in0_", "").replace("in1_", "")

        if col_name in columns_to_drop:
            continue
        else:
            if original_column_name in conflicting_columns and col_name.startswith("in1_"):
                output_columns.append(f"`in1_{original_column_name}` AS `Right_{original_column_name}`")
            elif col_name.startswith("in1_"):
                output_columns.append(f"`in1_{original_column_name}` AS `{original_column_name}`")
            elif col_name.startswith("in0_"):
                output_columns.append(f"`in0_{original_column_name}` AS `{original_column_name}`")
            else:
                output_columns.append(f"`{original_column_name}` AS `{original_column_name}`")

    return output_columns

def textToColumns(exploded_df, input_column, num_cols, rootName, field_separator):
    split_col_df = exploded_df.withColumn(
        "splitCol",
        call_spark_fcn("splitIntoMultipleColumnsUdf", col(input_column), lit(field_separator), lit(num_cols))\
          .alias("splitCol")
    )
    final_df = split_col_df
    index = - 1
    output_schema_columns = [f"{rootName}{i}" for i in range(1, num_cols + 1)]

    for column in output_schema_columns:
        index += 1
        final_df = final_df.withColumn(column, col("splitCol")[index])

    final_df = final_df.drop("splitCol").drop(input_column)

    return final_df

def transposeDataFrame(df, key_columns, data_columns):
    unpivoted_cols = (
        [col(column) for column in key_columns]
        + [lit(column).alias("Name") for column in data_columns]
        + [col(column).alias("Value") for column in data_columns]
    )

    return df.select(*unpivoted_cols)

def registerUDFs(spark: SparkSession):
    try:
        from prophecy.utils import ScalaUtil
        ScalaUtil.initializeUDFs(spark)
    except :
        pass
