from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def JupyterCode_111(spark: SparkSession, in0: DataFrame, in1: DataFrame) -> DataFrame:
    # List all non-standard packages to be imported by your
    # script here (only missing packages will be installed)
    from ayx import Package
    #Package.installPackages(['pandas','numpy'])
    # Import necessary libraries
    import pandas as pd
    from ayx import Alteryx
    # Read Input 1 from the first Alteryx input anchor (#1)
    input1_df = Alteryx.read(
        "#1"
    )
    # Read Input 2 from the second Alteryx input anchor (#2)
    input2_df = Alteryx.read(
        "#2"
    )
    # Merge Input 1 and Input 2 on 'Hyp Cat' to bring in the 'pa account's and 'percent_of_spend'
    merged_df = pd.merge(input1_df, input2_df, on = 'Hyp Cat', how = 'left')
    # Allocate the sums based on the 'percent_of_spend'
    merged_df['Allocated_Sum_LCL'] = merged_df['Sum_LCL'] * (merged_df['percent_of_spend'] / 100)
    merged_df['Allocated_Sum_LCLCFX'] = merged_df['Sum_LCLCFX'] * (merged_df['percent_of_spend'] / 100)
    merged_df['Allocated_Sum_USDAFX'] = merged_df['Sum_USDAFX'] * (merged_df['percent_of_spend'] / 100)
    # Output the result to the first Alteryx output anchor (#1)
    Alteryx.write(merged_df, 1)

    return out0
