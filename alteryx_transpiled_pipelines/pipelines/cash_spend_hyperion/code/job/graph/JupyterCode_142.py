from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def JupyterCode_142(spark: SparkSession, in0: DataFrame) -> DataFrame:
    # List all non-standard packages to be imported by your
    # script here (only missing packages will be installed)
    from ayx import Package
    #Package.installPackages(['pandas','numpy'])
    from ayx import Alteryx
    import pandas as pd
    # Step 1: Read in the datasets from the inputs in the Python Tool
    spend_data = Alteryx.read(
        "#1"
    ) # Input from source #1 with spend data: 'scenario name', 'version', 'year', 'Hyp Cat', 'Sum_LCL', 'Sum_LCLCFX', 'Sum_USDAFX'
    percent_data = Alteryx.read(
        "#2"
    ) # Input from source #2 with 'Hyp Cat' and 'PA Account'
    # Step 2: Count the number of PA Account entries for each unique combination of 'Hyp Cat', 'scenario name', 'version', and 'year' in spend_data
    pa_account_count = spend_data\
                           .groupby(['Hyp Cat', 'scenario name', 'version', 'year'])\
                           .size()\
                           .reset_index(name = 'PA_Account_Count')
    # Step 3: Ensure that percent_data contains unique 'Hyp Cat' and 'PA Account' details
    percent_data_unique = percent_data[['Hyp Cat']].drop_duplicates()
    # Step 4: Merge the spend_data with percent_data on 'Hyp Cat'
    # This ensures that spend_data gets the 'Hyp Cat' PA account details
    merged_df = pd.merge(spend_data, percent_data_unique, on = 'Hyp Cat', how = 'left')
    # Step 5: Merge the PA Account count with the merged dataframe to bring in the 'PA_Account_Count'
    # This step ensures that the number of PA Accounts per combination of 'Hyp Cat', 'scenario name', 'version', and 'year' is added to the merged dataframe
    merged_df = pd.merge(merged_df, pa_account_count, on = ['Hyp Cat', 'scenario name', 'version', 'year'], how = 'left')
    # Step 6: Distribute the spend amounts across the number of PA Accounts for each unique combination
    # Assuming an equal distribution of the spend across the PA Accounts
    merged_df['LCL_Split'] = merged_df['Sum_LCL'] / merged_df['PA_Account_Count']
    merged_df['LCLCFX_Split'] = merged_df['Sum_LCLCFX'] / merged_df['PA_Account_Count']
    merged_df['USDAFX_Split'] = merged_df['Sum_USDAFX'] / merged_df['PA_Account_Count']
    # Step 7: Output the relevant columns for further analysis or reporting
    output_df_1 = merged_df[['year', 'scenario name', 'version', 'Hyp Cat', 'LCL_Split', 'LCLCFX_Split', 'USDAFX_Split']]
    # Step 8: Write the output back to Alteryx
    Alteryx.write(output_df_1, 1) # Output to first anchor for the final result

    return out0
