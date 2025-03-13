from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def JupyterCode_17(spark: SparkSession, in0: DataFrame) -> DataFrame:
    # List all non-standard packages to be imported by your
    # script here (only missing packages will be installed)
    from ayx import Package
    #Package.installPackages(['pandas','numpy'])
    #################################
    # List all non-standard packages to be imported by your
    # script here (only missing packages will be installed)
    from ayx import Package
    Package.installPackages(['pandas', 'numpy'])
    #################################
    from ayx import Alteryx
    import pandas as pd
    # Step 1: Read in the dataset from the first input of the Python tool
    df = Alteryx.read(
        "#1"
    ) # Input is assumed to be from the first input
    # Step 2: Ensure the 'spend' column is numeric
    df['spend'] = pd.to_numeric(df['spend'], errors = 'coerce')
    # Step 3: Calculate the total spend per Planning Account (across all categories)
    account_totals = df.groupby('Planning Account')['spend'].transform('sum')
    # Step 4: Calculate the percentage each category contributes to its Planning Account's total spend
    df['Percent_of_Account'] = (df['spend'] / account_totals) * 100
    # Step 5: Round the percentages to two decimal places for better readability
    df['Percent_of_Account'] = df['Percent_of_Account'].round(8)
    # Step 6: Select the relevant columns: Planning Account, Category, and Percent_of_Account
    output_df = df[['Planning Account', 'Category', 'Percent_of_Account']]
    # Step 7: Write the output back to Alteryx for downstream tools
    Alteryx.write(output_df, 1) # Output to the first output anchor

    return out0
