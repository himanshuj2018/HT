# The code below reads two datasets, merges them based on 'counter_party', performs custom grouping and aggregation,
# and saves the result to a CSV file.
import pandas as pd

def custom_groupby_and_aggregate(df, group_column):
    """
    This function groups the input DataFrame by the specified 'group_column' and calculates the total count of records,
    the maximum rating, and the sum of 'value' for two different 'status' categories ('ARAP' and 'ACCR') within each group.
    It then ensures that the 'legal_entity', 'counter_party', and 'tier' columns exist in the grouped DataFrame,
    filling in missing values with the 'total' column.

    Parameters:
    - df (pandas.DataFrame): The input DataFrame containing the data to be processed.
    - group_column (str): The name of the column by which the DataFrame should be grouped.

    Returns:
    - pandas.DataFrame: Aggregated results with columns for group, max rating, and sum of values.
    
    """
    aggr_columns =  ['legal_entity', 'counter_party', 'tier']
    # Group by the specified group_column and aggregate the required values
    # NOTE: "Also create new record to add total for each of legal entity, counterparty & tier."
    # The above statement is a bit ambiguoes in the sample_test.txt 
    # so it is assumed that the Total here means the number of rows within the group
    grouped = df.groupby(group_column).agg(
        total=("rating", "count"),
        max_rating=("rating", "max"),
        sum_arap=("value", lambda x: x[df["status"] == "ARAP"].sum()),
        sum_accr=("value", lambda x: x[df["status"] == "ACCR"].sum())
    ).reset_index()

    # Ensure that all aggr_columns exist in the grouped DataFrame
    for column in aggr_columns:
        if column not in grouped.columns:
            grouped[column] = grouped['total']
    
    # Remove the 'total' column, as it's no longer needed
    grouped.drop(columns=["total"], inplace=True)

    # Rename columns to match the result DataFrame's column names
    grouped.rename(columns={"max_rating": "max(rating by counterparty)",
                           "sum_arap": "sum(value where status=ARAP)",
                           "sum_accr": "sum(value where status=ACCR)"}, inplace=True)

    return grouped


if __name__ == "__main__":
    # Load two datasets and merge them based on 'counter_party' column
    dataset1 = pd.read_csv("dataset1.csv")
    dataset2 = pd.read_csv("dataset2.csv")
    df = dataset1.merge(dataset2, on="counter_party")
    
    # Define a list of group_by_columns for custom grouping and aggregation
    group_by_columns = [['legal_entity'], ['counter_party'], ['tier'], 
                        ['legal_entity', 'counter_party'], ['counter_party', 'tier'], ['tier', 'legal_entity'],
                        ['legal_entity', 'counter_party', 'tier']
                       ]
    
    # Initialize the result DataFrame with required column names
    result_df = pd.DataFrame(columns=["legal_entity", "counter_party", "tier", 
                                    "max(rating by counterparty)", "sum(value where status=ARAP)", 
                                    "sum(value where status=ACCR)"])

    # Loop through the group_by_columns and perform custom grouping and aggregation
    for group_column in group_by_columns:
        grouped_df = custom_groupby_and_aggregate(df, group_column)
        result_df = result_df.append(grouped_df, ignore_index=True)
    
    # Save the resulting DataFrame to a CSV file
    result_df.to_csv("pandas_framework_output.csv", index=False)
