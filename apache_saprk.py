
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max as max_

def main():
    # Create a SparkSession
    spark = SparkSession.builder \
        .appName("Merge and Clean DataFrames") \
        .getOrCreate()

    # Define the CSV files
    df_file1 = "dataframe1.csv"
    df_file2 = "dataframe2.csv"

    # Read the CSV files into DataFrames
    df1 = read_csv(spark, df_file1)
    df2 = read_csv(spark, df_file2)

    # Merge the DataFrames
    merged_df = df1.union(df2)

    # Convert the 'Date' column to Date data type
    merged_df = merged_df.withColumn('Date', col('Date').cast('date'))

    # Group by ID and find the maximum date for each ID
    max_dates_df = merged_df.groupBy('ID').agg(max_('Date').alias('MaxDate'))

    # Join the original DataFrame with the DataFrame containing maximum dates
    final_df = merged_df.alias("m").join(max_dates_df.alias("max"), 
        (col("m.ID") == col("max.ID")) & (col("m.Date") == col("max.MaxDate")), 'inner') \
        .select(col("m.ID"), col("m.Date"), col("m.Amount"), col("m.Currency"))

    # Show the final DataFrame
    final_df.show()

def read_csv(spark:SparkSession, file_path:str) -> SparkSession.createDataFrame:
    """
    Function to read a CSV file and return a DataFrame.
    """
    return spark.read.csv(file_path, header=True, inferSchema=True)

if __name__ == "__main__":
    main()
