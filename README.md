<!-- PROJECT POD -->
<br />
<p align="center">
  <h3 align="center"> PySpark DataFrame Merge and Clean </h3>
</p>

<!-- TABLE OF CONTENTS -->
<details open="open">
  <summary>Table of contents</summary>
  <ol>
    <li>
      <a href="#Process target">Process target</a>
      <ul>
        <li><a href="#Process description">Process description</a></li>
      </ul>
    </li>
    <li><a href="#Requirements">Requirements</a></li>
    <li><a href="#Usage">Usage</a></li>
    <li><a href="#Code Explanation">Code Explanation</a></li>
  </ol>
</details>

# Process target

This project demonstrates how to merge and clean DataFrames using PySpark, particularly focusing on eliminating duplicates and retaining the most recent records for each entity.

## Process description

The code provided in this repository showcases a Python script that utilizes PySpark, a Python API for Apache Spark, to merge two DataFrames containing financial transaction data from different sources. The script then cleans the data to remove duplicates based on certain columns and retains only the most recent records for each entity.

## Requirements

- Python 3.x
- Apache Spark
- PySpark

## Usage

1. Clone the repository:

    ```
    git clone https://github.com/your-username/pyspark-dataframe-merge-clean.git
    ```

2. Install dependencies:

    ```
    pip install pyspark==3.3.0
    ```

3. Ensure your financial transaction data is available in CSV format and place it in the repository directory with the file names `dataframe1.csv` and `dataframe2.csv`.

4. Run the script:

    ```
    python apache_saprk.py
    ```

## Code Explanation

- The script starts by creating a SparkSession, which is the entry point to Spark functionality in the application.

- It then reads the financial transaction data from two CSV files into separate DataFrames using the `read_csv()` function.

- The DataFrames are merged using the `union()` method, and the 'Date' column is converted to a Date data type.

- Next, the script groups the merged DataFrame by 'ID' and finds the maximum date for each ID using the `groupBy()` and `agg()` functions.

- Finally, it joins the original DataFrame with the DataFrame containing maximum dates, selects the relevant columns, and displays the final cleaned DataFrame.
