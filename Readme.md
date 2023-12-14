# SparkStream Analytics

![PySpark Logo](./images/PySpark.png)

## Overview

This project showcases PySpark's capabilities for streaming data analysis on a set of CSV files. It guides you through the steps of creating a streaming DataFrame, performing data transformations, and writing the results to output files.

## Prerequisites

Before you start, ensure you have the following:

- **Python:** Make sure Python is installed on your system, preferably Python 3.x.
  
- **Apache Spark and PySpark:** Install Apache Spark and PySpark to efficiently work with streaming data. Follow the installation instructions on the [Apache Spark website](https://spark.apache.org/downloads.html).

- **CSV Files:** Prepare a directory containing the CSV files for streaming and analysis. In this project, we use financial data (e.g., stock prices) in CSV format.

## Project Structure

Here's a brief overview of the project structure:

- **README.md:** The documentation you are currently reading.
  
- **Streaming_Practical_Session:** The Python script that performs PySpark streaming data analysis.

## Getting Started

1. **Define the Schema:** Create a schema for the streaming data by specifying column names and data types.

2. **Create the Streaming DataFrame:** Use PySpark to create a streaming DataFrame by reading data from the specified directory. Ensure the data source format is set to "csv" and provide the defined schema.

3. **Check Streaming Status:** Verify that the streaming DataFrame is correctly configured for streaming data. You should see a True output for `df.isStreaming`.

4. **Create Stream Writer:** Set up a stream writer to handle the data, using an in-memory writer for intermediate results.

5. **Start the Write Stream:** Begin the stream writing process and ensure it's working correctly. You can check by running queries to display the data.

6. **Data Preprocessing:** Perform data preprocessing tasks, such as removing rows with all null values and creating a new column to calculate the difference between "High" and "Low" prices.

7. **Create a New Stream Writer:** Set up a new stream writer for the modified data, and start the write stream.

8. **Write to Files:** Instead of writing to memory, write the generated data into output files, specifying the output path and checkpoint location.

9. **Stop the Query:** Stop the streaming query once the data is written to files.

10. **Read Generated Files:** Read the data from the generated output files using a predefined schema. Sort the DataFrame based on the "ID" column.

## Results

The project results include:

- Streaming data analysis on CSV files.
  
- Data preprocessing and transformation.

- Writing the modified data to output files.

Access the results in the generated output files located in the "outputstream" directory.

## Contributing

If you find any issues, have suggestions for improvements, or would like to contribute, please feel free to open an issue or create a pull request. We welcome collaboration and contributions from the community.
