
# Data Aggregation Solution

This repository provides two solutions for aggregating data from two datasets based on specific criteria. The solutions are implemented using two different frameworks: Pandas and Apache Beam Python.

## Framework 1: Pandas

To generate the sample dataset using the Pandas framework, run the following command:

```bash
python pandas_framework_solution.py
```

This will save the output CSV in the same folder with the name `pandas_framework_output.csv`.

## Framework 2: Apache Beam Python

To generate the sample dataset using the Apache Beam framework, run the following command:

```bash
python apache_beam_framework_solution.py
```

This will save the output CSV in the same folder with the name `apache_beam_framework_output.csv`.
Apache Beam is designed for parallel processes so the data written will not be in order.

## Note

The requirement states: `"Also create a new record to add a total for each of legal entity, counterparty, and tier."` Please note that this statement is a bit ambiguous in the `sample_test.txt` file. So it is assumed that the `"Total"` here means the number of rows within each group. 

