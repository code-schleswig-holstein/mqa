# Open Data Metadata Quality Analysis

Run `1_collect.py` first. It will query GovData's SPARQL endpoint. It will take approx. 1.5 minutes to run the queries. The program will write three files with the numbers:

- `datasets.csv` - absolute numbers for datasets
- `distributions.csv` - absolute numbers for distributions
- `relative.csv` - relative values for datasets and distributions

The second program `2_analyze.py` will read the data in `relative.csv` and generate diagrams.
