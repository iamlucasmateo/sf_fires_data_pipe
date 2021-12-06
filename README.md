# San Francisco Fire Incidents - Data Pipeline

This repository contains the data analysis and code implementation required to build a Data Pipeline that loads this [San Francisco fires dataset](https://data.sfgov.org/Public-Safety/Fire-Incidents/wr8u-xric) into a Data Warehouse

### Repository Structure

The Exploration folders contains Jupyter notebooks with the analysis of the data and the API requirements, necessary for a correct posterior implementation. I've also used that space to try different ideas for the actual coding. The App folder contains a complete implementation of the pipeline. The final deploymen, as explained below, made heavy use of AWS (which seemed to most sensible approach, given what the job description was asking for).

### The pipeline, in a nutshell

After gaining an understanding of the data, the behavior of the API, and the requirements of the challenge, the data pipeline that I constructed runs as follows:
- Every day (as per the requirements), the Socrata API is called, with the very specific SoQL syntax needed to pull the whole dataset (which was explored during the first part of the development).
- The data is mildly processed in pandas and loaded into AWS S3, as a .csv. The only column that was actually changed was the "point" column, the key of which contained a nested js object / python dictionary as a value. The data inside this key was broken down into latitude and longitude, values which allow for much more useful analysis (e.g., they could be used to create a heatmap).
- The .csv from S3 if loaded into AWS Redshift, the chosen Data Warehousing solution for this challenge. This is done using connecting boto3 to the Redshift Data API. Other options were explored (redshift_connector, AWS Glue), but finally discarded. 
- The table in Redshift was built considering the requirements (i.e., the columns that will be frequently queried and aggregated by the Business Intelligence area), through the use of sort and distribution keys.
- In order to orchestrate the daily run of the pipeline, an instance of AWS Lambda is triggered through AWS EventWatcher. This run is monitored in AWS CloudWatch. 
- I tried to leave the requirements as light as possible. Boto3 is the most used library; pandas is used as well.
- The code base contains additional developments (for example, a pipelne for AWS RDS - MySQL, which doesn't use S3 at all). These were part of the discovery process but were finally discarded in the deployment to AWS.

### Data Warehouse access

In order to obtain credentials for querying the Data Warehouse, you can send me an email.

### Nice to haves

Due to time constraints, many ideas that I think could improve the solution were discarded: providing a front to show the queries, benchmarking different sorting and distribution possibilities, creating a dashboard, etc.
