# San Francisco Fire Incidents - Data Pipeline

This repository contains the data analysis and code implementation required to build a Data Pipeline that loads this [San Francisco fires dataset](https://data.sfgov.org/Public-Safety/Fire-Incidents/wr8u-xric) into a Data Warehouse

### Repository Structure

The Exploration folder contains Jupyter notebooks that include an analysis of the data and the API requirements, which proved quite necessary for a correct posterior implementation. I've also used the notebooks to try and sketch different ideas before the actual coding. On the other hand, the App folder contains a complete implementation of the pipeline. The final deployment, as explained below, makes heavy use of AWS (which seemed the most sensible approach, given what the job description was asking for). The folder AWS Screenshots shows some examples of the AWS Services in use.

### The pipeline, in a nutshell

After gaining an understanding of the data, the behavior of the API, and the requirements of the challenge, the data pipeline that I constructed (App/app.py) runs as follows:
- Every day (as per the requirements), the Socrata API is called, with the very specific SoQL syntax needed to pull the whole dataset (the format for this API call was explored in the notebooks).
- Once the JSON is received, the data is mildly processed in pandas and loaded into AWS S3, as a .csv. The only column that was actually changed was the "point" column, the key of which contained a nested js object as a value. The data inside this key was broken down into latitude and longitude, values which allow for much more useful analysis (e.g., they could be used to create a heatmap).
- The .csv from S3 if loaded into AWS Redshift, the chosen Data Warehousing solution for this challenge. This is done using boto3 to connect to the Redshift Data API. Other options were explored (redshift_connector, AWS Glue), but finally discarded. 
- The table in Redshift was built considering the requirements (i.e., the columns that will be frequently queried and aggregated by the Business Intelligence area), through the use of sort and distribution keys. I have checked the performance of the queries that use agreggations of the columns mentioned in the challenge, and they are fast indeed (this check was done manually; in a production situation, this is one thing that could be done more thorougly). 
- In order to orchestrate the daily run of the pipeline, an instance of AWS Lambda is triggered through AWS EventWatcher. This run is monitored in AWS CloudWatch. 
- I tried to leave the requirements as light as possible. Boto3 is the most used library; pandas is used as well.
- The codebase contains additional developments (for example, a pipelne for AWS RDS - MySQL, which doesn't use S3 at all). These were part of the discovery process but were finally discarded in the deployment to AWS.
- Some screenshots of the AWS services involved can be seen on the AWS Screenshots folder
- Other architectures were explored but this one emerged as the most natural one for the task at hand, using AWS Services.

### How to use the Data Warehouse

The Data Warehouse can be accessed through the use of credentials, be that through a GUI client (Workbench, DBeaver) or through a library (e.g., boto3).
Credentials for accessing the Redshift cluster: 
- cluster_name: redshift-cluster-1
- database: dev
- schema: sf_fires
- table: sf_fires
- user: awsuser
- host: ????
- password: ????? 

To obtain the hostname and password for querying the Data Warehouse, you can send me an email.

Of course, I could make a walkthrough of what things look like in AWS (configurations, Lambda, CloudWatch, Redshift)

### Nice to haves

Due to time constraints, many ideas that I think could improve the solution were discarded: providing a front to show the queries, benchmarking different sorting and distribution possibilities, creating a backup and considering an strategy for downtime, creating a dashboard, monitoring the solution, etc.
