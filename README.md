# Data Engineering with AWS and AWS Redhsift
A basic ETL process from S3 to Redshift to Business Intelligence using Python and AWS resources.

## Introduction
This project builds a pipeline from AWS S3 storage to a Redshift Cloud hosted Cluster where an ETL process extracts the staged data and creates a STAR Schema to allow business users to query the data easier for business intelligence. 

It is meant to demonstrate in simple form how the data engineering life cycle is represented in real time and in its simplest form. Typically, the users submit get/put requests from various applications represented in the first three sources below. Then data is stored//staged and ultimately ingested into a transformation phase (STAR schema used here) and finally an analytics service is provided to end users.

Below we load data from an S3 bucket to a Redshift database created on a cluster where a STAR schema is used for Business Intelligence. 

## Objective
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As a data engineer, we are tasked with building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights into what songs their users are listening to. We'll be able to test the database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.

[See the Redshift Cluster and ETL process](https://github.com/jkenney0501/AWS-Data-Engineering-Project-/blob/main/create_cluster_to_ETL.ipynb)

### Process Flow for Sparkify
<img src="AWS Process Flow.png" />

The process flow demonstrates the data flowing from various sources to the AWS S3 bucket where it is staged. After staging in S3, python is utilized to extract the data into a STAR schema where it is hosted on a Redshfit cluster and can be queried by business users.

 

### Redshift Star Schema used in Spakify Cluster
<img src="ER.png" />

Below is a representation of the STAR schema used for this project after the ETL process is finished and queries for business intellignece can begin producing valyable insights.



### Testing The Data

[See the Redshift Cluster and ETL process](https://github.com/jkenney0501/AWS-Data-Engineering-Project-/blob/main/create_cluster_to_ETL.ipynb)

After buiding the pipline succesfully, we need to run tsome test queries on iur schema to validate its functionality.

Some simple queries can include:

` %sql SELECT * FROM songplays LIMIT 3;`

` %sql SELECT * FROM users LIMIT 3; `

To more complex queries:

` SELECT 
        u.first_name,
        u.last_name,
        u.gender,
        f.level,
        a.name as artist_name,
        f.user_agent
        FROM songplays AS f
        JOIN users AS u ON f.user_id = u.user_id
        JOIN artists AS a ON f.artist_id = a.artist_id
        WHERE f.level != 'free'
        LIMIT 10;
   """ ` 

<img src="sql_complex.PNG" />


After running the above queries, we can see the STAR schema is successful! 

Dont forget to delete the cluster and all resources!
