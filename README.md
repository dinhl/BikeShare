# BikeShare
##### This is a project I completed during the Insight Data Engineering program (Seattle, Winter 2020)
---
## Project type: 
Ingesting and joining of multiple large and diverse datasets to build an ETL pipeline

## Project ideas:
Bike Share is very popular in many cities. Besides bus and taxi, Bike Share is one of the convenient transportation methods, especially commute in short distance. To have a deeper understanding about how does this eco-friendly commute trends develop over the years in different locations, this project combined the seperated data sets collected from multiple sources to analyze this business model.

## Project goals:
* Ingested Bike Share data from 10 data sources for analyzing eco-friendly commute trend in 10 different cities.
* Built an ETL pipeline by extracting the data from AWS S3, applying PySpark to transform and loading the output into AWS PostgreSQL.
* Implemented Dash framework to create an analytical web application to query the data for user visualization.

## Business use cases:
Analyzing the data sets to answer the questions:

* When do people use Bike Share?
  * How does usage change over the year, the month?
  
* Who are using Bike Share?
  * Members vs. casual users

From the analyzation, the Bike Share investors can make decisions about:

  * Should I invest more bikes for this city?
  
  * Should I invest this business model in a new city?

## Tech Stack
* The 10 collected Bike Share data are stored at AWS S3.
* A Spark cluster is used to extract and transform the raw data in parallel.
* A AWS RDS PostgreSQL database is used to store the combined dataset ready for receiving queries from frontend.
* Dash is used to build the frontend that dispaly results of the Bike Share information.

![](image/techStack_2.PNG)
