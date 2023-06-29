# MovieLens Analytics with Spark and Python
This repository contains the code and analysis for solving analytical questions on the MovieLens dataset using Apache Spark and Python. The dataset used in this project is the semi-structured MovieLens dataset with a million records.

## Project Overview
The goal of this project is to explore and analyze the MovieLens dataset using Spark's RDD, Spark SQL, and Spark DataFrames. By leveraging different forms of Spark APIs, we aim to extract valuable insights about users and movies.

## Features
- Utilizes Apache Spark for processing and analysis
- Implements Spark RDD, Spark SQL, and Spark DataFrames
- Solves analytical questions based on the MovieLens dataset
- Provides code examples and explanations for each analysis
- Runs on the pyspark shell environment

## Dataset
The MovieLens dataset used in this project contains a million records of user ratings, movie information, and tags. The dataset is semi-structured and provides valuable information for movie analytics.

## How to Use
To use this project, follow these steps:

1. Clone the repository: `https://github.com/NarveerDhariwal/Movie-Analytics-project.git`
2. Set up the Spark environment and ensure that you have the required dependencies.
3. Launch the pyspark shell: `pyspark`
4. Navigate to the directory containing the project files.
5. Execute the Python scripts in the pyspark shell to perform the analysis and answer the analytical questions.
6. Refer to the code comments and analysis descriptions for understanding the approach and insights gained.

## Analytical Questions

Spark RDD
1.	What are the top 10 most viewed movies?
2.	What are the distinct list of genres available?
3.	How many movies for each genre?
4.	How many movies are starting with numbers or letters (Example: Starting with 1/2/3../A/B/C..Z)?
5.	List the latest released movies

Spark SQL
1.	Create tables for movies.dat, users.dat and ratings.dat: Saving Tables from Spark SQL
2.	Find the list of the oldest released movies.
3.	How many movies are released each year?
4.	How many number of movies are there for each rating?
5.	How many users have rated each movie?
6.	What is the total rating for each movie?
7.	What is the average rating for each movie?

Spark Data Frames
1.	Prepare Movies data: Extracting the Year and Genre from the Text
2.	Prepare Users data: Loading a double delimited csv file
3.	Prepare Ratings data: Programmatically specifying a schema for the data frame
4.	Import Data from URL: Scala
5.	Save table without defining DDL in Hive
6.	Broadcast Variable example
7.	Accumulator example

## Results and Findings
The analysis conducted on the MovieLens dataset using Spark and Python has provided valuable insights into user behaviour and movie preferences. The results and findings are documented within each analysis, showcasing the power and capabilities of Apache Spark in big data analytics.

## Future Work
This project can be further expanded and improved by incorporating additional analytical questions, visualizations, and machine learning models. It can also be integrated with other data sources or combined with external datasets for more comprehensive analysis.

## Contributions
Contributions to this project are welcome. If you have any ideas, improvements, or bug fixes, feel free to submit a pull request.
## Acknowledgements
We would like to thank the creators of the MovieLens dataset for providing such a comprehensive dataset for movie analytics. We also acknowledge the Apache Spark community for developing a powerful framework for big data processing and analytics.
