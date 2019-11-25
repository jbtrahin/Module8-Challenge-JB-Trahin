# Module8-Challenge-JB-Trahin
Module 8 Challenge on using ETL process to create data pipelines using Python, Jupyter Notebook and SQL.


## Project Overview

In this challenge, we will write a Python script that performs all three ETL steps on the Wikipedia and Kaggle data. We'll perform exploratory data analysis, and add code to handle potentially unforeseen errors due to changes in the underlying data.

The goals for this challenge are to:
1. Create an automated ETL pipeline.
2. Extract data from multiple sources.
3. Clean and transform the data automatically using Pandas and regular expressions.
4. Load new data into PostgreSQL.

## Resources

- Data Source: wikipedia-movies.json, movies_metadata.csv, ratings.csv
- Database: movie_data
- Software: pgAdmin 4.13, Pandas, Jupyter Notebook, Anaconda 4.7.12

## Assumptions
In order to build a function and automate the ETL process for the challenge, we made the following assumptions:

1. The updated data will stay in the same formats:
	- We're creating a function that will extract, transform and load updated csv files if, and only if, the new data is in the same format as the intial datasets. We know that there is a possibility for the data structure to change overtime and early data inspection of new files will be crucial.

2. There is competing data between the different datasets. Based on observed patterns, we have built a system to transform for the best possible outcome:
	- We're assuming that the future datasets will follow the same pattern as the initialone.

Current resolution system of competing data patterns

| Wiki                 | MovieLens                | Resolution                                     |  
|:--------------------:|:------------------------:|:----------------------------------------------:|
| title_wiki           | title_kaggle             | Drop Wikipedia.                                | 
| running_time         | runtime                  | Keep Kaggle; fill in zeros with Wikipedia data.| 
| budget_wiki          | budget_kaggle            | Keep Kaggle; fill in zeros with Wikipedia data.| 
| box_office           | revenue                  | Keep Kaggle; fill in zeros with Wikipedia data.| 
| release_date_wiki    | release_date_kaggle      | Drop Wikipedia.                                | 
| Language             | original_language        | Drop Wikipedia.                                |
| Production company(s)| production companies     | Drop Wikipedia.                                |

3. We're dropping rows with corrupted data when two movies got merged based on release date filtering:
	- We're assuming that the time range we have selected based on our intial dataser will help us catch and drop outliers from dataset, without deleting good data.

4. We're dropping columns with only one value:
	- We added an exception to print "No columns to drop because of only one value" if this happens with the new dataset. We want the function to keep running withour errors.

5. New updated files will be included in the section "Declaring new files to upload":
	- To allow the function to run properly, files have to be inputted in the right place.
