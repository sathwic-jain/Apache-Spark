# CS5052 Practical 1: Apache Spark

## Overview
This repository contains the implementation of a console-based application and a web-based application using Python and Apache Spark for analyzing a large dataset on pupil absences in schools in England from 2006 to 2018.

## Getting Started
To run the console application:
- Ensure you have Python v3.11.2 installed along with pip v22.3.1.
- Install all the dependencies using:
  ```
  pip install -r requirements.txt
  ```
- Run the console application using:
  ```
  python console.py
  ```

To run the web-based application:
- Execute the following command:
  ```
  streamlit run webapp.py
  ```

## Structure
The project structure is organized into the following files:

- `console.py`: Implements the console-based application.
- `create.py`: Contains functions to create a PySpark session and handle CSV file reading.
- `enrolls_by_la.py`: Provides functions for displaying pupil enrolments by local authority.
- `authorised_absence.py`: Functions for displaying authorised absences by school type.
- `unauth_absence.py`: Functions for unauthorised absences breakdown by region or local authority.
- `top_three_auth_absence.py`: Functions to list the top 3 reasons for authorised absences in each year.
- `compare_two_la.py`: Allows users to compare two local authorities in a given year.
- `region_performance_2006_18.py`: Implements the exploration of region performance from 2006 to 2018.
- `webapp.py`: Web application with Streamlit library for interactive data visualization.

## Report
The project includes a comprehensive report (included in the repository) that provides a detailed overview of the implemented features, any encountered problems and their solutions, the implemented functionalities in Parts 1, 2, and 3, along with data analysis, charts, and key insights derived from the dataset. Additionally, a video demonstrating the execution of the application is included.

## Conclusion
The project successfully utilizes Apache Spark, Python, and PySpark's capabilities for dataset analysis and visualization. It includes data exploration, cleaning, and implementation of various functionalities, providing valuable insights and learning experiences in data analysis with PySpark.
