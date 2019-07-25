Purpose: Sparkify a new startup company and it's analytics team wanted to perform some amalysis on their music streaming app to know the songs users are listening to. This project contains the Sparkity data modelling with relational database and the entire ETL process to extract, process and load the data into tables. This will help users from Sparkify to query the data based on their business requirements and identify the areas to improve their music streaming app.

Project summary: Create the sparkify data in structured way and store it in realtional tables, so that Sparkify team can query the data from different tables based on their business needs 


Input data : Resides in JSON log and song files which are stored in local directories
DB         : PostgreSQL

ETL Schema : Star

Tables: DIM_songs - contains song information including song_id, title, artist_id, year, duration
        DIM_artists - contains artist information including artist_id, name, location, lattitude, longitude
        DIM_users - contains users information including user_id, first_name, last_name, gender, level
        DIM_time - contains song play time information including date and time values
        FACT_songplays - Fact table -consists most of the information combined form song and log data files
       
Files:
1. create_tables.py - contains the code for creating/restting database and all tables mentioned above
2. etl.py           - contains the code to Extract the data from JSON files, process it and load into the tables
3. test.ipynb       - contains the code For testing the results from etl.py
4. data             - folder contains the entire sparkify music streaming app data in form of JSON log and song files


Execution guidelines:
1. Execute create_tables.py first in terminal for creating/resetting database and all tables mentioned above
2. Execute etl.py after succesful completion of the first step for loading the data from JSON files into tables
3. Test your results by executing test.ipynb. Each time you run this, remember to restart this notebook to close the connection to your database. Otherwise, you won't be able to run your code in create_tables.py, etl.py, or etl.ipynb files since you can't make multiple connections to the database sparkifydb
        