## Project Data Lake
### Introduction
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. 
Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.


### Project Description
In this project, I'll apply what've learned on Spark and data lakes to build an ETL pipeline for a data lake hosted on S3. loading data from S3, process the data into analytics tables using Spark, and load them(parquet files) back into S3 and deploy this Spark process on a cluster using AWS.

So as a data engineer , My task is:
- Import pyspark.sql and all need libraries.
- Create spark session using python API.
- Read(load) and extracts JSON file(song data and log data) from S3, 
- Create a set of dimensional tables from spark data frame.
- Write back to the parquet files and save back on S3. 



## Database schema:
1.Fact table: songplays  ( songplay_id,start_time , user_id , level, song_id , artist_id , session_id , location , user_agent)

2.Dimensional tables:

- Table: songs (song_id , title , artist_id , year , duration )

- Table: artists (artist_id , name , location , latitude , longitude ) 

- Table: users (user_id , first_name , last_name , gender , level )

- Table time (start_time , hour , day , week , month , year , weekday ) 

                                
 
## ETL pipeline:

- 1.Create spark session using python API.
- 2.Using spark API to Read data from S3 , transform to spark data frame. 
(Song data: s3://udacity-dend/song_data ,Log data json path: s3://udacity-dend/log_json_path.json
- and Log data: s3://udacity-dend/log_data).
- 3.Create fact table,dimension table and extract useful columns from spark data frame
- 4.Write tables back to parquet files that store in S3





## How to run python script.
- Environment requirement: AWS,IAM,Spark. 

- 1, Collect yourself "AWS_ACCESS_KEY_ID" and "AWS_SECRET_ACCESS_KEY" fillwith in configuration file "dl.cfg". 

- 2, Create a S3 for save parquet files. 

- 3, Run "python etl.py" to Extract data from S3 and extract data into the tables.

- 4, Specify the S3 path to '''input_data = "data source path" ''' and '''output_data = "output data path"''',in this example data source from "s3://udacity-dend/song_data", you can change to your own data source path.
    

## Note
Because data source in "s3://udacity-dend/song_data" is miss, So I use local file data,just in "/data/", and output data to local path "./test2".
