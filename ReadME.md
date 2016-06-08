## ReadMe : 
### Here, The required task( described in detail below ) is to simulate data partitioning approaches on-top of an open source relational database management system (i.e. PostgreSQL). 
**The Python script generates four functions that load the input data into a relational table, partition the table using different horizontal fragmentation approaches, and insert new tuples into the right fragment.**

**Input Data:**
The input data is a Movie Rating data set collected from the MovieLens web site (http://movielens.org). The raw data is available as comma separated text files where all ratings are contained in the file ratings.dat

The rating.dat file contains 10 million ratings and 100,000 tag applications applied to 10,000 movies by
72,000 users. Each line of this file represents one rating of one movie by one user, and has the following
format:

                        UserID::MovieID::Rating::Timestamp
### Required Task:
### Below are the steps you need to follow to run this script and description of implemented functions:
1. Download **PostgreSQL** (http://www.postgresql.org)
2. Download rating.dat file from the **MovieLens** website(http://files.grouplens.org/datasets/movielens/ml-10m.zip)
3. The Python function **LoadRatings(  )** takes a file system path that contains the rating.dat file as input. Load Ratings() then load the rating.dat content into a table (saved in PostgreSQL) named Ratings that has the following schema UserID - MovieID - Rating
4. The function **RangePartition()** takes as input: (1) the Ratings table stored in PostgreSQL and (2) an integer value N; that represents the number of partitions. Range Partition() then generates N horizontal fragments of the Ratings table and store them in PostgreSQL. The algorithm partitions the ratings table based on N uniform ranges of the Rating attribute.
5. The function **RoundRobin Partition()** takes as input: (1) the Ratings table stored in PostgreSQL and (2) an integer value N; that represents the number of partitions. The function then generates N horizontal fragments of the Ratings table and stores them in PostgreSQL. The algorithm partitions the ratings table using the round robin partitioning approach.
6. The function **RoundRobinInsert()** takes as input: (1) Ratings table stored in PostgreSQL, (2) UserID, (3) ItemID, (4) Rating. RoundRobin Insert() then inserts a new tuple in the right fragment (of the partitioned ratings table) based on the round robin approach.
7. The function **Range Insert()** takes as input: (1) Ratings table stored in Post- greSQL (2) UserID, (3) ItemID, (4) Rating. Range Insert() then inserts a new tuple in the correct fragment (of the partitioned ratings table) based upon the Rating value.
8. The function **DeletePartitions()** deletes all generated partitions as well as any metadata related to the partitioning scheme.
