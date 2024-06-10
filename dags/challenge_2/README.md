# Challenge 2

You can find the ERD diagram of the data model on the gile `ERD_diagram.png`.
You can also find screenshot of the DAG and query results in the screenshots folder
<br><br>
Solution:
Create a Star schema where the facts are: **logins** and **transactions**.<br> 
Transactions fact include both deposits and withdrawals and to identify the type of transaction a column `transaction_type` 
was added; the granularity of this table is at transaction level.<br>
For login fact, I filtered the event_type == login so that the granularity of this table is each login that a user made. <br><br>
The dimension tables are **user** and **date**. Date dim is a generated table, I got the earliest date that we had and generate records for each day of the year from that date until the end of this year.

I used Athena to create the tables on top of the s3 csv's to visualize the data and create the queries to solve some business questions. 

**Future improvements:** <br>
1. Currently the code is using pandas, for the amount of data it runs fine with it but we could use PySpark to use distributed processing in case we have more data in the future.
2. Instead of Athena we could use Snowflake as a warehouse solution since it is a warehouse that has columnar storage and could be leveraged to make more complex data transformations. Athena works well for now since we don't have too much data.
3. Load the fact tables with only the delta instead of a whole load, only load the modified, added or deleted records.
4. Include more data to the user dimension, and we could include more dimensions like converting currency to a dimension or if we had location data abour from where the transaction was made we could include a city dimension. 
5. Date dim last date is the last day of this year, maybe we could add more dates in a more dynamic way. 
