The loading of files and identifying the top 50 clients can be done in different ways.

Method 1: Using an ETL tool like informatica, Datastage, SSIS, etc we can load the files to onprem databases like oracle, sql server, etc and run queries and generate the report.

Method 2: We can load the files to cloud storage and create a notebook to read the files and write to tables and generate the report.

I followed method 2 , I am also comfortable using method 1. 

With the actual production data set we can also generate a report how frequently a customer is coming back, is it monthly, quartely , 6 months... and also on avg how much they are spending. Depending on it, a business owner can send some offers on some products to the targeted customers.


Step 1: Manually Copied the .csv files to azure blob storage
Note: This process can be automated using azure data factory or using python or any other tool, but just for this test purpose I did not take that route.

Step 2: Created table for each file in dataricks catalog by running the scripts.

Step 3: Created a databricks note book to read the files from azure blob storage and write to tables.
If we have more files, this process can be improved by running the code in loop and read each file and load in to table. 

Step 4: Wrote script to identify the top 50 clients that have accumulated the most loyalty points since 2018-01-01 00:00:00
Logic: First I analysed the data in each table and found that the loyalty points are in two tables, services and in purchases.
So when a customer goes to a salon/spa he/she can get points for the services they get and also if they buy some products they can also accumulate some points.
That means I need to consider the loyalty points of both (services and purchases). For this I did a union all, group by and also used ranking function to idenify the top 50.

Step 5: Validated the results from my script and manually compared to the data against the csv files amd data matched.




