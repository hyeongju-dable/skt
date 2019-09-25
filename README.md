**Data Engineer - Test Application:**
Push your code in github or gitlab.
Deploy your application, tools, environment in a fat container.
Let me know your git repository and container address.

**Requirements:**
* Programing language: Scala 2.11, Java 1.8, Python 3.6
* Processing framework: Spark 2.3 or above
* Queuing technology: Kafka 1.10 or above
* Database technology: Postgres or Mariadb

You can use Cloudera, HDP or vanila apache hadoop distribution.

**Database tables:**  
`driver (id BIGINT, date_created TIMESTAMP, name CHARACTER VARYING(255))`  
`passenger (id BIGINT, date_created TIMESTAMP, name CHARACTER VARYING(255))`  
`booking ( id BIGINT, date_created TIMESTAMP, id_driver BIGINT, id_passenger BIGINT, rating INT, start_date TIMESTAMP, end_date TIMESTAMP,  tour_value BIGINT)`  

**Tasks:**
* Task 01:
    Given the DDL above, please create a full database model including keys, constraints or any other requirement necessary.  
    Upload the driver.csv and passenger.csv files to your local database.
    Publish booking.csv to booking topic in kafka.
* Task 02:
    Based on your new database and booking topic, create a report listing the TOP 10 drivers every week from 2016.
    We like drivers with a high tour value and a good average rating.
    For now you can just print the result as a nice list to your log.
* Task 03:
    We do care a lot about relationships between our drivers and passengers.  
    Our goal is to create a very positive user experience for both, thus we promote passengers to request their favorite drivers frequently.  
    In order to know how successful we are at creating a good passenger driver relationship, please provide a list of the   
    TOP 10 strongest relationships between passengers and drivers. This relationship is defined by the number of tours they  
    did together.
* Task 04:
    Your next task is something for the upper management. Since they need to know the overall performance of our company.
    Provide a KPI report containing the yearly figures for bookings, the average driver evaulation and revenue for 2016.
* Task 05:
    The last task is to deliver these KPIs to our Management Reporting Tool.
    This tool can fetch data from our Kafka Queue. Thus setup your own Kafka Broker locally and publish the results of Task 02, Task 03 and Task 04   
    to different kafka topic.
