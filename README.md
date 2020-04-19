# Hadoop - Kafka Data Analysis Application

Project purpose is to create data processing applications using APIs of Apache Hadoop, Apache Hive and Apache Kafka Frameworks.

## Prerequisites

● Cloudera Quickstart VM (VirtualBox) 5.13 has been used for this project.

● Cluster address is localhost.

Note: Maven dependencies of the project has been configured to create a fat jar. Therefore, there is no requirement for running application with additional jar on Big Data cluster (Cloudera Cluster).

## Installing

Create fat jar on project directory using below maven command. Expected artifact file will be data-analysis-1.0-SNAPSHOT-jar-with-dependencies.jar (fat-jar).

```HTML
mvn clean package
```

## Getting Started

This project have requirements for Hadoop/Hive data analysis and Kafka data analysis

## Hadoop data loading & Hive data analysis
● Task #1: Load the data into Hadoop, and perform a count of the records. List the steps you took to get the data in and to make the count.

Hive Metastore table has been created on default schema using below HQL.
```HTML
Create table search_results (
 userid string,
 unix_time bigint,
 hotelresults map<int,struct<advertisers:map<string,array<struct<eurocents:int,breakfast:boolean>>>>>);
```
data.dat file has been copied to Cloudera VM home directory using MobaXTerm FTP. Below script has been used for copying data to hdfs.
```HTML
hdfs dfs -put /home/cloudera/data.dat /user/hive/warehouse/search_results
```
Note: Hue UI can be used for copying data to HDFS as well.

Below query has been used for data row counting
```HTML
select count(*) from search_results;

Result:1012
```

● Task #2: Execute a query to find per advertiser and hotel the cheapest price that was offered. Provide the query, and the result you got.

Below HQL has been used

```HTML
SELECT main.hotel_id,
       advs.advertiser,
       min(cost.eurocents) AS min_cost
FROM search_results LATERAL VIEW explode(hotelresults) main AS hotel_id,
                                 advertisers LATERAL VIEW explode(advertisers.advertisers) advs AS advertiser,
                                                          costs LATERAL VIEW inline(costs) cost AS eurocents,
                                                                             breakfast
GROUP BY main.hotel_id,
         advs.advertiser
ORDER BY main.hotel_id,min_cost;
```

Results:
Time taken: 38.017 seconds, Fetched: 78 row(s)

main.hotel_id | advs.advertiser | min_cost
--- | --- | ---
6032 | Mercure | 3804
6032 | Amoma | 3804
6032 | Destinia | 3814
6032 | booking.com | 3852
6032 | Tui.com | 3893
6032 | expedia | 3924
6033 | Destinia | 3808
6033 | Mercure | 3810
6033 | Amoma | 3815
6033 | expedia | 3819
6033 | Tui.com | 3840
6033 | booking.com | 3845
6035 | expedia | 3804
6035 | Mercure | 3854
6035 | booking.com | 3860
6035 | Destinia | 3879
6035 | Amoma | 3919
6035 | Tui.com | 3970
6036 | expedia | 3813
6036 | Destinia | 3823
6036 | Amoma | 3839
6036 | booking.com | 3855
6036 | Tui.com | 3884
6036 | Mercure | 3965
7045 | Destinia | 3805
7045 | expedia | 3812
7045 | Tui.com | 3839
7045 | booking.com | 3847
7045 | Amoma | 3847
7045 | Mercure | 3879
7046 | Tui.com | 3803
7046 | Destinia | 3812
7046 | expedia | 3814
7046 | Mercure | 3836
7046 | Amoma | 3857
7046 | booking.com | 3961
7047 | Amoma | 3805
7047 | Destinia | 3823
7047 | booking.com | 3832
7047 | Tui.com | 3832
7047 | Mercure | 3850
7047 | expedia | 3945
8001 | Destinia | 3800
8001 | Amoma | 3808
8001 | Mercure | 3829
8001 | expedia | 3833
8001 | booking.com | 3835
8001 | Tui.com | 3892
8002 | Destinia | 3802
8002 | Tui.com | 3807
8002 | booking.com | 3817
8002 | Mercure | 3831
8002 | Amoma | 3841
8002 | expedia | 3869
9089 | Amoma | 3804
9089 | booking.com | 3814
9089 | expedia | 3819
9089 | Destinia | 3820
9089 | Tui.com | 3859
9089 | Mercure | 3863
9090 | Mercure | 3802
9090 | expedia | 3816
9090 | Tui.com | 3837
9090 | Destinia | 3839
9090 | booking.com | 3884
9090 | Amoma | 3927
9091 | expedia | 3802
9091 | Tui.com | 3819
9091 | booking.com | 3846
9091 | Amoma | 3853
9091 | Destinia | 3871
9091 | Mercure | 3875
9092 | Destinia | 3807
9092 | booking.com | 3807
9092 | expedia | 3817
9092 | Tui.com | 3824
9092 | Amoma | 3893
9092 | Mercure | 3910

● Task #3: For each search generate a list containing the cheapest price per hotel that offers breakfast. Again, please provide the query you used and the result.

```HTML
SELECT a.userid,
       a.unix_time,
       collect_list(a.min_cost) AS min_list
FROM
  (SELECT rawdata.userid,
          rawdata.unix_time,
          hotel_results.hotel_id,
          min(cost.eurocents) AS min_cost
   FROM search_results rawdata LATERAL VIEW explode(hotelresults) hotel_results AS hotel_id,
                                            advertisers LATERAL VIEW explode(advertisers.advertisers) advs AS advertiser,
                                                                     costs LATERAL VIEW inline(costs) cost AS eurocents,
                                                                                        breakfast
   WHERE cost.breakfast=TRUE
   GROUP BY rawdata.userid,
            rawdata.unix_time,
            hotel_results.hotel_id) a
GROUP BY a.userid,
         a.unix_time;
```
Results:
0 results.
Time taken: 21.483 seconds
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Hadoop job information for Stage-1: number of mappers: 1; number of reducers: 1

● Task #4: Generate the list from "task 3" with more efficiency (think about UDFs!). Provide all resources to understand your solution, and measure the difference in execution times.
  
In order to deserialize "hotelresults" data to map<int,struct<advertisers:map<string,array<struct<eurocents:int,breakfast:boolean>>>>>, Apache Hive SerDe API is used. The executable jar which was achieved after running mvn clean package command has been copied to /user/cloudera/udf HDFS path.

```HTML
hdfs dfs -put /home/cloudera/data-analysis-1.0-SNAPSHOT-jar-with-dependencies.jar /user/cloudera/udf
```
Note: Hue UI can be used for copying data to HDFS as well.

In the hive shell, below commands are used for adding and defining developed UDF
```HTML
add jar hdfs://localhost:8020/user/cloudera/udf/data-analysis-1.0-SNAPSHOT-jar-with-dependencies.jar;
create function fn_min_cost_with_breakfast AS 'com.company.hive.udf.CheapestPricePerHotel';
```
UDF returns each minimum cost of the hotel, if the breakfast is provided. If it is not provided, returns as null

```HTML
SELECT userid,
          unix_time,
          fn_min_cost_with_breakfast(hotelresults) AS min_cost,hotelresults
   FROM default.search_results where fn_min_cost_with_breakfast(hotelresults) is not null;
```
Time taken: 14.755 seconds, Fetched: 0 row(s)

Total jobs = 1
Launching Job 1 out of 1
Number of reduce tasks is set to 0 since there's no reduce operator
Hadoop job information for Stage-1: number of mappers: 1; number of reducers: 0

### Conclusion
In task 3, in order to find breakfast hotel, grouping is used. It creates shuffling for reducer. But, in task 4, using UDF, in collection api level, minimum cost has been tried to be found, and eliminates the shuffling for reducer.

## Kafka stream event aggregation

Use the attached Kafka logs to aggregate the events by time and per hotel/hotel location in a streaming fashion.

The Kafka log directory of the Cloudera quickstart 5.13 VM is /var/local/kafka/data

Whole files and directories have been copied to /var/local/kafka/data VM directory.

For reading topics, kafka command is used for each topic directory.
```HTML
kafka-console-consumer --bootstrap-server localhost:9092 --topic data --from-beginning
kafka-console-consumer --bootstrap-server localhost:9092 --topic instructions0 --from-beginning
kafka-console-consumer --bootstrap-server localhost:9092 --topic intro --from-beginning
kafka-console-consumer --bootstrap-server localhost:9092 --topic locations --from-beginning
```
After reading all of these topics, They could be seen under zookeeper's registered list of topics as below

```HTML
kafka-topics --zookeeper localhost:2181 --list

data
instructions0
intro
locations
 ```

instructions0 topic says;

```HTML
Welcome back! in the data topic we stored some information about interactions of users with some hotels. you will find a comma separated list of values in each of the "data"s topics messages. Every message consist of the hotels ID and the type of user interaction (clicks and views eg.). Each message will also carry a timestamp of when the event happended. The assignment is to write an application that will consume these events and will count per hotel and action a sum of how often this event occured. ideally outputting one row per each hotel containing the two sums for clicks and views per every 30 minutes timeframe (0.00:00 - 0.29:59 0.30:00 - 0.59:59 1:00:00 - 1.29:59 eg) we wich you good luck and that you enjoy the challange. your trivago team
 ```
Kafka Streaming application can be used as below on VM
```HTML
java -cp data-analysis-1.0-SNAPSHOT-jar-with-dependencies.jar com.company.kafka.streaming.HotelActionEventAggregation 
 ```
Aggregated data is stored on windowedAggregation-output-topic topic

Topic data can be checked as below kafka-console-consumer tool

```HTML
kafka-console-consumer --bootstrap-server localhost:9092 --topic windowedAggregation-output-topic --from-beginning --formatter kafka.tools.DefaultMessageFormatter --property print.key=true --property print.value=true --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer 
 ```

## Running the tests

Test classes can found under src/test/java/com/company directory.

```HTML
CheapestPricePerHotelTest.class
ConfigCreatorTest.class
```
### Break down into end to end tests

<li>Exception handling has been checked for initializer</li>
<li>Kafka Streaming Config generation correctness has been checked</li>  

## Deployment

 This project can be run as JAR in the;
 
 <li>On a Cloudera Quickstart 5.13 VM</li>
 <li>On a Cloudera server hosted by cloud provider</li>
 <li>In a Cloudera Quickstart 5.13 container</li>
 <li>In a Cloudera Quickstart 5.13 container hosted by cloud provider</li>

## Built With

* [Maven](https://maven.apache.org/) - Dependency Management
* [Apache Kafka 2.2.1](https://kafka.apache.org/) - Kafka Streaming API
* [Apache Hadoop 2.6.0](https://hadoop.apache.org/) - Hadoop File System Configuration
* [Apache Hive 1.1.0](https://hive.apache.org/) - Hive SerDe
* [junit 4.13](https://junit.org/junit4/index.html) - JUnit
 

## Authors

Onur Tokat

## Limitations

* There is no limitation.