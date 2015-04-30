# Finding the Top 10 using Aerospike Aggregations

##Problem
You want to create a leaderboard of the top 10 scores, or 10 most recent events, using Aerospike as the data store

##Solution
The solution is to use an Aggregation that processes the stream of tuples flowing from a query on a secondary index. The aggregation is done in code on each node in the cluster and finally aggregated, or reduced, in the client.

### How to build

The source code for this solution is available on GitHub,  
https://github.com/helipilot50/aerospike-top-10-aggregation.git. 


This example requires a working Java development environment (Java 6 and above) including Maven (Maven 2). The Aerospike Java client will be downloaded from Maven Central as part of the build.

After cloning the repository, use maven to build the jar files. From the root directory of the project, issue the following command:
```bash
mvn clean package
```
A JAR file will be produced in the directory 'target', `aerospike-log4j-1.0.0-full.jar`

###Using the solution
The jar is complete with all the dependencies packaged and it is intended to be loaded and use by Log4j. This means that the jar must be available on the class path.

A Unit test in the `src/test/java` directory demonstrates the usage, and the `log4j.properties` file in the `src/test/resources` directory demonstrates how to configure the appender.

##Discussion

The Java code is very simple, the class `AerospikeLog4jAppender` subclasses `AppenderSkeleton`.

n Properties are declared to facilitate the connection and configuration. These are:

