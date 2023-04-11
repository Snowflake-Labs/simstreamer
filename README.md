# IceStream

## NOTICE

- This application is not part of the Snowflake Service and is governed by the terms in LICENSE file, unless expressly agreed to in writing.  You use this application at your own risk, and Snowflake has no obligation to support your use of this application.

- This utility is not production grade code rather than a quickly written sample to demonstrate a Snowflake functionality. It is not optimized for performance. 


## Introduction

This utility leverages the Java Snowflake Ingest SDK to stream fake but realistic data into Snowflake. It's leveraging the Java Faker project for this purpose. Currently, the only use case supported is tpch data. It's a multi-threaded program which allows to configure the number of sessions to open to be able to load the data in parallel.

NOTE: THIS IS NOT PRODUCTION GRADE CODE.

There are 2 ways to use this:

- If you want to extend the utility to add your own use cases, you can pull the repo, add your use case and compile from source using maven
- You could also use the jar executable IceStream.jar as-is for the existing use case.

## Prerequisite

- Clone this repository.
- This has been developed using OpenJDK version 19.0.2. So you will need this JDK level:

```
java -version
openjdk version "19.0.2" 2023-01-17
OpenJDK Runtime Environment (build 19.0.2+7-44)
OpenJDK 64-Bit Server VM (build 19.0.2+7-44, mixed mode, sharing)
```
- If you need to compile from source, you need [Maven](https://maven.apache.org/download.cgi)

- You will need to set-up a [private key-pair authentication](https://docs.snowflake.com/en/user-guide/key-pair-auth#configuring-key-pair-authentication) with Snowflake.

## Use Cases

### tpch Use Case

Currently, the only use case supported. This will generate fake data in LINEITEM fact table, ensuring that the foreign keys are in the range of all the dimension tables. The current range is configured for SF (Scale Factor) 1. But, you could customize the range in simulation.json file.

- Create a target lineitem table in a database of your choice as follows:

```
create table lineitem like snowflake_sample_data.tpch_sf1.lineitem;
```

## Use compiled jar

- You can use the 'IceStream.jar' compiled jar from the target subdirectory.
- You need to customize the connection.jar file with [properties](https://docs.snowflake.com/en/user-guide/data-load-snowpipe-streaming-overview#snowpipe-streaming-properties) pertaining to your Snowflake target account.
- You can leave the simulation.json as-is, and customize the number of rows you want to stream and the number of channels you want to open to load rowsets in parallel. 
- You can now execute the jar as follows:

```
java -jar IceStream.jar --help
Unrecognized option: --help
usage: Usage:
 -c,--connection_parms <arg>   Path to connection.json file
 -s,--simulation_parms <arg>   Path to simulation.json file
 -u,--use-case <arg>           Use Cases supported: tpch
```

- Once you have filled the connection.json & customized the simulation.json, you can run it as following:

```
java -jar IceStream.jar -c connection.json -s simulation.json -u tpch
```

## Compile from source

- Download this repo
- Run the following mvn command from the directory where pom.xml is located:

```
mvn package
```

- This will generate a `IceStream-1.0-SNAPSHOT-jar-with-dependencies.jar` under target directory that you can rename as `IceStream.jar`.

