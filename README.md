**How to run the application:**
- Run via IDE
  Just run java class as Java Application -> **com.akg.dbperfcheck.DBPerfCheckApplication.java**

- Build with maven and Run the jar file.<br/>
  1.&nbsp;Go to the project home directory and run<br/>
  	&nbsp;&nbsp;**mvn clean install**<br/>
  2.&nbsp;Copy/Paste dbperfcheck-0.0.1-SNAPSHOT.jar and src/main/resources directory where you want, for example let be directory TEST.<br/>
  3.&nbsp;Go to TEST directory open application.yaml file and set the proper values for the properties ( input-data-file, use-cases, etc.<br/>
  4.&nbsp;Run.<br/>
    &nbsp;&nbsp;**java -jar dbperfcheck-0.0.1-SNAPSHOT.jar -Dspring.config.location=.**<br/>


**Configurations**
  - the main configuration is at src/main/resources/application.yml
  - each DB has its own file for example for Elasticsearch the file is src/main/resources/application-elasticsearch.yml  
  - Docker setup can be found at directory src/main/resources/docker. Each DB has its own sub-directory, for example, Cassandra directory is src/main/resources/docker/cassandra
  - When the particular docker container is run it has to be run commands that are important for proper setup, for example, the creation of the tables, indexes, etc.
  For each DB example of such commands can be found at docker folder -> https://github.com/atanasgegov/db-perf-check/blob/main/src/main/resources/docker/docker-setup-helpful-commands


**How to integrate new DB to the project.**<br/>
  The following steps should be executed.
  - [Required] -> Create com.akg.dbperfcheck.config.{DBNAME}Config.java ( example ElasitcsearchConfig.java )
  - [Required] -> Create com.akg.dbperfcheck.service.{DBNAME}Command.java ( example ElasitcsearchCommand.java )
  - [Required] -> Create configuration file application-{DBNAME}.yml  ( example application-elasticsearch.yml )
  - [Optional] -> Create docker folder at PROJECT_HOME/src/main/resources/docker with docker-compose file in it connected for the run of DB container ( for example PROJECT_HOME/src/main/resources/docker/elasticsearch/docker-compose.yml ). 
