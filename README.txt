 *How to run the application:*

1. Run via IDE
  Just run java class as Java Application -> com.akg.dbperfcheck.DBPerfCheckApplication.java

2. Build with maven and Run the jar file.
  2.1. Go to the project home directory and run 
  	$mvn clean install
  2.2. Copy/Paste dbperfcheck-0.0.1-SNAPSHOT.jar and src/main/resources directory where you want, for example let be directory TEST
  2.3. Go to TEST directory open application.yaml file and set the proper values for the properties ( input-data-file, use-cases, etc.
  2.4. Run 
    $java -jar dbperfcheck-0.0.1-SNAPSHOT.jar -Dspring.config.location=.


Configurations
  - the main configuration is at src/main/resources/application.yml
  - each DB has its own file for example for Elasticsearch the file is src/main/resources/application-elasticsearch.yml  
  - Docker setup can be found at directory src/main/resources/docker. Each DB has its own subdirectory, for example, Cassandra directory is src/main/resources/docker/cassandra
  - When the particular docker container is run it has to be run commands that are important for proper setup, for example, the creation of the tables, indexes, etc.


How to integrate new DB to the project.
  The following steps should be executed.
  - [Required] -> Create com.akg.dbperfcheck.config.{DBNAME}Config.java ( example ElasitcsearchConfig.java )
  - [Required] -> Create com.akg.dbperfcheck.service.{DBNAME}Command.java ( example ElasitcsearchCommand.java )
  - [Required] -> Create configuration file application-{DBNAME}.yml  ( example application-elasticsearch.yml )
  - [Optional] -> Create docker folder at PROJECT_HOME/src/main/resources/docker with docker-compose file in it connected for the run of DB container ( for example PROJECT_HOME/src/main/resources/docker/elasticsearch/docker-compose.yml ). 