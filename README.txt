How to run the application:

1. Run via IDE
  1.1. Import Existing Maven Project -> comparisons
  1.2. Run as Java Application -> com.akg.data.perf.comparisons.ComparisonsApplication.java
  
2. Build with maven and run the jar file.
  2.1. Go to the project home directory and run 
  	$mvn clean install
  2.2. Copy/Paste comparisons-0.0.1-SNAPSHOT.jar and src/main/resources directory where you want, for example let be directory TEST
  2.3. Go to TEST directory open application.yaml file and set the proper values for the properties ( input-data-file, use-cases, etc.
  2.4. Run 
    $java -jar comparisons-0.0.1-SNAPSHOT.jar -Dspring.config.location=.
  
2. Configurations
  - the main configuration is at application.yml
  - each DB has its own file for example for Elasticsearch the file is application-elasticsearch.yml  
