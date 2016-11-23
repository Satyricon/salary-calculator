# Apache Spark Salary calculator

This application calculates monthly wage for every person based on the given calculation guidelines.

## Input File Format
File format is CSV. Every data row specifies one work shift and split shifts are allowed. All timestamps are given in 15-minute increments.

```
Person Name, Person ID, Date, Start, End
```
Example row
``` 
John Smith, 8, 26.3.2014, 13:15, 2:00
```
## Calculation guidlines
Hourly wage for all employees is $3.75. 

```
Total Daily Pay = Regular Daily Wage + Evening Work Compensation + Overtime Compensations
```
```
Regular Daily Wage = Regular Working Hours * Hourly Wage
```

Evening work compensation is +$1.15/hour for hours between 18:00 - 06:00.

```
Evening Work Compensation = Evening Hours * Evening Work Compensation
```

Overtime compensation is paid when daily working hours exceeds 8 hours.

```
Overtime Compensations[] = Overtime Hours * Overtime Compensation Percent * Hourly Wage

First 2 Hours > 8 Hours = Hourly Wage + 25%
Next 2 Hours = Hourly Wage + 50%
After That = Hourly Wage + 100%	
```

Other extra compensations are not included in hourly wage when calculating overtime compensations. If evening hours were collected before normal working hours, they take priority in calculations over normal hours. For example if person's daily shift look like this

``` 
John Smith, 8, 26.3.2014, 4:00, 6:00
John Smith, 8, 26.3.2014, 10:00, 17:00
John Smith, 8, 26.3.2014, 20:00, 3:00
```

Calculations will be as follows: 2 first hour will be calculated with evening compensation, 6 hours from day shif will be calculated with normal hourly wage, all other hours (8 hours) will be calculated with corresponding overtime rates (2 + 2 + 4)

```
2 * (3.75 + 1.15) + 6 * 3.75 + 2 * (3.75 + 3.75 * 0.25) + 2 * (3.75 + 3.75 * 0.5) + 4 * (3.75 + 3.75) = 82.925
```

## Running the appliation
This is a Scala Spark Application. Used Scala version is 2.11. 

There are 3 different ways to run this application.

### Using IntelliJ
Clone the repository and create out of it new SBT scala project. IDE will download all the needed dependencies. After that create run configuration through Run menu (Run -> Run... -> Edit Configurations...)

```
Main class: SalaryCalculator
Program arguments: PATH_TO_THE_INPUT_CSV_FILE PATH_TO_THE_OUTPUT_FILE
```
where PATH_TO_THE_INPUT_CSV_FILE and PATH_TO_THE_OUTPUT_FILE are paths to input and output files (e.g. ~/Desktop/HourList201403.csv and ~/Desktop/salaries.csv). Output files will be created by the application.

After this click Run. Application will compile and run. You should see the output in about 10 seconds.

### Using spark-submit with default deploy-mode
Under console cd to Applications main directory and run

```
sbt package
```

This will create under target/scala-2.11 directory a jar file ready to be submited to Apache Spark. Download latest (2.0.2) version of Apache Spark for your platform, extract it somewhere, cd to its root directory and execute the following command

```
bin/spark-submit PATH_TO_APPLICATION_JAR_FILE PATH_TO_THE_INPUT_CSV_FILE PATH_TO_THE_OUTPUT_FILE
```
where PATH_TO_APPLICATION_JAR_FILE is the path to the jar file created in the previous step.

### Using spark-submit with cluster deploy-mode
Under console cd to Applications main directory and run

```
sbt package
```

This will create under target/scala-2.11 directory a jar file ready to be submited to Apache Spark. Download latest (2.0.2) version of Apache Spark for your platform, extract it somewhere, cd to its root directory and execute the following commands 

```
sbin/start-master.sh
```

This will start Spark Master. Open browser and navigate to http://localhost:8080/. From there copy the URL from the top (should be something like spark://ubuntu:7077) and execute

```
sbin/start-slave.sh URL
```

where URL is the Spark Master URL. This will start one Spark worker node which should appear under Master's UI in the browser (also available through http://localhost:8081/). Next copy REST URL from Spark Master's web page (should be something like spark://ubuntu:6066) and execute

```
bin/spark-submit --master SPARK_MASTER_REST_URL --deploy-mode cluster PATH_TO_APPLICATION_JAR_FILE PATH_TO_THE_INPUT_CSV_FILE PATH_TO_THE_OUTPUT_FILE
```

where SPARK_MASTER_REST_URL is the URL aquired from Spark Master's web page. In this case PATH_TO_THE_INPUT_CSV_FILE and PATH_TO_THE_OUTPUT_FILE should be full paths to those files. Otherwise Spark will try to search for them under its home directories.

This command will submit our Salary Calculator appliation to Spark and you will be able to see its progress through Web UI. When job has been finished, you should see output file under specified location (PATH_TO_THE_INPUT_CSV_FILE).
