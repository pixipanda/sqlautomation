ETL
=====================================
```text
1. This framework helps to extract data from multiple sources, 
   transform the data by running sql queries and 
   load the transformed data to multiple sinks. 
2. It is implemented using Apache Spark 2.3
3. While reading data from File sources, you can specify explicit schema. Schema must be specifed as avsc file(Avro Schema)
4. It supports caching.

```

Supported Data stores
========================================
```text
Data could be extracted from 
   HIVE,
   FILES like CSV, XML, JSON, ORC, PARQUET,AVRO, FTPSERVER, JDBC like MYSQL, ORACLE, POSTGRES, etc
Data could be load to 
   HIVE,   
   FILES like CSV, XML, JSON, ORC, PARQUET and AVRO
   
This is an ongoing project. More sources and sinks will be added.   
``` 
  
Config file template
---------------------------------
```text
Sample config files
 src/test/resources/jobs/csv_transform_hive/csv_transform_hive.conf
 src/test/resources/jobs/csv_transform_hive/sqlFile1.conf.conf
 
This config file will extract data from Csv file,do some transformation using sql queries and load the data to hive
 Please do create similar config files.
```


Sample Config File
=====================================
## Main cofig file
```hocon
ETL {
  Extract {
    sources = [
      {
        sourceType = "csv"
        options {
          path = "/tmp/csvfiles/input/employee.csv"
          format = "csv"
          header = "true"
          inferSchema = "true"
        }
        viewName = "employeeView"
      }
    ]
  }
  Transform {
    sqls = [
      {
        order = "1"
        fileName = "sqlFile1.conf"
        queries = [
          {
            order = "1"
            queryName = "filterDept10"
          }
        ]
      }
    ]
  }
  Load {
    sinks = [
      {
        sinkType = "hive"
        options {
          db = "test_db1"
          table = "employee"
          format = "orc"
          mode = "overwrite"
        }
      }
    ]
  }
}
```
## sql config file
```hocon
{
  filterDept10 = "SELECT * FROM employeeView where emp_dept_id = 10"
}
```

Config Details
===================================================
```text
Config file contains 3 sections
1. Extract Section
   You can specify multiple sources from where you want to extract the data from.
   Here we are extracting data from CSV file.
   After extracting the data, employeeView, view is created so that the queries in the transform section 
   could be run on that view.
   If you don't want to run any queries and just want to save data to hive then you need not specify the view
   
    
2. Transform Section   
   Here you can specify the queries that you want to run and transform your data.
   All your queries must be in another .conf file. You can have multiple .conf files.
   You just need to specify the order in which the queries will be run
   Example: Here the sql config file is sqlFile1.conf
            Query name is filterDept10
   So this query will be run on the view created in the Extract Section. 
   
 3. Load Section
   Once the data is transformed, you can save the data to multiple sinks.
   Here we are saving the data to hive.               
```

More Examples
=====================================================
```text
Config files: 
src/test/resources/jobs/

Test cases:
src/test/scala/com/pixipanda/sqlautomation/pipeline

```

Class Name
-------------------------------
```scala
com.pixipanda.sqlautomation.Main
```



How to run this program
======================================
```text
Local:
Checkout from BitBucket 
Import to IntelliJ or your favourite IDE
Run the below test case to get a demo of this framework.
src/test/scala/com/pixipanda/sqlautomation/pipeline/CsvTransformHiveSpec.scala
This test case will extract data from csv, do simple transformation using sql queries and load data to hive
```

Integration with oozie and shell script
=============================================
TBD