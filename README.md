# WhyNot
Implementation of algorithm provided by Adriane Chapman and H. V. Jagadish. 2009. Why Not. SIGMOD Conference (2009), 523–534.



## Before Running
### Set Up Database
1. Install MySQL / MySQL Workbench

    Go to https://dev.mysql.com/downloads/installer/ to install MySQL

2. Run *DataInsert.sql* to create smallmovies database & insert data

    **Note**: Different versions of mysql may have different syntax for running certain commands

    *Recommended way to run this is by opening in MySQL Workbench and running.*

    What this file does:
    1. Create Database
    ```
    CREATE DATABASE smallmovies;
    USE smallmovies;
    ```
    2. Create user and grant privileges on it
    ```
    GRANT ALL PRIVILEGES ON smallmovies.* TO 'corie'@'localhost' IDENTIFIED BY '1234' WITH GRANT OPTION;
    ```
    **Note**: If you want to use a different user/password, will have to update this in *DatabaseConnection.java*

    3. Creates tables, such as:
    ```
    DROP TABLE IF EXISTS `Movie`;
    /*!40101 SET @saved_cs_client     = @@character_set_client */;
    /*!40101 SET character_set_client = utf8 */;
    CREATE TABLE `Movie` (
      `id` int(11) NOT NULL AUTO_INCREMENT,
      `movie_id` int(11) DEFAULT NULL,
      `title` varchar(120) DEFAULT NULL,
      `yearReleased` int(11) DEFAULT NULL,
      PRIMARY KEY (`id`)
    ) ENGINE=InnoDB AUTO_INCREMENT=198 DEFAULT CHARSET=latin1;
    /*!40101 SET character_set_client = @saved_cs_client */;
    ```
    4. Inserts data, such as:
    ```
    LOCK TABLES `Movie` WRITE;
    /*!40000 ALTER TABLE `Movie` DISABLE KEYS */;
    INSERT INTO `Movie` VALUES (1,862,'Toy Story',1995);
    /*!40000 ALTER TABLE `Movie` ENABLE KEYS */;
    UNLOCK TABLES;
    ```

### Open Project in Desired IDE (IntelliJ used for Development)
#### Install Maven Dependencies
1. org.apache.calcite:calcite-core:1.19.0
2. mysql:mysql-connector-java:8.0.16

#### Edit configurations to indicate which class to run

Set running class as *QueryDatabase.java*

#### Add output folder for running
**Note**: These steps are described for using IntelliJ and may be different for different IDEs.
1. Create *out* folder inside *WhyNot* folder

2. Open Project Settings

    -File -> Project Structure -> Project

3. Set compiler output as the new folder created

## Running the Project
Run the project by pressing Run.

The program prompts for user input for which query to run. Enter 0-9 for the corresponding query. This will run the query
and the why not algorithm for the corresponding unpicked data item.

### Outputs
1. The result set in the form: TITLE (YEAR).
2. The list of *Picky Manipulations* for why the unpicked data item is not in the result set.

## Queries Explained
All the queries being used by the project currently are detailed in *Queries.sql*. In this file, the queries are
explained and the unpicked data item is listed along with an explaniation as to why this data item isn't in the result
set of said query. This information can be used to verify that the Why Not algorithm is returning the correct results.

## Files Explained
1. QueryDatabase.java

Main file that handles making calls to connect to the database, running the query, and making calls to run the Why Not algorithm.

2. DatabaseConnection.java

File that handles creating the connection to the database and closing the database, called by QueryDatabase.java.

3. WhyNot.java

Class to handle the main algorithm of Why Not. Manages creating the DAG, running the algorithm, and outputting the picky
manipulatins.

4. RelNodeLink.java

Helper class used for creating the DAG. Creates a link between a manipulation and its parent manipulation.

5. DAG.java

Class to create the directed acyclic graph to be used by the Why Not Algorithm. Invoked by WhyNot.java. Provides the graph
that provides a bottom up understanding of the query (from table scans upwards) as a parse tree. Also returns a topologically
sorted ordering for use by the Why Not algorithm.