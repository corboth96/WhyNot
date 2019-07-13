# Improving the Implementation of Why Not
Original algorithm provided by Adriane Chapman and H. V. Jagadish. 2009. Why Not. SIGMOD Conference (2009), 523–534.
Seeking to improve this algorithm by including improvements proposed by Katerina Tzompanaki Nicole Bidoit, Melanie Herschel. 2014. Query-Based Why-Not Provenance with NedExplain. EDBT (2014), 145–156.



## Before Running
### Set Up Database
1. Install MySQL / MySQL Workbench

    Go to https://dev.mysql.com/downloads/installer/ to install MySQL

2. Run *DataExport.sql* to create smallmovies database and insert data

    [DataInsert.sql is old version - in order to use this version, queries will have to be adjusted for old table structure.]

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

Set running class as *RunAlgorithms.java*

#### Add output folder for running
**Note**: These steps are described for using IntelliJ and may be different for different IDEs.
1. Create *out* folder inside *WhyNot* folder

2. Open Project Settings

    -File -> Project Structure -> Project

3. Set compiler output as the new folder created

## Running the Project
Run the project by pressing Run.

The program prompts for user input for which query to run. Enter 0-10 for the corresponding query. This will run the query
and the three algorithms for the corresponding missing data.

### Outputs
1. The results from WhyNot
2. The results from NedExplain
3. The results from HybridWhyNot

## Queries Explained
All the queries being used by the project currently are detailed in *Queries.sql*. In this file, the queries are
explained and the unpicked data item is listed along with an explaniation as to why this data item isn't in the result
set of said query. This information can be used to verify that the Why Not algorithm is returning the correct results.

## Packages Explained

### WhyNot
1. WhyNot.java

    Class to handle the main algorithm of Why Not. Manages creating the DAG, running the algorithm, and outputting the picky
manipulations.

2. DAG.java

    Class to create the directed acyclic graph to be used by the Why Not Algorithm. Invoked by WhyNot.java. Provides the graph
that provides a bottom up understanding of the query (from table scans upwards) as a parse tree. Also returns a topologically
sorted ordering for use by the Why Not algorithm.

3. RelNodeLink.java

    Helper class used for creating the DAG. Creates a link between a manipulation and its parent manipulation.

### NedExplain
1. NedExplain.java

   Class that handles the main algorithm of NedExplain. Uses the Tab class to create the table that is the main data structure
of NedExplain and runs the algorithm, outputting 3 different answers:
- Detailed Answer: a list of compatibles and the manipulations that cause them to not be returned
- Condensed Answer: a list of manipulations causing all compatibles to not be returned
- Secondary Answer: a list of any manipulation that has empty output

2. Tab.java

    Class that encapsulates the main data structure of the NedExplain algorithm. A class that contains information such as
current manipulation, current manipulation's child, level, compatibles, input and output.

### HybridWhyNot (One System using both WhyNot and NedExplain ideas)
1. HybridWhyNot.java

    Class to handle the main algorithm of HybridWhyNot which provides a mixture of the ideas from both WhyNot and NedExplain.
    In this algorithm, we use the DAG structure with each node of the DAG being a slightly modified Tab. The main algorithm
    runs similarly to how WhyNot runs but uses the compatibility tracking from NedExplain.

2. HybridTab.java

    Class that encapsulates the main data structure of the HybridWhyNot algorithm. This class contains such information as
    level, name, child, and compatibles. (We don't store the inputs and outputs due to space.)

3. HybridDAG.java

    Class that builds the DAG used by the HybridWhyNot algorithm. Each node in the DAG is a HybridTab which allows us
    to do better compatibility tracing.

4. HybridSuccessors.java

    Class that contains the list of successors of a current manipulation and whether the manipulation is seen as picky
    or not for properly following the Why Not structure.

### Util
1. DatabaseConnection.java

    Class that handles creating the connection to the database and closing the database, called by QueryDatabase.java. Used
    by all 3 algorithms

2. AnswerTuple.java

    Class that associates a compatible tuple with the manipulation that causes that data to not be returned. Used by NedExplain
    and HybridWhyNot.

3. ConditionalTuple.java

    Class to represent the unpicked data item given by the definition in NedExplain. Used by NedExplain and HybridWhyNot.

4. Queries.java

    Class that contains 11 queries and their respective unpicked data items/conditional tuples for testing.
