# Improving the Implementation of Why Not
The goal of this project was to analyze and understand the state-of-the-art algorithms for why not queries in order to combine these algorithms into one complete system to provide efficient and detailed answers for why not questions.

Why Not is the first state-of-the-art algorithm that thinks of SQL queries in terms of a directed acyclic graph. To find the manipulations in the query that are causing the data to not be returned, we traverse the graph in a BFS order and evaluate the output of each manipulation to determine if our data item is lost at that step.

On the other hand, NedExplain is a follow-up to the original algorithm that uses a table rather than a directed acyclic graph. Here, this algorithm seeks to improve on the original algorithm by providing better tracing for the tuples that we are looking for at each step in the algorithm. This seeks to provide a better answer for the why not query by providing a better linkage between the manipulation and the tuples lost at that manipulation.

In order to evaluate these algorithms to determine how to combine these in the most effective way possible, we go about implementing each of these algorithms as described in their respective papers. In order to build the primary data structures (DAG and Table), we investigated and successfully integrated Apache Calcite as a SQL parser that could easily convert our queries into relational algebra parse trees. Once we implemented each of these algorithms, we had a better understanding of the benefits and drawbacks of each and were more capable of picking and choosing the best of each in order to create a new, more efficient algorithm. The algorithm implemented here is HybridWhyNot and seeks to take the best parts of each algorithm in order to create a new-and-improved algorithm.

*Why Not*: Adriane Chapman and H. V. Jagadish. 2009. Why Not. SIGMOD Conference (2009), 523–534. <br>
*NedExplain*: Katerina Tzompanaki Nicole Bidoit, Melanie Herschel. 2014. Query-Based Why-Not Provenance with NedExplain. EDBT (2014), 145–156.

Paper detailing the work provided in this project can be accessed at [INSERT LINK WHEN AVAILABLE].


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
    2. Create user and grant privileges on it (change username and password if different credentials wanted)
    ```
    GRANT ALL PRIVILEGES ON smallmovies.* TO 'user'@'localhost' IDENTIFIED BY 'password' WITH GRANT OPTION;
    ```
    **Note**: If you want to use a different user/password, will have to update this in *DatabaseConnection.java* also

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

The program prompts for user input for which query to run. Enter 0-12 for the corresponding query. This will run the query
and the three algorithms for the corresponding missing data.

### Outputs
1. The results from WhyNot
2. The results from NedExplain
3. The results from HybridWhyNot
4. The running time for each algorithm

### Note
At this time, the answers being returned are the names of the RelNodes that Apache Calcite generates in relation to the relational algebra operators. Converting these into an understandable format for a novice user is a step of future work. At this time, the algorithms write out their data structures (table, DAG, HybridDAG) to a text file so that you can use this data structure information, along with the returned manipulations in the answer, to determine what manipulation this corresponds to. This is obviously a shortcoming of the current implementation with Apache Calcite, but more work can be done to determine how to get this information in a more readable format.

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

    Class that contains 12 queries and their respective unpicked data items/conditional tuples for testing.

## Algorithm Discussion

With the implementation of Why Not and NedExplain, we have been able to isolate current issues in these algorithms.

### Why Not Shortcomings
- Inaccurate tuple tracing
- Inaccurate results due to reliance on attribute preservation (i.e. if the question is Why not title = Titanic, will be looking specifically for title.)

### NedExplain Shortcomings
- Large storage space needed for storing all inputs and outputs
- Less efficient because has to define stopping criteria

### Hybrid Why Not Discussion

#### Borrowed Components
- Conditional Tuple from NedExplain
- Directed Acyclic Graph from Why Not
- Table from NedExplain (excluding inputs and outputs) as nodes in DAG for better tuple tracing
- Detailed Answer from NedExplain

#### Benefits
- Less storage space needed in the table
- Efficient traversal and no need for stopping conditions based on the DAG
- Better answers: highlights thnot only the manipulation but the tuples that are lost at that manipulation

## Future Work
With the completion of the three algorithms as proposed above, we have successfully improved upon the Why Not and NedExplain algorithms in order to create our new Hybrid Why Not algorithm.

Foreseen future work can include:
1. NedExplain issue: Queries 7, 10, and 11

Per the queries, in *Queries.java*, results are not being returned for these aforementioned queries. This could be due to the stopping criteria of the NedExplain algorithm or another misstep in the implementation of the algorithm (though this does not seem to be affecting the Hybrid Why Not). Exploration should confirm if this algorithm is working as it should be.

2. User Interface

The next foreseeable step is to create a user interface where the user can enter their own queries and unpicked data items in order to get a result returned for why the user-written queries are not returning the expected results. This is meant to be an educational tool where users can understand the mistakes they are making by running their queries and expected results through this "debugger".

In order for this to happen, work will have to be done in order to provide the user with a more viewable friendly way to see the manipulations causing the data to not be returned. At the current step, the Apache Calcite RelNodes are being returned and this would not be helpful to a user who does not want to have to understand what this means. These RelNodes should be returned in a more user-friendly fashion.

The user interface should then be a GUI that allows the user to enter their queries and unpicked data items to interact with the algorithms.

3. Query Mutation

Another foreseeable step given the fact that we can isolate the manipulations that are causing the data to be lost is to provide automatic query mutation. Instead of just returning the manipulation to the user, a query mutation tool would use the algorithm built here in order to mutate the picky manipulation until it returns the unpicked data item as expected.  Here, the answer returned to the user would not be a manipulation, but a re-write of the whole SQL query that now returns the unpicked data item.
