# WhyNot
Implementation of algorithm provided by Adriane Chapman and H. V. Jagadish. 2009. Why Not. SIGMOD Conference (2009), 523–534.



## Before Running
### Set Up Database
1. Install MySQL / MySQL Workbench

    Go to https://dev.mysql.com/downloads/installer/ to install MySQL

2. Run *DataInsert.sql* to create smallmovies database & insert data

    **Note**: Different versions of mysql may have different syntax for running certain commands

    *Recommended way to run this is by opening in MySQL workbench and running.*

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
1. Create *out* folder inside *WhyNot* folder

2. Open Project Settings

    -IntelliJ: File -> Project Structure -> Project

    -Eclipse:

3. Set compiler output as the new folder created