package Util;

import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

/**
 * Class to manage the database connection
 * Creates and closes the connection
 * @author Corie Both
 * Created June 5, 2019
 */
public class DatabaseConnection {
    private Connection con;
    private CalciteConnection cc = null;

    public DatabaseConnection() {
        createConnection();
    }

    /**
     * open database connection
     * @return Connection
     */
    private Connection createConnection() {
        String driverStr = "com.mysql.cj.jdbc.Driver";
        String url = "jdbc:mysql://127.0.0.1:3306/smallmovies?serverTimezone=EST";
        String user = "user";
        String password = "password";

        try {
            Class.forName("org.apache.calcite.jdbc.Driver");
            Properties info = new Properties();
            info.setProperty("caseSensitive","false");
            info.setProperty("conformance","MYSQL_5");
            con = DriverManager.getConnection("jdbc:calcite:",info);
            cc = con.unwrap(CalciteConnection.class);
            SchemaPlus rootSchema = cc.getRootSchema();
            DataSource ds = JdbcSchema.dataSource(url,driverStr,user,password);
            rootSchema.add("DB",JdbcSchema.create(rootSchema,"DB",ds,null,null));
            System.out.println("Connection Established");
        } catch (ClassNotFoundException|SQLException ex) {
            ex.printStackTrace();
        }
        return con;
    }

    /**
     * createStatement method to keep con private and still be able to createStatement
     * @return Statement
     */
    public Statement createStatement() {
        Statement smt = null;
        try {
            smt = con.createStatement();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return smt;
    }

    /**
     * getter to get CalciteConnection
     * @return CalciteConnection
     */
    public CalciteConnection getCc() {
        return cc;
    }

    /**
     * close database connection
     */
    public void closeConnection() {
        try {
            con.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
