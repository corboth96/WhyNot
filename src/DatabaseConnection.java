import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Class to manage the database connection
 * Creates and closes the connection
 * @author Corie Both
 * @date June 5, 2019
 */
public class DatabaseConnection {
    Connection con;
    CalciteConnection cc = null;


    public Connection createConnection() {
        String driverStr = "com.mysql.cj.jdbc.Driver";
        String url = "jdbc:mysql://127.0.0.1:3306/smallmovies?serverTimezone=EST";
        String user = "corie";
        String password = "1234";

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
            System.out.println("connection established");
        } catch (ClassNotFoundException|SQLException ex) {
            ex.printStackTrace();
        }
        return con;
    }

    public void closeConnection() {
        try {
            con.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
