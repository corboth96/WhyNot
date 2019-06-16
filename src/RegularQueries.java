import java.sql.*;

/**
 * Created by Corie on 6/6/19.
 */
public class RegularQueries {

    public static void connect() {
        String driverStr = "com.mysql.cj.jdbc.Driver";
        String url = "jdbc:mysql://127.0.0.1:3306/smallmovies?serverTimezone=EST";
        String user = "corie";
        String password = "1234";

        try {
            Class.forName(driverStr);
            Connection con = DriverManager.getConnection(url, user, password);
            Statement smt = con.createStatement();
            String sql = "select ss.id, ss.title, ss.year from " +
                    "(select id, title, year from Movie m " +
                    "join Roles r on m.id = r.movie_id  where r.actor_id = " +
                    "(select id from Actor where first = 'Kate' and last = 'Winslet')) ss " +
                    "inner join (select id, title, year from Movie m join Roles " +
                    "r on m.id = r.movie_id where r.actor_id = (select id from Actor " +
                    "where first = 'Leonardo' and last = 'DiCaprio')) jc on ss.id = jc.id ";
            ResultSet rs = smt.executeQuery(sql);
            while (rs.next() ) {
                System.out.println(rs.getString(2));

            }

            String sql2 = "select * from Movie m " +
                    "join Roles r on m.id = r.movie_id " +
                    "left join Roles r2 on m.id = r2.movie_id " +
                    "where r.actor_id = (select id from Actor where first = 'Kate' and last = 'Winslet') and " +
                    "r2.actor_id = (select id from Actor where first = 'Leonardo' and last = 'DiCaprio')";
            ResultSet rs2 = smt.executeQuery(sql2);
            while (rs2.next() ) {
                System.out.println(rs2.getString(3));

            }

            String sql3 = "SELECT movie_id, COUNT(*) AS $f1 FROM DirectedBy GROUP BY movie_id HAVING $f1 >= 2";
            ResultSet rs3 = smt.executeQuery(sql3);
            while (rs3.next() ) {
                System.out.println(rs3.getString(1));

            }
        } catch (ClassNotFoundException|SQLException ex) {
            ex.printStackTrace();
        }

    }
    public static void main(String[] args) {
        connect();
    }
}
