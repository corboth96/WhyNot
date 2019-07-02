import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Scanner;

/**
 * Main class:
 *  1. Gets the database connection
 *  2. Runs the requested query
 *  3. Gets the data item that we want to look for
 *  4. Calls why not for the query/unpicked data item
 * @author Corie Both
 * Created Jun 5, 2019
 */
public class QueryDatabase {
    private DatabaseConnection conn;

    /**
     * Function to run the query with apache calcite
     * @param i - query to run
     * @return sql string for use by why not algorithm
     */
    private String RunQuery(int i) {
        Statement smt;
        String sql = null;
        try {
            smt =  conn.con.createStatement();
            ResultSet rs = null;
            switch (i) {
                case 0:
                    sql = "select movie_id, title, yearReleased from db.Movie";
                    rs = smt.executeQuery(sql);
                    break;
                case 1:
                    sql = "select movie_id, title, yearReleased from db.Movie " +
                            "WHERE yearReleased < 2018 and yearReleased > 2000";
                    rs = smt.executeQuery(sql);
                    break;
                case 2:
                    sql = "select ss.movie_id, ss.title, ss.yearReleased from " +
                            "(select m.movie_id, title, yearReleased from db.Movie m " +
                            "join db.Roles r on m.movie_id = r.movie_id  where r.actor_id in " +
                            "(select actor_id from db.Actor where fname= 'Kate' and lname = 'Winslet')) ss " +
                            "inner join (select m.movie_id, title, yearReleased from db.Movie m join db.Roles " +
                            "r on m.movie_id = r.movie_id where r.actor_id in (select actor_id from db.Actor " +
                            "where fname = 'Leonardo' and lname = 'DiCaprio')) jc on ss.movie_id = jc.movie_id ";
                    rs = smt.executeQuery(sql);
                    break;
                case 3:
                    sql = "select m.movie_id,m.title,m.yearReleased from db.Movie m " +
                            "left join db.MovieGenres mg on m.movie_id = mg.movie_id " +
                            "left join db.Genre g on g.genre_id = mg.genre_id where m.movie_id in " +
                            "(select movie_id from db.DirectedBy group by movie_id " +
                            "having count(director_id)>=2) and g.genre = 'Action'";
                    rs = smt.executeQuery(sql);
                    break;
                case 4:
                    sql = "select g.movie_id, g.title, g.yearReleased from " +
                            "(select m.movie_id, title, yearReleased from db.movie m join db.moviegenres mg on m.movie_id = mg.movie_id " +
                            "where mg.genre_id in (select genre_id from db.genre where genre = 'Family')) g " +
                            "inner join " +
                            "(select m.movie_id, title, yearReleased from db.movie m join db.directedby db on m.movie_id = db.movie_id " +
                            "where db.director_id in (select director_id from db.director where fname = 'John' and lname = 'Lasseter')) d " +
                            "using(movie_id)" +
                            "inner join " +
                            "(select m.movie_id, title, yearReleased from db.movie m join db.roles r on m.movie_id = r.movie_id " +
                            "where r.actor_id in (select actor_id from db.actor where fname = 'Tom' and lname = 'Hanks')) a " +
                            "on a.movie_id = g.movie_id and g.movie_id = d.movie_id";
                    rs = smt.executeQuery(sql);

                    break;
                case 5:
                    sql = "select g.movie_id, g.title, g.yearReleased from " +
                            "(select m.movie_id, title, yearReleased from db.movie m join db.moviegenres mg on m.movie_id = mg.movie_id " +
                            "where genre_id in (select genre_id from db.genre where genre = 'Action')) g " +
                            "inner join " +
                            "(select m.movie_id, title, yearReleased from db.movie m join db.directedby db on m.movie_id = db.movie_id " +
                            "where director_id in (select director_id from db.director where fname = 'James' and lname = 'Cameron')) d " +
                            "using (movie_id)";
                    rs = smt.executeQuery(sql);
                    break;
                case 6:
                    sql = "select * from " +
                            "(select m.movie_id, title, yearReleased from db.movie m join db.moviegenres mg on m.movie_id = mg.movie_id " +
                            "where genre_id in (select genre_id from db.genre where genre = 'Drama')) g " +
                            "inner join " +
                            "(select m.movie_id, title, yearReleased from db.movie m join db.directedby db on m.movie_id = db.movie_id " +
                            "where director_id in (select director_id from db.director where fname = 'Steven' and lname = 'Spielberg')) d " +
                            "using (movie_id) " +
                            "where g.yearReleased > 2000";
                    rs = smt.executeQuery(sql);
                    break;
                case 7:
                    sql = "select m.movie_id, m.title, m.yearReleased from " +
                            "(select movie_id, title, yearReleased from db.movie) m " +
                            "inner join " +
                            "(select movie_id from db.moviegenres mg join db.genre g on g.genre_id = mg.genre_id " +
                            "where genre = 'Romance') a " +
                            "on m.movie_id = a.movie_id " +
                            "inner join " +
                            "(select movie_id from db.moviegenres mg join db.genre g on g.genre_id = mg.genre_id " +
                            "where genre = 'Comedy') r " +
                            "on m.movie_id = r.movie_id " +
                            "inner join " +
                            "(select movie_id from db.moviegenres mg join db.genre g on g.genre_id = mg.genre_id " +
                            "where genre = 'Family') d " +
                            "on m.movie_id = d.movie_id";
                    rs = smt.executeQuery(sql);
                    break;
                case 8:
                    sql = "select ss.movie_id as id, ss.title, ss.yearReleased from " +
                            "(select m.movie_id, title, yearReleased from db.movie m join db.directedby db on m.movie_id = db.movie_id " +
                            "where director_id in (select director_id from db.director where fname = 'James' and lname = 'Cameron')) ss " +
                            "where ss.movie_id in " +
                            "(select movie_id from db.directedby " +
                            "group by movie_id " +
                            "having count(director_id)=1)";
                    rs = smt.executeQuery(sql);
                    break;
                case 9:
                    sql = "select c.movie_id, c.title, c.yearReleased, movieCount from (" +
                            "select m.movie_id,m.title,m.yearReleased, count(*) as movieCount from db.movie m " +
                            "join db.roles r on r.movie_id = m.movie_id " +
                            "join db.actor a on a.actor_id = r.actor_id " +
                            "where a.actor_id in (" +
                            "select b.actor_id from (select a.actor_id, a.fname,a.lname, count(*) as cnt from db.Actor a " +
                            "join db.roles r on r.actor_id = a.actor_id " +
                            "group by a.actor_id,a.fname,a.lname) b " +
                            "where b.cnt > 5) " +
                            "group by m.title, m.movie_id,m.yearReleased " +
                            ") c " +
                            "where movieCount >= 2 " +
                            "order by c.movie_id";
                    rs = smt.executeQuery(sql);
                    break;
                case 10:
                    sql = "SELECT m.movie_id,m.title,m.yearReleased FROM db.Movie m " +
                            "JOIN db.MovieGenres mg on mg.movie_id = m.movie_id " +
                            "JOIN db.Genre g on g.genre_id = mg.genre_id WHERE g.genre = 'Action'";
                    rs = smt.executeQuery(sql);
                    break;
            }
            if (rs != null) {
                while (rs.next()) {
                    String title = rs.getString(2);
                    int year = rs.getInt(3);

                    System.out.print(title);
                    System.out.println(" (" + year + ")");
                }
                rs.close();
            }
            System.out.println();

            smt.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return sql;
    }

    public static void main(String[] args) {
        QueryDatabase q = new QueryDatabase();
        q.conn = new DatabaseConnection();
        q.conn.createConnection();
        Scanner scan = new Scanner(System.in);
        System.out.print("Enter input: ");
        int input = scan.nextInt();
        String sql = q.RunQuery(input);

        WhyNot whyNot = new WhyNot(q.conn);
        HashMap<String,String> unpicked = new HashMap<>();
        switch (input) {
            case 0:
                unpicked.put("title", "Divergent");
                break;
            case 1:
                unpicked.put("title","The Beach");
                break;
            case 2:
                unpicked.put("title","Avatar 2");
                break;
            case 3:
                unpicked.put("title","Aladdin");
                break;
            case 4:
                unpicked.put("title","Toy Story 3");
                break;
            case 5:
                unpicked.put("title","Titanic");
                break;
            case 6:
                unpicked.put("title","Saving Private Ryan");
                break;
            case 7:
                unpicked.put("title","Forrest Gump");
                break;
            case 8:
                unpicked.put("title", "Aliens of the Deep");
                break;
            case 9:
                unpicked.put("title", "Mrs. Doubtfire");
                break;
            case 10:
                unpicked.put("title","Titanic");
                break;
        }

        for (HashMap.Entry<String,String> e : unpicked.entrySet()) {
            whyNot.whyNot(sql,e);
        }

        q.conn.closeConnection();
    }
}
