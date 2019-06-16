import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.*;
import org.apache.calcite.rel.externalize.RelJsonWriter;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.*;

import javax.sql.DataSource;
import java.sql.*;
import java.sql.Statement;
import java.util.*;

/**
 * Test class for Apache Calcite
 */
public class AC_Test {
    private Connection con = null;
    private CalciteConnection cc = null;


    /**
     * Connect to database with apache calcite
     */
    private void connectWithAC() {
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
    }

    /**
     * Function to run the query with apache calcite
     * @param i - query to run
     * @return sql string for use by why not algorithm
     */
    private String RunQuery_AC(int i) {
        Statement smt;
        String sql = null;
        try {
            smt =  con.createStatement();
            ResultSet rs = null;
            switch (i) {
                case 1:
                    sql = "select * from db.Movie";
                    rs = smt.executeQuery(sql);
                    while (rs.next()) {
                        String title = rs.getString(3);
                        int year = rs.getInt(4);

                        System.out.print(title);
                        System.out.println(" (" + year + ")");
                    }
                    System.out.println();
                    break;
                case 2:
                    sql = "select * from db.Movie WHERE yearReleased < 2018 and yearReleased > 2000";
                    rs = smt.executeQuery(sql);
                    while (rs.next()) {
                        String title = rs.getString(3);
                        int year = rs.getInt(4);

                        System.out.print(title);
                        System.out.println(" (" + year + ")");
                    }
                    System.out.println();
                    break;
                case 3:
                    sql = "select ss.id, ss.title, ss.yearReleased from " +
                            "(select id, title, yearReleased from db.Movie m " +
                            "join db.Roles r on m.id = r.movie_id  where r.actor_id in " +
                            "(select id from db.Actor where fname= 'Kate' and lname = 'Winslet')) ss " +
                            "inner join (select id, title, yearReleased from db.Movie m join db.Roles " +
                            "r on m.id = r.movie_id where r.actor_id in (select id from db.Actor " +
                            "where fname = 'Leonardo' and lname = 'DiCaprio')) jc on ss.id = jc.id ";
                    rs = smt.executeQuery(sql);
                    while (rs.next()) {
                        String title = rs.getString(2);
                        int year = rs.getInt(3);

                        System.out.print(title);
                        System.out.println(" (" + year + ")");

                    }
                    break;
                case 4:
                    sql = "select m.title,m.yearReleased from db.Movie m " +
                            "left join db.MovieGenres mg on m.id = mg.movie_id " +
                            "left join db.Genre g on g.id = mg.genre_id where m.id in " +
                            "(select movie_id from db.DirectedBy group by movie_id " +
                            "having count(director_id)>=2) and g.genre = 'Action'";
                    rs = smt.executeQuery(sql);
                    while (rs.next()) {
                        String title = rs.getString(1);
                        int year = rs.getInt(2);

                        System.out.print(title);
                        System.out.println(" (" + year + ")");
                    }
                    System.out.println();
                    break;
                case 5:
                    sql = "select g.id as id, g.title, g.yearReleased from " +
                            "(select id, title, yearReleased from db.movie m join db.moviegenres mg on m.id = mg.movie_id " +
                            "where mg.genre_id in (select id from db.genre where genre = 'Family')) g " +
                            "inner join " +
                            "(select id, title, yearReleased from db.movie m join db.directedby db on m.id = db.movie_id " +
                            "where db.director_id in (select id from db.director where fname = 'John' and lname = 'Lasseter')) d " +
                            "using(id)" +
                            "inner join " +
                            "(select id, title, yearReleased from db.movie m join db.roles r on m.id = r.movie_id " +
                            "where r.actor_id in (select id from db.actor where fname = 'Tom' and lname = 'Hanks')) a " +
                            "on a.id = g.id and g.id = d.id";
                    rs = smt.executeQuery(sql);
                    while (rs.next()) {
                        String title = rs.getString(2);
                        int year = rs.getInt(3);

                        System.out.print(title);
                        System.out.println(" (" + year + ")");
                    }
                    System.out.println();
                    break;
            }
            rs.close();
            smt.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return sql;
    }

    /**
     * create the dag by parsing the Apache Calcite parse tree
     * @param sql - query to parse
     * @return DAG of queries from bottom up
     */
    private Map<RelNode,ArrayList<RelNode>> generateDAG(String sql) {
        Map<RelNode,ArrayList<RelNode>> dag = new HashMap<>();
        try {
            //cc = con.unwrap(CalciteConnection.class);
            SchemaPlus schema = cc.getRootSchema().getSubSchema("DB");

            Frameworks.ConfigBuilder cb = Frameworks.newConfigBuilder(
            ).defaultSchema(schema).parserConfig(SqlParser.configBuilder().setCaseSensitive(false).build());
            FrameworkConfig config = cb.build();
            Planner p = Frameworks.getPlanner(config);

            // parses and validates sql
            SqlNode sqlNode = p.parse(sql);

            // must call validate before conversion
            SqlNode validatedNode = p.validate(sqlNode);

            // Converts a SQL parse tree into a tree of relational expressions
            RelRoot relRoot = p.rel(validatedNode);
            RelNode relNode = relRoot.rel;

            System.out.println(RelOptUtil.toString(relNode));

            // rel visitor
            List<RelNodeLink> entries = new ArrayList<>();
            RelVisitor rv = new RelVisitor() {
                @Override
                public void visit(RelNode node, int ordinal, RelNode parent) {
                    System.out.println(node.toString());
                    System.out.println("\t"+convertToSqlString(node));
                    RelNodeLink e = new RelNodeLink(node,parent);
                    entries.add(e);
                    super.visit(node, ordinal, parent);
                }

                @Override
                public void replaceRoot(RelNode node) {
                    super.replaceRoot(node);
                }

                @Override
                public RelNode go(RelNode p) {
                    return super.go(p);
                }
            };
            rv.go(relNode);

            ArrayList<RelNode> notAllocated = new ArrayList<>();
            for (RelNodeLink e : entries) {
                RelNode entry = e.entry;
                RelNode parent = e.parent;
                notAllocated.add(entry);

                if (!dag.containsKey(entry)) {
                    dag.put(entry,new ArrayList<>());
                }

                if (parent != null) {
                    dag.get(entry).add(parent);
                }
                for (RelNode item : dag.keySet()) {
                    if (dag.get(item).contains(parent)) {
                        notAllocated.remove(parent);
                    }
                }


            }
            dag.put(null,notAllocated);

            /*
            System.out.println("Bottom up");
            // bottom up? Parent pointing to child
            for (HashMap.RelNodeLink e : dag.entrySet()) {
                System.out.print(e.getKey());
                System.out.println(" -> "+e.getValue());
            }
            System.out.println();
            */

        }
        catch (SqlParseException|RelConversionException|ValidationException e) {
            e.printStackTrace();
        }

        return dag;
    }


    /**
     * Successor exists function to determine if unpicked data item is in the result set
     * @param queryNode - relnode to convert and run on database
     * @param unpicked data item we are looking for
     * @return true if data item exists else false
     */
    private boolean successorExists(RelNode queryNode, HashMap.Entry<String,String> unpicked) {
        Statement smt;
        try {
            smt = con.createStatement();
            smt.setQueryTimeout(60);
            String query = convertToSqlString(queryNode);
            System.out.println(query);

            ResultSet rs = smt.executeQuery(query);
            System.out.println("Query executed");
            ResultSetMetaData rsmd = rs.getMetaData();
            boolean successorVisible = false;

            for (int i = 1; i<rsmd.getColumnCount(); i++) {
                if (unpicked.getKey().equals(rsmd.getColumnName(i))) {
                    successorVisible = true;
                }
            }

            // successor visibility
            if (successorVisible) {
                System.out.println("HERE");
                int columnIndex = rs.findColumn(unpicked.getKey());
                System.out.println(columnIndex);
                while (rs.next()) {
                    if (rs.getString(columnIndex).equals(unpicked.getValue())) {
                        rs.close();
                        smt.close();
                        return true;
                    }
                }
                rs.close();
                smt.close();
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * Helper function to convert RelNode into SQL string to run
     * @param query relnode to convert
     * @return sql string to run
     */
    private String convertToSqlString(RelNode query) {
        SqlDialect.Context c = SqlDialect.EMPTY_CONTEXT.withDatabaseProductName(
                SqlDialect.DatabaseProduct.MYSQL.name()).withDatabaseProduct(SqlDialect.DatabaseProduct.MYSQL);
        SqlDialect dialect = new SqlDialect(c);
        RelToSqlConverter relToSqlConverter = new RelToSqlConverter(dialect);
        RelToSqlConverter.Result res;
        res = relToSqlConverter.visitChild(0,query);
        SqlNode newNode = res.asSelect();
        return newNode.toSqlString(dialect,false).getSql();
    }

    private String convertManipulation(RelNode query) {
        String cond = "";
        if (query.getRelTypeName().equals("LogicalFilter")) {
            LogicalFilter filter = (LogicalFilter) query;
            cond = filter.getCondition().toString();
            System.out.println(filter.getInput().toString());

            RelJsonWriter writer = new RelJsonWriter();
            filter.explain(writer);
            System.out.println(writer.asString());
            //System.out.println(sw.toString());

        } else if (query.getRelTypeName().equals("LogicalJoin")) {
            LogicalJoin join = (LogicalJoin) query;
            cond = join.getCondition().toString();
        }
        System.out.println();
        return cond;
    }

    /**
     * Start with all nodes initialized to not picky
     * @param parentCounts parent count map of all nodes
     * @param picky map to initialize
     */
    /*private void initializePicky(Map<RelNode,Integer> parentCounts, Map<RelNode,Boolean> picky) {
        for (RelNode r : parentCounts.keySet()) {
            if (r != null) {
                picky.put(r,false);
            }
        }
    }*/

    /**
     * Why Not algorithm to find picky manipulations
     * @param roots - root nodes
     * @param dag - directed acyclic graph
     * @param parentCounts - counts of the parents
     * @param unpicked - the data item we are looking for
     */
    private void traverseAndFindPicky(List<RelNode> roots, Map<RelNode,ArrayList<RelNode>> dag,
                               Map<RelNode,Integer> parentCounts,List<RelNode> sorted,
                               Map.Entry<String,String> unpicked) {
        List<RelNode> possiblePicky = new ArrayList<>();

        // traverse tree
        Map<RelNode,Boolean> visited = new HashMap<>();
        LinkedList<RelNode> queue = new LinkedList<>();
        for (RelNode item : sorted) {
            if (roots.contains(item)) {
                queue.add(item);
            }
        }
        while (queue.size() != 0) {
            RelNode r = queue.poll();
            visited.put(r,true);

            boolean successorExists = successorExists(r,unpicked);
            System.out.println("\t"+successorExists);
            if (successorExists) {
                if (dag.get(r) != null) {
                    for (RelNode next : dag.get(r)) {
                        parentCounts.replace(next, parentCounts.get(next) - 1);
                        // next has not been visited but all of its parents have
                        if (visited.get(next) == null && parentCounts.get(next) <=1) {
                            queue.add(next);
                        }
                    }
                }
            } else {

                possiblePicky.add(r);
            }
        }

        System.out.println("-----Picky Manipulation(s)-----");
        List<RelNode> actuallyPicky = postProcessPicky(dag,possiblePicky);
        for (RelNode n : actuallyPicky) {
            System.out.println(convertToSqlString(n));
            System.out.println(convertManipulation(n));
        }

    }

    /**
     * Post process function to why not
     * Seeks to ensure that a picky manipulation does not have a picky manipulation as its successor
     * @param dag - directed acyclic graph to test
     * @param picky - list of manipulations flagged as picky
     * @return - final list of "frontier picky manipulations"
     */
    private List<RelNode> postProcessPicky(Map<RelNode,ArrayList<RelNode>> dag, List<RelNode> picky) {
        List<RelNode> finalList = new ArrayList<>(picky);
        for (RelNode item : picky) {
            List<RelNode> children = dag.get(item);
            LinkedList<RelNode> queue = new LinkedList<>(children);
            while (queue.size() > 0) {
                RelNode r = queue.poll();
                if (picky.contains(r)) {
                    finalList.remove(item);
                }
                queue.addAll(dag.get(r));

            }
        }
        return finalList;
    }

    /**
     * Get parent count for each node
     * @param dag directed acyclic graph of relnodes
     * @return parent count map
     */
    private Map<RelNode,Integer> getParentCount(Map<RelNode,ArrayList<RelNode>> dag) {
        Map<RelNode,Integer> counts = new HashMap<>();
        for (RelNode entry : dag.keySet()) {
            for (RelNode child : dag.get(entry)) {
                if (!counts.containsKey(child)) {
                    counts.put(child,0);
                }
                if (entry != null) {
                    counts.replace(child, counts.get(child) + 1);
                }
            }
        }
        return counts;
    }

    /**
     * find the root node
     * @param dag - directed acyclic graph
     * @return list of root nodes
     */
    private List<RelNode> findRoot(Map<RelNode,ArrayList<RelNode>> dag) {
        List<RelNode> roots = new ArrayList<>();
        roots.addAll(dag.get(null));
        return roots;
    }



    private void closeConnection() {
        try {
            con.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * topological order sorting
     * @param dag directed acyclic graph to sort
     * @return stack of topological ordering
     */
    private List<RelNode> topologicalSort(Map<RelNode,ArrayList<RelNode>> dag) {
        Stack<RelNode> stack = new Stack<>();

        Map<RelNode,Boolean> visited = new HashMap<>();
        for (RelNode element : dag.keySet()) {
            visited.put(element,false);
        }

        for (RelNode el : visited.keySet()) {
            if (!visited.get(el)) {
                recursiveSort(stack,visited,dag,el);
            }
        }
        List<RelNode> sortedNodes = new ArrayList<>();
        while (!stack.empty()) {
            RelNode n = stack.pop();
            if (n != null) {
                sortedNodes.add(n);
            }
        }
        return sortedNodes;
    }

    /**
     * topological sort recursive call
     * @param s - the stack to add to
     * @param visited - map keeping track of if we have seen the node yet
     * @param dag - directed acyclic graph to sort
     * @param item - item we are currently looking at
     */
    private void recursiveSort(Stack<RelNode> s, Map<RelNode,Boolean> visited,
                               Map<RelNode,ArrayList<RelNode>> dag,RelNode item ) {
        visited.replace(item,true);

        for (RelNode node : dag.get(item)) {
            if (!visited.get(node)) {
                recursiveSort(s,visited,dag,node);
            }
        }

        s.push(item);

    }

    public static void main(String[] args) {
        AC_Test c = new AC_Test();
        Scanner scan = new Scanner(System.in);
        c.connectWithAC();
        System.out.print("Enter input: ");
        int input = scan.nextInt();
        String sql = c.RunQuery_AC(input);

        Map<RelNode,ArrayList<RelNode>> dag = c.generateDAG(sql);
        Map<RelNode,Integer> parentCounts = c.getParentCount(dag);
        List<RelNode> roots = c.findRoot(dag);
        List<RelNode> sorted = c.topologicalSort(dag);

        // unpicked
        HashMap<String,String> unpicked = new HashMap<>();
        unpicked.put("title","Aladdin");
        for (HashMap.Entry<String,String> e : unpicked.entrySet()) {
            c.traverseAndFindPicky(roots, dag, parentCounts,sorted, e);
        }

        c.closeConnection();
    }
}
