import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.*;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Trying to figure out NedExplain
 * @author Corie Both
 */
public class NedExplain {
    DatabaseConnection conn;
    List<Tab> tabQ;
    List<RelNode> nonPickyManip;
    List<RelNode> pickyManip;
    List<RelNode> emptyOutput;

    public String runNedExplain(String sql, Map.Entry<String,String> unpicked) {
        // 1. Compatible Finder


        // 2. Canonicalization - AC??
        DatabaseConnection conn = new DatabaseConnection();
        DAG dag = new DAG();
        Map<RelNode, ArrayList<RelNode>> graph =  dag.generateDAG(sql,new DatabaseConnection());
        compatibleFinder(graph,unpicked);

        // 3. Initializations
        emptyOutput = new ArrayList<>();
        pickyManip = new ArrayList<>();
        nonPickyManip = new ArrayList<>();
        //tabQ = new ArrayList<>();

        // 4. Run Algorithm
        for (int i = 0; i<tabQ.size(); i++) {
            Tab m = tabQ.get(i);
            if (checkEarlyTermination(i,m)) {
                return getDetailedAnswer();
            }
            m.output = applyManipulation(m);
            Tab child = m.child;
            child.input.addAll(m.output);
            if (m.output == null) {
                emptyOutput.add(m.name);
                if (m.compatibles != null) {
                    //pickyManip.addAll(m.compatibles);
                }
            }
            if (m.name.getRelTypeName().equals("JdbcTableScan")) {
                child.compatibles.add("null");
            } else {
                if (m.compatibles != null) {
                    child.compatibles.addAll(findSuccessors(m));
                    nonPickyManip.add(m.name);
                }
            }

        }
        return "NONE";
    }

    private boolean checkEarlyTermination(int i, Tab m) {
        if (i != 0 && m.level != tabQ.get(i-1).level) {
            int j = i-1;
            while (j >= 0 && tabQ.get(j).level == tabQ.get(i-1).level) {
                if (nonPickyManip.contains(tabQ.get(j).name)) {
                    return false;
                }
                j -= 1;
            }
            while (i<tabQ.size()) {
                if (tabQ.get(i).name.getRelTypeName().equals("JdbcTableScan")) {
                    return false;
                }
                i += 1;
            }
        } else {
            return false;
        }
        return true;
    }

    private String getDetailedAnswer() {
        return "NONE";
    }

    private List<String> applyManipulation(Tab op) {
        return null;
    }

    private List<String> findSuccessors(Tab m) {
        List<String> successors = new ArrayList<String>();
        for (String o : m.output) {

        }
        List<String> blocked = new ArrayList<>(m.compatibles);
        blocked.removeAll(successors);
        if (successors!= null) {

        }
        return successors;
    }

    private void compatibleFinder(Map<RelNode, ArrayList<RelNode>> graph,Map.Entry<String,String> unpicked) {
        for (RelNode n : graph.keySet()) {
            if (n.getRelTypeName().equals("JdbcTableScan")) {
                if (successorExists(n, unpicked)) {

                }
            }
        }
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

    /**
     * Successor exists function to determine if unpicked data item is in the result set
     * @param queryNode - relnode to convert and run on database
     * @param unpicked data item we are looking for
     * @return true if data item exists else false
     */
    private boolean successorExists(RelNode queryNode, HashMap.Entry<String,String> unpicked) {
        Statement smt;
        try {
            smt = conn.con.createStatement();
            smt.setQueryTimeout(60);
            String query = convertToSqlString(queryNode);
            //System.out.println(query);

            ResultSet rs = smt.executeQuery(query);
            ResultSetMetaData rsmd = rs.getMetaData();
            boolean successorVisible = false;
            for (int i = 1; i<rsmd.getColumnCount(); i++) {
                if (unpicked.getKey().equals(rsmd.getColumnName(i))) {
                    successorVisible = true;
                }
            }
            // successor visibility
            if (successorVisible) {
                int columnIndex = rs.findColumn(unpicked.getKey());
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





    public void generateTABQ(String sql, DatabaseConnection conn) {
        try {
            tabQ = new ArrayList<>();
            SchemaPlus schema = conn.cc.getRootSchema().getSubSchema("DB");

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

            HashMap<RelNode, Integer> levels = new HashMap<>();
            List<Tab> tabs = new ArrayList<>();

            RelVisitor rv = new RelVisitor() {
                @Override
                public void visit(RelNode node, int ordinal, RelNode parent) {
                    System.out.println(parent);
                    int level;
                    if (parent == null) {
                        level = 0;
                    } else {
                        level = levels.get(parent)+1;
                    }
                    levels.put(node,level);
                    Tab tab = new Tab(level,node);
                    tabs.add(tab);
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


            int topLevel = tabs.get(tabs.size()-1).level;
            for (int i = topLevel; i>= 0; i--) {
                for (Tab t : tabs) {
                    if (t.level == i) {
                        tabQ.add(t);
                    }
                }
            }
        }

        catch (SqlParseException | RelConversionException | ValidationException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        NedExplain ne = new NedExplain();
        ne.conn = new DatabaseConnection();
        ne.conn.createConnection();
        String sql = "select m.id,m.title,m.yearReleased from db.Movie m " +
                "left join db.MovieGenres mg on m.id = mg.movie_id " +
                "left join db.Genre g on g.id = mg.genre_id where m.id in " +
                "(select movie_id from db.DirectedBy group by movie_id " +
                "having count(director_id)>=2) and g.genre = 'Action'";
        ne.generateTABQ(sql,ne.conn);




       /* Map<String,String> unpicked = new HashMap<>();
        unpicked.put("title","Titanic");
        for (Map.Entry item : unpicked.entrySet()) {
            ne.runNedExplain(sql, item);
        }*/

        // skipping qualified attributes for now
        List<ConditionalTuple> predicate = new ArrayList<>();
        ConditionalTuple ct = new ConditionalTuple();
        ct.addVTuple("Movie.title", "Titanic");
        ct.addCondition("ap",">", 25);
        predicate.add(ct);


        for (ConditionalTuple tc : predicate) {
            Object[] type = tc.getType();
            for (Object t : type) {
                System.out.print(t+" ");
            }
            System.out.println();
        }

        ne.conn.closeConnection();
    }

}
