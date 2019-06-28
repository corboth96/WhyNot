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
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * NedExplain algorithm
 * @author Corie Both
 * Date Created: June 24, 2019
 */
public class NedExplain {
    DatabaseConnection conn;
    private List<Tab> tabQ;
    private List<RelNode> nonPickyManip;
    private List<AnswerTuple> pickyManip;
    private List<RelNode> emptyOutput;

    public String runNedExplain(String sql, ConditionalTuple unpicked) {
        generateTABQ(sql,conn);
        // 1. Compatible Finder
        compatibleFinder(unpicked);
        // 2. Canonicalize

        // 3. Initializations
        emptyOutput = new ArrayList<>();
        pickyManip = new ArrayList<>();
        nonPickyManip = new ArrayList<>();

        // 4. Run Algorithm
        for (int i = 0; i<tabQ.size(); i++) {
            Tab m = tabQ.get(i);
            if (checkEarlyTermination(i,m)) {
                return getDetailedAnswer();
            }
            m.output = applyManipulation(m);
            int child_index = -1;
            for (int ind = 0; ind < tabQ.size(); ind++) {
                if (tabQ.get(ind).name.equals(m.child)) {
                    child_index = ind;
                }
            }
            tabQ.get(child_index).input.addAll(m.output);
            if (m.output == null) {
                emptyOutput.add(m.name);
                if (m.compatibles != null) {
                    AnswerTuple at = new AnswerTuple(m.name,m.compatibles);
                    pickyManip.add(at);
                }
            }
            if (m.name.getRelTypeName().equals("JdbcTableScan")) {
                tabQ.get(child_index).compatibles.addAll(findSuccessors(m, unpicked));
            } else {
                if (m.compatibles != null) {
                    tabQ.get(child_index).compatibles.addAll(findSuccessors(m, unpicked));
                    nonPickyManip.add(m.name);
                }
            }

        }
        return null;
    }

    /**
     * check for stopping early
     * @param i - index that we are looking at
     * @param m - manipulation that we are looking at
     * @return T/F if we are done early
     */
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
        System.out.println("----------Detailed Answer:----------");
        System.out.print("{");
        boolean isFirst = true;
        for (AnswerTuple answer : pickyManip) {
            for (HashMap<String,Object> ans : answer.getDetailed().keySet()) {
                if (isFirst) {
                    System.out.print("(");
                    System.out.print(ans);
                    System.out.print("," + answer.getDetailed().get(ans));
                    System.out.println(")");
                    isFirst = false;
                } else {
                    System.out.print(", (");
                    System.out.print(ans);
                    System.out.print("," + answer.getDetailed().get(ans));
                    System.out.println(")");
                }
            }
        }
        System.out.println("}");

        System.out.println("----------Condensed Answer:----------");
        System.out.print("{");
        isFirst = true;
        for (AnswerTuple answer : pickyManip) {
            if (isFirst) {
                System.out.print(answer.manipulation);
                isFirst = false;
            } else {
                System.out.print(", "+answer.manipulation);
            }
        }
        System.out.println("}");

        System.out.println("----------Secondary Answer:----------");
        System.out.print("(");
        isFirst = true;
        for (RelNode m : emptyOutput) {
            if (isFirst) {
                System.out.print(m);
                isFirst = false;
            } else {
                System.out.print(", "+m);
            }
        }
        System.out.println(")");


        return "NONE";
    }

    /**
     * Get result of manipulation
     * @param m - manipulation to apply
     * @return list of result tuples
     */
    private List<HashMap<String, Object>> applyManipulation(Tab m) {
        String query = convertToSqlString(m.name);
        return runQuery(query);
    }

    /**
     * Find successors in result set
     * @param m manipulation
     * @return tuples that match
     */
    private List<HashMap<String,Object>> findSuccessors(Tab m, ConditionalTuple tc) {
        List<HashMap<String,Object>> successors = new ArrayList<>();
        for (HashMap<String,Object> o : m.output) {
            for (String key : o.keySet()) {
                if (tc.getType().contains(key)) {
                    Object col = tc.vtuple.get(key);
                    if (o.get(key).equals(col)) {
                        successors.add(o);
                    }
                }
            }
        }
        List<HashMap<String,Object>> blocked = new ArrayList<>(m.compatibles);
        blocked.removeAll(successors);
        if (successors.size() != 0) {
            nonPickyManip.add(m.name);
        }
        if (blocked.size() != 0) {
            AnswerTuple at = new AnswerTuple(m.name,blocked);
            pickyManip.add(at);
        }
        return successors;
    }

    /**
     * Compatibility finder to initialize with first compatibles
     * @param unpicked - unpicked data item
     */
    private void compatibleFinder(ConditionalTuple unpicked) {
        List<HashMap<String,Object>> compatibles;
        for (String str : unpicked.vtuple.keySet()) {
            if (str.contains(".")) {
                String[] strings = str.split("\\.");
                String table = strings[0];
                String column = strings[1];
                for (Tab m : tabQ) {
                    if (m.name.getRelTypeName().equals("JdbcTableScan")) {
                        String relNodeStr = convertToSqlString(m.name);
                        if (relNodeStr.split(table).length == 1 && relNodeStr.contains(table)) {
                            for (HashMap<String,Object> tuple : m.input) {
                                if (tuple.get(column).equals(unpicked.vtuple.get(str))) {
                                    m.compatibles.add(tuple);
                                }
                            }
                        }

                    }

                }
            }
        }

        System.out.println();
        for (int i = 0; i<tabQ.size(); i++) {
            System.out.print(tabQ.get(i).name);
            System.out.print("\t"+tabQ.get(i).level);
            System.out.print("\t"+tabQ.get(i).child);
            System.out.print("\t"+tabQ.get(i).input.size());
            System.out.print("\t"+tabQ.get(i).compatibles);
            System.out.println();
        }
    }

    /**
     * Helper function to run query and save tuples
     * @param sql - query to run
     * @return list of tuples from that query
     */
    private List<HashMap<String,Object>> runQuery(String sql) {
        Statement smt;
        List<HashMap<String,Object>> tuples = new ArrayList<>();
        try {
            smt = conn.con.createStatement();
            ResultSet rs = smt.executeQuery(sql);

            List<String> columns = new ArrayList<>();
            List<String> colTypes = new ArrayList<>();
            for (int i = 1; i<=rs.getMetaData().getColumnCount();i++) {
                columns.add(rs.getMetaData().getColumnName(i));
                colTypes.add(rs.getMetaData().getColumnTypeName(i));
            }

            while (rs.next()) {
                HashMap<String,Object> tuple = new HashMap<>();
                for (int i = 0; i<columns.size();i++) {
                    Object col;
                    switch (colTypes.get(i)) {
                        case "INTEGER":
                            col = rs.getInt(i + 1);
                            break;
                        case "DATE":
                            col = rs.getDate(i + 1);
                            break;
                        case "TIME":
                            col = rs.getTime(i + 1);
                            break;
                        case "LONG":
                            col = rs.getLong(i + 1);
                            break;
                        case "FLOAT":
                            col = rs.getFloat(i + 1);
                            break;
                        case "DOUBLE":
                            col = rs.getDouble(i + 1);
                            break;
                        case "BOOLEAN":
                            col = rs.getBoolean(i + 1);
                            break;
                        case "DECIMAL":
                            col = rs.getBigDecimal(i + 1);
                            break;
                        case "TIMESTAMP":
                            col = rs.getTimestamp(i + 1);
                            break;
                        default:
                            col = rs.getString(i + 1);
                    }
                    tuple.put(columns.get(i), col);
                }
                tuples.add(tuple);
            }
            rs.close();
            smt.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return tuples;
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
     * Generate the initial table
     * @param sql - query to initialize table for
     * @param conn - connection
     */
    private void generateTABQ(String sql, DatabaseConnection conn) {
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
                    int level;
                    if (parent == null) {
                        level = 0;
                    } else {
                        level = levels.get(parent)+1;
                    }
                    levels.put(node,level);
                    Tab tab;
                    if (node.getRelTypeName().equals("JdbcTableScan")) {
                        String tableScan = convertToSqlString(node);
                        tab = new Tab(runQuery(tableScan), level, node, parent);
                    } else {
                        tab = new Tab(level, node, parent);
                    }
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

        // skipping qualified attributes for now
        List<ConditionalTuple> predicate = new ArrayList<>();
        ConditionalTuple ct = new ConditionalTuple();
        ct.addVTuple("Movie.title", "Titanic");
       // ct.addCondition("ap",">", 25);
        predicate.add(ct);

        /***** Not ready for this yet *****/
        for (ConditionalTuple tc : predicate) {
            ne.compatibleFinder(tc);
            //ne.runNedExplain(sql,tc);
        }



        ne.conn.closeConnection();
    }

}
