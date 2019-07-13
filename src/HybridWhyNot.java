import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;


/**
 * Hybrid WhyNot/NedExplain implementation
 * Adds improvements from NedExplain into WhyNot
 * @author Corie Both
 * Date Created: Jul 8, 2019
 */
public class HybridWhyNot {
    private DatabaseConnection conn;
    private List<HybridTab> hTabSorted;
    private List<RelNode> nonPickyManip;
    private List<AnswerTuple> pickyManip;
    private List<RelNode> emptyOutput;

    private List<HashMap<String,Object>> dirTc;
    private List<HashMap<String,Object>> inDirTc;

    private String runWhyNot(String sql, ConditionalTuple unpicked) {
        HybridDAG hDAG = new HybridDAG();
        Map<HybridTab, ArrayList<HybridTab>> dag = hDAG.generateDAG(sql,conn);
        List<String> tables = hDAG.getTables(dag);
        List<HybridTab> roots = hDAG.findRoots(dag);
        Map<RelNode, Integer> parentCount = hDAG.getParentCount(dag);
        hTabSorted = hDAG.topologicalSort(dag);

        compatibleFinder(tables, unpicked);
        emptyOutput = new ArrayList<>();
        pickyManip = new ArrayList<>();
        nonPickyManip = new ArrayList<>();
        computeCompatibles(unpicked);

        Map<HybridTab,Boolean> visited = new HashMap<>();
        LinkedList<HybridTab> queue = new LinkedList<>();
        for (HybridTab item : hTabSorted) {
            if (roots.contains(item)) {
                queue.add(item);
            }
        }

        while (queue.size() != 0) {
            HybridTab m = queue.poll();
            visited.put(m,true);
            boolean successorExists = successorExists(m);
            if (successorExists) {
                if (dag.get(m) != null) {
                    for (HybridTab next : dag.get(m)) {
                        parentCount.replace(next.name, parentCount.get(next.name) - 1);
                        // next has not been visited but all of its parents have
                        if (visited.get(next) == null && parentCount.get(next.name) == 0) {
                            queue.add(next);
                        }
                    }
                }
            }
        }
        return getDetailedAnswer();
    }


    private boolean successorExists(HybridTab m) {
        List<HashMap<String,Object>> output = runQuery(convertToSqlString(m.name));

        if (output.size() == 0) {
            emptyOutput.add(m.name);
            if (m.compatibles.size() != 0) {
                AnswerTuple at = new AnswerTuple(m.name,m.compatibles);
                pickyManip.add(at);
            }
            return false;
        }

        if (!m.name.getRelTypeName().equals("JdbcTableScan")) {
            for (HybridTab t : hTabSorted) {
                if (t.name.equals(m.child)) {
                    HybridSuccessors hs = findSuccessors(m,output);
                    t.compatibles.addAll(hs.successors);
                    return hs.returnType;
                }
            }
        } else {
            if (m.compatibles.size() != 0) {
                for (HybridTab t : hTabSorted) {
                    if (t.name != null) {
                        if (t.name.equals(m.child)) {
                            t.compatibles.addAll(m.compatibles);
                        }
                    }
                }
                nonPickyManip.add(m.name);
            }
        }
        return true;
    }

    private HybridSuccessors findSuccessors(HybridTab m, List<HashMap<String,Object>> output) {
        List<HashMap<String,Object>> successors = new ArrayList<>();
        for (HashMap<String,Object> o : output) {
            for (HashMap<String,Object> tuple : dirTc) {
                if (o.entrySet().containsAll(tuple.entrySet())) {
                    successors.add(tuple);
                }
            }
            for (HashMap<String,Object> tuple : inDirTc) {
                if (o.entrySet().containsAll(tuple.entrySet())) {
                    if (!successors.contains(o)) {
                        successors.add(o);
                    }
                }
            }
        }
        // check for compatibles that are not listed as successors
        // these become the blocked tuples
        List<HashMap<String,Object>> blocked = new ArrayList<>();
        boolean blockedTuple = true;
        for (HashMap<String,Object> obj : m.compatibles) {
            for (HashMap<String,Object> s : successors) {
                if (s.entrySet().containsAll(obj.entrySet())) {
                    blockedTuple = false;
                }
            }
            if (blockedTuple) {
                blocked.add(obj);
            }
        }

        if (successors.size() != 0) {
            nonPickyManip.add(m.name);
        }
        if (blocked.size() != 0) {
            AnswerTuple at = new AnswerTuple(m.name,blocked);
            pickyManip.add(at);
        }

        HybridSuccessors hs;
        if (blocked.size() == m.compatibles.size() && m.compatibles.size() != 0) {
            hs = new HybridSuccessors(successors,false);
        } else {
            hs = new HybridSuccessors(successors, true);
        }
        return hs;
    }

    /**
     * fill in direct and indirect compatible sets
     * @param tables - list of tables involved in the query
     * @param tc - conditional tuple we are looking for
     */
    private void compatibleFinder(List<String> tables, ConditionalTuple tc) {
        dirTc = new ArrayList<>();
        HashMap<String,HashMap<String,Object>> qualifieds = tc.getQualifiedAttributes();
        for (String table : qualifieds.keySet()) {
            String sql = "SELECT * from db."+table;
            tables.remove(table);
            List<HashMap<String,Object>> results = runQuery(sql);
            for (HashMap<String,Object> tuple : results) {
                if (tuple.entrySet().containsAll(qualifieds.get(table).entrySet())) {
                    for (String key: tuple.keySet()) {
                        if (key.contains("id")) {
                            HashMap<String,Object> id = new HashMap<>();
                            id.put(key,tuple.get(key));
                            dirTc.add(id);
                        }
                    }
                }
            }
        }

        inDirTc = new ArrayList<>();
        for (String table : tables) {
            String sql = "SELECT * from db."+table;
            for (HashMap<String,Object> res : runQuery(sql)) {
                res.put("table",table);
                inDirTc.add(res);
            }
        }
    }

    /**
     * Compatibility finder to initialize with first compatibles
     * @param tc unpicked data item we are looking for
     */
    private void computeCompatibles(ConditionalTuple tc) {
        for (HybridTab m : hTabSorted) {
            if (m.name.getRelTypeName().equals("JdbcTableScan")) {
                for (HashMap<String,Object> compatible : dirTc) {
                    if (tc.getQualifiedAttributes().keySet().
                            contains(m.name.getTable().getQualifiedName().get(1))) {
                        m.compatibles.add(compatible);
                    }
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
     * Helper function to print out detailed, condensed and secondary answers
     * @return always true
     */
   private String getDetailedAnswer() {
        String answerString = "----------Detailed Answer:----------\n";
        answerString += "{\n";
        boolean isFirst = true;
        for (AnswerTuple answer : pickyManip) {
            for (HashMap<String,Object> ans : answer.getDetailed().keySet()) {
                if (isFirst) {
                    answerString += "(";
                    answerString += ans;
                    answerString += "," + answer.getDetailed().get(ans);
                    answerString += ")";
                    isFirst = false;
                } else {
                    answerString += ", \n";
                    answerString += "(";
                    answerString += ans;
                    answerString += "," + answer.getDetailed().get(ans);
                    answerString += ")";
                }
            }
        }
        answerString += "\n";
        answerString += "}\n";

        answerString += "----------Condensed Answer:----------\n";
        answerString += "{";
        isFirst = true;
        for (AnswerTuple answer : pickyManip) {
            if (isFirst) {
                answerString += answer.manipulation;
                isFirst = false;
            } else {
                answerString += ", "+answer.manipulation;
            }
        }
        answerString += "}\n";

        answerString += "----------Secondary Answer:----------\n";
        answerString += "(";
        isFirst = true;
        for (RelNode m : emptyOutput) {
            if (isFirst) {
                answerString += m;
                isFirst = false;
            } else {
                answerString += ", "+m;
            }
        }
        answerString += ")\n";


        return answerString;
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

    public static void main(String[] args) {
        HybridWhyNot hwn = new HybridWhyNot();
        hwn.conn = new DatabaseConnection();
        hwn.conn.createConnection();

        String sql = "select m.movie_id,m.title,m.yearReleased from db.Movie m " +
                "left join db.MovieGenres mg on m.movie_id = mg.movie_id " +
                "left join db.Genre g on g.genre_id = mg.genre_id where m.movie_id in " +
                "(select movie_id from db.DirectedBy group by movie_id " +
                "having count(director_id)>=2) and g.genre = 'Action'";

        List<ConditionalTuple> predicate = new ArrayList<>();
        ConditionalTuple ct = new ConditionalTuple();
        ct.addVTuple("Movie.title", "Titanic");
        List<String> tables = new ArrayList<>();
        tables.add("Movie");
        predicate.add(ct);
        for (ConditionalTuple tc : predicate) {
            String answer = hwn.runWhyNot(sql,tc);
            System.out.println(answer);
        }
    }
}
