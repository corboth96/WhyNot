import Util.*;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.*;

import java.util.*;

/**
 * NedExplain algorithm
 * @author Corie Both
 * Date Created: June 24, 2019
 */
public class NedExplain {
    private DatabaseConnection conn;
    private UtilityOperations ops;
    private List<Tab> tabQ;
    private List<RelNode> nonPickyManip;
    private List<AnswerTuple> pickyManip;
    private List<RelNode> emptyOutput;

    private List<HashMap<String,Object>> dirTc;
    private List<HashMap<String,Object>> inDirTc;

    /**
     * constructor
     * @param conn - database connection
     */
    public NedExplain(DatabaseConnection conn) {
        this.conn = conn;
        this.ops = new UtilityOperations();
    }

    /**
     * run function to be called by main
     * @param sql query string
     * @param predicates list of items we are looking for
     */
    public void NedExplain_Run(String sql, List<ConditionalTuple> predicates) {
        for (ConditionalTuple tc : predicates) {
            String answer = runNedExplain(sql,tc);
            System.out.println(answer);
        }
    }

    /**
     * NedExplain algorithm
     * @param sql - query we are looking for data item in
     * @param unpicked - unpicked item we are looking for
     * @return true/false if finished
     */
    private String runNedExplain(String sql, ConditionalTuple unpicked) {
        List<String> tables = generateTABQ(sql);
        // 1. Compatible Finder
        compatibleFinder(tables, unpicked);

        // 2. Initializations
        emptyOutput = new ArrayList<>();
        pickyManip = new ArrayList<>();
        nonPickyManip = new ArrayList<>();
        computeCompatibles(unpicked);

        // 3. Run Algorithm
        for (int i = 0; i<tabQ.size(); i++) {
            Tab m = tabQ.get(i);
            if (checkEarlyTermination(i,m)) {
                return ops.getDetailedAnswer(pickyManip,emptyOutput);
            }
            m.output = applyManipulation(m);
            int child_index = -1;
            for (int ind = 0; ind < tabQ.size(); ind++) {
                if (tabQ.get(ind).name.equals(m.child)) {
                    child_index = ind;
                }
            }

            tabQ.get(child_index).input.addAll(m.output);
            if (m.output.size() == 0) {
                emptyOutput.add(m.name);
                if (m.compatibles.size() != 0) {
                    AnswerTuple at = new AnswerTuple(m.name,m.compatibles);
                    pickyManip.add(at);
                }
            }
            if (!m.name.getRelTypeName().equals("JdbcTableScan")) {
                tabQ.get(child_index).compatibles.addAll(findSuccessors(m));
            } else {
                if (m.compatibles.size() != 0) {
                    tabQ.get(child_index).compatibles.addAll(m.compatibles);
                    nonPickyManip.add(m.name);
                }
            }

        }
        return ops.getDetailedAnswer(pickyManip,emptyOutput);
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
            while (j >= 0 && tabQ.get(j).level == tabQ.get(i-1).level && tabQ.get(j).child.equals(tabQ.get(i-1).child)) {
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

    /**
     * Get result of manipulation
     * @param m - manipulation to apply
     * @return list of result tuples
     */
    private List<HashMap<String, Object>> applyManipulation(Tab m) {
        String query = ops.convertToSqlString(m.name);
        return ops.runQuery(conn,query);
    }

    /**
     * Find successors in result set
     * @param m manipulation
     * @return tuples that match
     */
    private List<HashMap<String,Object>> findSuccessors(Tab m) {
        List<HashMap<String,Object>> successors = new ArrayList<>();
        for (HashMap<String,Object> o : m.output) {
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
        return successors;
    }


    /**
     * fill in direct and indirect compatible sets
     * @param tables - list of tables involved in the query
     * @param tc - conditional tuple we are looking for
     */
    private void compatibleFinder(List<String> tables, ConditionalTuple tc) {
        // Dirtc = match qualified named attributes
        // Indirtc = match all other attributes
        dirTc = new ArrayList<>();
        HashMap<String,HashMap<String,Object>> qualifieds = tc.getQualifiedAttributes();
        for (String table : qualifieds.keySet()) {
            String sql = "SELECT * from db."+table;
            tables.remove(table);
            List<HashMap<String,Object>> results = ops.runQuery(conn,sql);
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
            for (HashMap<String,Object> res : ops.runQuery(conn,sql)) {
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
        for (Tab m: tabQ) {
            if (m.name.getRelTypeName().equals("JdbcTableScan")) {
                for (HashMap<String,Object> tuple : m.input) {
                    for (HashMap<String,Object> compatible : dirTc) {
                        if (tuple.entrySet().containsAll(compatible.entrySet())) {
                            if (tc.getQualifiedAttributes().keySet().
                                    contains(m.name.getTable().getQualifiedName().get(1))) {
                                m.compatibles.add(compatible);
                            }

                        }
                    }
                }
            }
        }
    }

    /**
     * Generate the initial table
     * @param sql - query to initialize table for
     */
    private List<String> generateTABQ(String sql) {
        tabQ = new ArrayList<>();
        try {
            SchemaPlus schema = conn.getCc().getRootSchema().getSubSchema("DB");

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
            RelNode relNode =  relRoot.rel;

            List<String> tables = new ArrayList<>();
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
                        tables.add(node.getTable().getQualifiedName().get(1));
                        String tableScan = ops.convertToSqlString(node);
                        tab = new Tab(ops.runQuery(conn,tableScan), level, node, parent);
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

            Collections.reverse(tabs);
            List<Tab> tablist  = new ArrayList<>();
            tablist.add(tabs.get(0));
            LinkedList<Tab> q = new LinkedList<>(tablist);
            List<Tab> visited = new ArrayList<>();
            while (!q.isEmpty()) {
                Tab polled = q.poll();
                tabQ.add(polled);
                visited.add(polled);
                int nextLevel = polled.level -1;
                RelNode nextToAdd = polled.child;

                for (Tab t : tabs) {
                    if (t.name.equals(nextToAdd)) {
                        if (!visited.contains(t) && !q.contains(t)) {
                            q.add(t);
                        }
                    }
                }
                for (Tab t : tabs) {
                    if (t.level == nextLevel) {
                        if (!q.contains(t) && !visited.contains(t)) {
                            q.add(t);
                        }
                    }
                }
            }
            return tables;
        } catch (SqlParseException|ValidationException|RelConversionException e) {
            e.printStackTrace();
        }

        return null;
    }

}
