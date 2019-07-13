import Util.*;
import org.apache.calcite.rel.RelNode;
import java.util.*;


/**
 * Hybrid WhyNot/NedExplain implementation
 * Adds improvements from NedExplain into WhyNot
 * @author Corie Both
 * Date Created: Jul 8, 2019
 */
public class HybridWhyNot {
    private DatabaseConnection conn;
    private UtilityOperations ops;

    private List<HybridTab> hTabSorted;
    private List<RelNode> nonPickyManip;
    private List<AnswerTuple> pickyManip;
    private List<RelNode> emptyOutput;

    private List<HashMap<String,Object>> dirTc;
    private List<HashMap<String,Object>> inDirTc;

    public HybridWhyNot(DatabaseConnection conn) {
        this.conn = conn;
        ops = new UtilityOperations();
    }

    public void HybridWhyNot_Run(String sql, List<ConditionalTuple> predicates) {
        for (ConditionalTuple tc : predicates) {
            String answer = runWhyNot(sql,tc);
            System.out.println(answer);
        }
    }

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
        return ops.getDetailedAnswer(pickyManip, emptyOutput);
    }


    private boolean successorExists(HybridTab m) {
        List<HashMap<String,Object>> output = ops.runQuery(conn,ops.convertToSqlString(m.name));

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
}
