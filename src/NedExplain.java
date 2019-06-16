import org.apache.calcite.rel.RelNode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Trying to figure out NedExplain
 * @author Corie Both
 */
public class NedExplain {
    // compatible finder
    // canonicalize

    public String runNedExplain(String sql, Map.Entry<String,String> unpicked) {
        // 1. Compatible Finder


        // 2. Canonicalization - AC??
        DatabaseConnection conn = new DatabaseConnection();
        DAG dag = new DAG();
        Map<RelNode, ArrayList<RelNode>> graph =  dag.generateDAG(sql,new DatabaseConnection());

        // 3. Initializations
        List<String> emptyOutput = new ArrayList<>();
        List<String> pickyManip = new ArrayList<>();
        List<String> nonPickyManip = new ArrayList<>();
        List<Tab> tabQ = new ArrayList<>();

        // 4. Run Algorithm
        for (int i = 0; i<tabQ.size(); i++) {
            Tab m = tabQ.get(i);
            if (checkEarlyTermination(m)) {
                return getDetailedAnswer();
            }
            m.output = applyManipulation(m);
            Tab child = m.child;
            child.input.addAll(m.output);
            if (m.output == null) {
                emptyOutput.add(m.name);
                if (m.compatibles != null) {
                    pickyManip.addAll(m.compatibles);
                }
            }
            if (m.name.equals("TableScan")) {
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

    public boolean checkEarlyTermination(Tab m) {
        return false;
    }

    public String getDetailedAnswer() {
        return "NONE";
    }

    public List<String> applyManipulation(Tab op) {
        return null;
    }

    public List<String> findSuccessors(Tab m) {
        return null;
    }
}
