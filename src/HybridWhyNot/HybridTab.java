package HybridWhyNot;

import org.apache.calcite.rel.RelNode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * HybridWhyNot.HybridWhyNot.HybridTab class to be used by HybridWhyNot.HybridWhyNot
 * @author Corie Both
 * Date Created: Jul 8, 2019
 */
public class HybridTab {
    List<HashMap<String,Object>> compatibles = new ArrayList<>();
    private int level;
    RelNode name;
    RelNode child;

    /**
     * constructor
     * @param level - level in query
     * @param name - Relnode
     * @param child - subquery relnode
     */
    public HybridTab(int level, RelNode name, RelNode child) {
        this.level = level;
        this.name = name;
        this.child = child;
    }
}
