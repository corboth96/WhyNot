package HybridWhyNot;

import org.apache.calcite.rel.RelNode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * HybridTab class to be used by HybridWhyNot
 * @author Corie Both
 * Date Created: Jul 8, 2019
 */
public class HybridTab {
    List<HashMap<String,Object>> compatibles = new ArrayList<>();
    RelNode name;
    RelNode child;

    /**
     * Constructor
     * @param name - current node
     * @param child - next node (applying one more manipulation)
     */
    public HybridTab(RelNode name, RelNode child) {
        this.name = name;
        this.child = child;
    }
}
