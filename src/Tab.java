import org.apache.calcite.rel.RelNode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Tab class for making the TabQ instances
 */
public class Tab {
    List<HashMap<String,Object>> input = new ArrayList<>();
    List<HashMap<String,Object>> compatibles = new ArrayList<>();
    List<HashMap<String,Object>> output = new ArrayList<>();
    int level;
    RelNode child;
    RelNode name;

    /**
     * first constructor
     * @param inputs - input list
     * @param level - level in query
     * @param name - name of manipulation
     * @param child - next manipulation
     */
    public Tab(List<HashMap<String,Object>> inputs, int level, RelNode name, RelNode child) {
        this.level = level;
        this.name = name;
        this.input.addAll(inputs);
        this.child = child;
    }

    /**
     * second constructor
     * @param level - level in query
     * @param name - name of manipulation
     * @param child - next manipulation
     */
    public Tab(int level, RelNode name, RelNode child) {
        this.level = level;
        this.name = name;
        this.child = child;
    }

}
