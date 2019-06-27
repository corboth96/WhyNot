import com.sun.org.apache.regexp.internal.RE;
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
    Tab child;
    RelNode name;

    public Tab(List<HashMap<String,Object>> input, List<HashMap<String,Object>> compatibles, int level,Tab child,RelNode name) {
        this.input = input;
        this.compatibles = compatibles;
        this.level = level;
        this.child = child;
        this.name = name;
    }

    public Tab(List<HashMap<String,Object>> compatibles, int level,Tab child,RelNode name) {
        this.compatibles = compatibles;
        this.level = level;
        this.child = child;
        this.name = name;
    }

    public Tab(int level,Tab child,RelNode name) {
        this.level = level;
        this.child = child;
        this.name = name;
    }

    public Tab(int level, RelNode name) {
        this.level = level;
        this.name = name;
    }

}
