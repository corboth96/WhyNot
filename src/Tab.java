import com.sun.org.apache.regexp.internal.RE;
import org.apache.calcite.rel.RelNode;

import java.util.ArrayList;
import java.util.List;

/**
 * Tab class for making the TabQ instances
 */
public class Tab {
    List<String> input = new ArrayList<>();
    List<String> compatibles = new ArrayList<>();
    List<String> output = new ArrayList<>();
    int level;
    Tab child;
    RelNode name;

    public Tab(List<String> input, List<String> compatibles, int level,Tab child,RelNode name) {
        this.input = input;
        this.compatibles = compatibles;
        this.level = level;
        this.child = child;
        this.name = name;
    }

    public Tab(List<String> compatibles, int level,Tab child,RelNode name) {
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
