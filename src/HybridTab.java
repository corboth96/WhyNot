import org.apache.calcite.rel.RelNode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Created by Corie on 7/8/19.
 */
public class HybridTab {
    List<HashMap<String,Object>> compatibles = new ArrayList<>();
    int level;
    RelNode name;
    RelNode child;

    public HybridTab(int level, RelNode name, RelNode child) {
        this.level = level;
        this.name = name;
        this.child = child;
    }
}
