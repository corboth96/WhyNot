package WhyNot;

import org.apache.calcite.rel.RelNode;

/**
 * Helper class to link parents and their entries
 * @author Corie Both
 * Created Jun 5, 2019
 */
public class RelNodeLink {
    RelNode entry;
    RelNode parent;

    /**
     * constructor
     * @param entry = current manipulation
     * @param parent = parent of this manipulation
     */
    public RelNodeLink(RelNode entry, RelNode parent) {
        this.entry = entry;
        this.parent = parent;
    }

}
