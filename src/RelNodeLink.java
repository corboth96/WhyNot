import org.apache.calcite.rel.RelNode;

/**
 * Helper class to link parents and their entries
 * @author Corie Both
 * @date Jun 5, 2019
 */
public class RelNodeLink {
    RelNode entry;
    RelNode parent;

    public RelNodeLink(RelNode entry, RelNode parent) {
        this.entry = entry;
        this.parent = parent;
    }

}
