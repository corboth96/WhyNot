import java.util.HashMap;
import java.util.List;

/**
 * Hybrid successors for HybridWhyNot
 * Allows us to return true or false for if a manipulation's successors should be
 * added to the queue to check as well as return the successors for the successor
 * manipulations's compatibles
 * @author Corie Both
 * Date Created: July 10, 2019
 */
public class HybridSuccessors {
    List<HashMap<String,Object>> successors;
    boolean returnType;

    /**
     * constructor
     * @param s - the successors
     * @param returnType - whether all compatibles were lost or not
     */
    public HybridSuccessors(List<HashMap<String,Object>> s, boolean returnType) {
        this.successors = s;
        this.returnType = returnType;
    }
}
