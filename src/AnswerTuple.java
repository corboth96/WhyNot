import org.apache.calcite.rel.RelNode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * AnswerTuple class for defining a more detailed answer
 * Keep track of the manipulation & the tuples that are lost at that manipulation
 * Will allow us to better create the answers
 * @author Corie Both
 * Date Created: Jun 28, 2019
 */
public class AnswerTuple {
    RelNode manipulation;
   List<HashMap<String,Object>> tuples;
    private HashMap<HashMap<String,Object>, RelNode> reversed;

    public AnswerTuple(RelNode manip, List<HashMap<String,Object>> compatibles) {
        this.manipulation = manip;
        this.tuples = new ArrayList<>(compatibles);
        this.reversed = new HashMap<>();
        for (HashMap<String,Object> tuple : compatibles) {
            this.reversed.put(tuple,manip);
        }
    }

    public HashMap<HashMap<String,Object>, RelNode> getDetailed() {
        return reversed;
    }
}
