package Util;

import org.apache.calcite.rel.RelNode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Util.AnswerTuple class for defining a more detailed answer
 * Keep track of the manipulation & the tuples that are lost at that manipulation
 * Will allow us to better create the answers
 * @author Corie Both
 * Date Created: Jun 28, 2019
 */
public class AnswerTuple {
    private  RelNode manipulation;
    private HashMap<HashMap<String,Object>, RelNode> reversed;

    /**
     * constructor
     * @param manip -  causing data to not be returned
     * @param compatibles - list of compatibles lost
     */
    public AnswerTuple(RelNode manip, List<HashMap<String,Object>> compatibles) {
        this.manipulation = manip;
        this.reversed = new HashMap<>();
        for (HashMap<String,Object> tuple : compatibles) {
            this.reversed.put(tuple,manip);
        }
    }

    /**
     * return the private reversed list of compatible and the manipulation that loses it
     * @return hash map of hashmaps
     */
    public HashMap<HashMap<String,Object>, RelNode> getDetailed() {
        return reversed;
    }

    /**
     * getter for private manipulation
     * @return the manipulation
     */
    public RelNode getManipulation() {
        return manipulation;
    }
}
