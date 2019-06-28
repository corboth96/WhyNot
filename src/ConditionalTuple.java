import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * ConditionalTuple class
 * @author Corie
 * Date Created: June 24, 2019
 */
public class ConditionalTuple {
    HashMap<String,Object> vtuple;
    List<Condition> conditions;

    /**
     * initialize hash map and list
     */
    public ConditionalTuple() {
        vtuple = new HashMap<>();
        conditions = new ArrayList<>();
    }

    /**
     * add the value-tuple
     * @param col - column of value
     * @param val - value in column
     */
    public void addVTuple(String col, Object val) {
        vtuple.put(col,val);
    }

    /**
     * create condition in the tuple
     * @param var - variable/column to apply condition to
     * @param op - operation in {!=, =,<,>,<=,=>}
     * @param val - value to match
     */
    public void addCondition(String var, String op, Object val) {
        Condition c = new Condition(var, op, val);
        conditions.add(c);

    }

    /**
     * get type of the conditional tuple
     * @return list of the types
     */
    public List<String> getType() {
        Object[] types = vtuple.keySet().toArray();
        List<String> typeStrings = new ArrayList<>();
        for (Object o : types) {
            typeStrings.add((String)o);
        }
        return typeStrings;
    }

    /**
     * internal class that encapsulates the condition part of the conditional tuple
     */
    private class Condition {
        String variable;
        String operator; // {!=, =, >, >=, <=, <}
        Object value;

        /**
         * Condition constructor
         * @param var - variable to try to compare
         * @param op - conditional operator
         * @param val - value to match
         */
        private Condition(String var, String op, Object val) {
            this.variable = var;
            this.operator = op;
            this.value = val;
        }
    }
}
