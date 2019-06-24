import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Created by Corie on 6/24/19.
 */
public class ConditionalTuple {
    HashMap<String,Object> vtuple;
    List<Condition> conditions;

    public ConditionalTuple() {
        vtuple = new HashMap<>();
        conditions = new ArrayList<>();
    }

    public void addVTuple(String col, Object val) {
        vtuple.put(col,val);
    }

    public void addCondition(String var, String op, Object val) {
        Condition c = new Condition(var, op, val);
        conditions.add(c);

    }

    public void compareCondition() {

    }

    public Object[] getType() {
        return vtuple.keySet().toArray();
    }

    private class Condition {
        String variable;
        String operator; // {!=, =, >, >=, <=, <}
        Object value;

        private Condition(String var, String op, Object val) {
            this.variable = var;
            this.operator = op;
            this.value = val;
        }
    }
}
