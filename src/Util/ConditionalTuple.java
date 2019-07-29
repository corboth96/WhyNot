package Util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * ConditionalTuple class
 * @author Corie
 * Date Created: June 24, 2019
 */
public class ConditionalTuple {
    private HashMap<String,Object> vtuple;
    private List<Condition> conditions;

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
    public void addCondition(String var, String op, Integer val) {
        Condition c = new Condition(var, op, val);
        conditions.add(c);
    }

    public boolean checkCondition(Double val) {

        for (Condition c : conditions) {
            switch (c.operator) {
                case "=":
                    System.out.println(val+" "+c.value);
                    return val == c.value.doubleValue();
                case "!=":
                    return (val > c.value || val < c.value);
                case ">":
                    return val > c.value;
                case ">=":
                    return val >= c.value;
                case "<=":
                    return val <= c.value;
                case "<":
                    return val < c.value;
            }
        }
        return false;
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
     * Helper function for typing tc and finding compatible tuples
     * @return hashmap of table and columns we are looking for
     */
    public HashMap<String,HashMap<String,Object>> getQualifiedAttributes() {
        HashMap<String,HashMap<String,Object>> qualifiers = new HashMap<>();
        for (String name : vtuple.keySet()) {
            if (name.contains(".")) {
                String[] strings = name.split("\\.");
                String table = strings[0];
                String col = strings[1];
                if (qualifiers.containsKey(table)) {
                    HashMap<String,Object> temp = qualifiers.get(table);
                    temp.put(col,vtuple.get(name));
                    qualifiers.replace(table,temp);


                } else {
                    HashMap<String,Object> temp = new HashMap<>();
                    temp.put(col,vtuple.get(name));
                    qualifiers.put(table,temp);
                }
            }
        }
        return qualifiers;
    }

    /**
     * internal class that encapsulates the condition part of the conditional tuple
     */
    private class Condition {
        String variable;
        String operator; // {!=, =, >, >=, <=, <}
        Integer value;

        /**
         * Condition constructor
         * @param var - variable to try to compare
         * @param op - conditional operator
         * @param val - value to match
         */
        private Condition(String var, String op, Integer val) {
            this.variable = var;
            this.operator = op;
            this.value = val;
        }
    }
}
