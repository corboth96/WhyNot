package WhyNot;

import Util.*;
import org.apache.calcite.rel.RelNode;

import java.sql.*;
import java.util.*;

/**
 * Class to run the why not algorithm
 *  Creates the dag and then runs the main algorithm
 * @author Corie Both
 * Created June 5, 2019
 */
public class WhyNot {
    private DatabaseConnection conn;
    private UtilityOperations ops;

    /**
     * Constructor to get database connection
     * @param c - db connection
     */
    public WhyNot(DatabaseConnection c) {
        this.conn = c;
        ops = new UtilityOperations();
    }

    /**
     * run function for main method
     * @param sql - query string
     * @param unpickeds - items we are looking for
     */
    public void whyNot_Run(String sql, HashMap<String, String> unpickeds) {
        for (HashMap.Entry<String,String> e : unpickeds.entrySet()) {
            whyNot(sql,e);
        }
    }

    /**
     * calls the main algorithm after initializing everything
     * @param sql - query string
     * @param unpicked - item we are looking for
     */
    private void whyNot(String sql, Map.Entry<String,String> unpicked) {
        // generate all of the information we need for the algorithm
        DAG dag = new DAG();
        Map<RelNode,ArrayList<RelNode>> graph = dag.generateDAG(sql,conn);
        Map<RelNode,Integer> parentCounts = dag.getParentCount(graph);
        List<RelNode> roots = dag.findRoot(graph);
        List<RelNode> sorted = dag.topologicalSort(graph);


        List<RelNode> picky = traverseAndFindPicky(roots,graph,parentCounts,sorted,unpicked);

        System.out.println("-----Picky Manipulation(s)-----");
        for (RelNode n : picky) {
            System.out.print(n+": ");
            System.out.println(ops.convertManipulation(n));
            System.out.println();
            System.out.println("SQL String: ");
            System.out.println(ops.convertToSqlString(n));
            System.out.println();
        }
    }

    /**
     * Why Not algorithm to find picky manipulations
     * @param roots - root nodes
     * @param dag - directed acyclic graph
     * @param parentCounts - counts of the parents
     * @param unpicked - the data item we are looking for
     */
    private List<RelNode> traverseAndFindPicky(List<RelNode> roots, Map<RelNode,ArrayList<RelNode>> dag,
                                      Map<RelNode,Integer> parentCounts, List<RelNode> sorted,
                                      Map.Entry<String,String> unpicked) {
        List<RelNode> possiblePicky = new ArrayList<>();
        List<RelNode> notPicky = new ArrayList<>();

        // traverse tree
        Map<RelNode,Boolean> visited = new HashMap<>();
        LinkedList<RelNode> queue = new LinkedList<>();
        for (RelNode item : sorted) {
            if (roots.contains(item)) {
                queue.add(item);
            }
        }
        while (queue.size() != 0) {
            RelNode r = queue.poll();
            visited.put(r,true);

            boolean successorExists = successorExists(r,unpicked);
            if (successorExists) {
                notPicky.add(r);
                if (dag.get(r) != null) {
                    for (RelNode next : dag.get(r)) {
                        parentCounts.replace(next, parentCounts.get(next) - 1);
                        // next has not been visited but all of its parents have
                        if (visited.get(next) == null && parentCounts.get(next) ==0) {
                            queue.add(next);
                        }
                    }
                }
            } else {
                removeParent(r,parentCounts,dag);
                possiblePicky.add(r);
            }
        }
        return postProcessPicky(dag,possiblePicky, notPicky);
    }

    /**
     * Helper function: remove parents for picky manipulations
     * @param r - relnode to reduce parents for
     * @param parentCounts - counts to fix
     * @param dag - graph
     */
    private void removeParent(RelNode r, Map<RelNode,Integer> parentCounts,Map<RelNode,ArrayList<RelNode>> dag) {
        LinkedList<RelNode> queue = new LinkedList<>();
        queue.add(r);
        while (queue.size() != 0) {
            RelNode next = queue.poll();
            for (RelNode n : dag.get(next)) {
                parentCounts.replace(n, parentCounts.get(n)-1);
                // this is the only path to the node
                // so we want to continue reducing parents
                if (parentCounts.get(n) == 0) {
                    queue.add(n);
                }
            }
        }
    }

    /**
     * Successor exists function to determine if unpicked data item is in the result set
     * @param queryNode - relnode to convert and run on database
     * @param unpicked data item we are looking for
     * @return true if data item exists else false
     */
    private boolean successorExists(RelNode queryNode, HashMap.Entry<String,String> unpicked) {
        Statement smt;
        try {
            smt = conn.createStatement();
            smt.setQueryTimeout(60);
            String query = ops.convertToSqlString(queryNode);

            ResultSet rs = smt.executeQuery(query);
            ResultSetMetaData rsmd = rs.getMetaData();
            boolean successorVisible = false;
            for (int i = 1; i<rsmd.getColumnCount(); i++) {
                if (unpicked.getKey().equals(rsmd.getColumnName(i))) {
                    successorVisible = true;
                }
            }
            // successor visibility
            if (successorVisible) {
                int columnIndex = rs.findColumn(unpicked.getKey());
                while (rs.next()) {
                    if (rs.getString(columnIndex).equals(unpicked.getValue())) {
                        rs.close();
                        smt.close();
                        return true;
                    }
                }
                rs.close();
                smt.close();
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false;
    }



    /**
     * Post process function to why not
     * Seeks to ensure that a picky manipulation does not have a picky manipulation as its successor
     * @param dag - directed acyclic graph to test
     * @param picky - list of manipulations flagged as picky
     * @return - final list of "frontier picky manipulations"
     */
    private List<RelNode> postProcessPicky(Map<RelNode,ArrayList<RelNode>> dag, List<RelNode> picky, List<RelNode> not) {
        List<RelNode> finalList = new ArrayList<>(picky);
        for (RelNode item : picky) {
            List<RelNode> children = dag.get(item);
                LinkedList<RelNode> queue = new LinkedList<>(children);
                while (queue.size() > 0) {
                    RelNode r = queue.poll();
                    if (picky.contains(r)) {
                        finalList.remove(item);
                    }
                    queue.addAll(dag.get(r));

                }
            }
            for (RelNode item : picky) {
                List<RelNode> children = dag.get(item);
                LinkedList<RelNode> queue = new LinkedList<>(children);
                while (queue.size() > 0) {
                    RelNode r = queue.poll();
                    if (not.contains(r)) {
                        finalList.remove(item);
                    }
                    queue.addAll(dag.get(r));
                }
            }
        return finalList;
    }
}
