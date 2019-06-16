import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;

import java.sql.*;
import java.util.*;

/**
 * Class to run the why not algorithm
 *  Creates the dag and then runs the main algorithm
 * @author Corie Both
 * @date June 5, 2019
 */
public class WhyNot {
    static DatabaseConnection conn = null;

    public WhyNot(DatabaseConnection c) {
        conn = c;
    }

    public void WhyNot(String sql, Map.Entry<String,String> unpicked) {
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
            System.out.println(convertManipulation(n));
            System.out.println(convertToSqlString(n));
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
            /*System.out.println("\t"+r);
            System.out.println("\t"+successorExists);
            System.out.println(parentCounts);*/
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
        //return testPost(dag,possiblePicky,notPicky);
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
            smt = conn.con.createStatement();
            smt.setQueryTimeout(60);
            String query = convertToSqlString(queryNode);
            //System.out.println(query);

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
     * Helper function to convert RelNode into SQL string to run
     * @param query relnode to convert
     * @return sql string to run
     */
    private String convertToSqlString(RelNode query) {
        SqlDialect.Context c = SqlDialect.EMPTY_CONTEXT.withDatabaseProductName(
                SqlDialect.DatabaseProduct.MYSQL.name()).withDatabaseProduct(SqlDialect.DatabaseProduct.MYSQL);
        SqlDialect dialect = new SqlDialect(c);
        RelToSqlConverter relToSqlConverter = new RelToSqlConverter(dialect);
        RelToSqlConverter.Result res;
        res = relToSqlConverter.visitChild(0,query);
        SqlNode newNode = res.asSelect();
        return newNode.toSqlString(dialect,false).getSql();
    }

    private String convertManipulation(RelNode query) {
        String cond = "";


        SqlDialect.Context c = SqlDialect.EMPTY_CONTEXT.withDatabaseProductName(
                SqlDialect.DatabaseProduct.MYSQL.name()).withDatabaseProduct(SqlDialect.DatabaseProduct.MYSQL);
        SqlDialect dialect = new SqlDialect(c);
        RelToSqlConverter relToSqlConverter = new RelToSqlConverter(dialect);
        RelToSqlConverter.Result res;


        if (query.getRelTypeName().equals("LogicalFilter")) {
            Filter filter = (Filter) query;

            System.out.println();
            System.out.println("Convert manipulation:");
            cond = filter.getCondition().toString();

            res = relToSqlConverter.visit(filter);
            List<SqlNode> newNode = res.qualifiedContext().fieldList();
            System.out.println(res.asFrom().toSqlString(dialect).getSql());

        } else if (query.getRelTypeName().equals("LogicalJoin")) {
            LogicalJoin join = (LogicalJoin) query;
            cond = join.getCondition().toString();
        }
        return cond;
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


    private List<RelNode> testPost(Map<RelNode,ArrayList<RelNode>> dag, List<RelNode> picky, List<RelNode> not) {
        List<RelNode> finalList = new ArrayList<>(picky);
        for (RelNode item : picky) {
            List<RelNode> children = dag.get(item);
            LinkedList<RelNode> queue = new LinkedList<>(children);
            while (queue.size() > 0) {
                RelNode r = queue.poll();
                if (picky.contains(r)) {
                    finalList.remove(item);
                    queue.clear();
                } else {
                    queue.addAll(dag.get(r));
                }

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
