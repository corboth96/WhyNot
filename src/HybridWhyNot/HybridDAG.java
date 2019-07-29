package HybridWhyNot;

import Util.*;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.externalize.RelXmlWriter;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.*;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.*;

/**
 * DAG for WhyNot/NedExplain integrated implementation
 * @author Corie Both
 * Date Created: Jul 8, 2019
 */
public class HybridDAG {
    public String agg;
    public String condition;


    public Map<HybridTab, ArrayList<HybridTab>> generateDAG(String sql, DatabaseConnection conn) {
        Map<HybridTab, ArrayList<HybridTab>> dag = new HashMap<>();
        try {
            SchemaPlus schema = conn.getCc().getRootSchema().getSubSchema("DB");

            Frameworks.ConfigBuilder cb = Frameworks.newConfigBuilder(
            ).defaultSchema(schema).parserConfig(SqlParser.configBuilder().setCaseSensitive(false).build());
            FrameworkConfig config = cb.build();
            Planner p = Frameworks.getPlanner(config);

            // parses and validates sql
            SqlNode sqlNode = p.parse(sql);

            // must call validate before conversion
            SqlNode validatedNode = p.validate(sqlNode);

            // Converts a SQL parse tree into a tree of relational expressions
            RelRoot relRoot = p.rel(validatedNode);
            RelNode relNode = relRoot.rel;
            getAgg(sql,relNode);

            RelVisitor rv = new RelVisitor() {
                @Override
                public void visit(RelNode node, int ordinal, RelNode parent) {
                    HybridTab t = new HybridTab(node,parent);

                    if (parent == null) {
                        dag.put(t,new ArrayList<>());
                    }
                    else {
                        HybridTab parentTab = null;
                        for (HybridTab tab : dag.keySet()) {
                            if (tab != null) {
                                if (tab.name.equals(parent)) {
                                    parentTab = tab;
                                    break;
                                }
                            }
                        }
                        dag.put(t,new ArrayList<>());
                        dag.get(t).add(parentTab);
                    }

                    if (node.getRelTypeName().equals("JdbcTableScan")) {
                        if (!dag.containsKey(null)) {
                            dag.put(null,new ArrayList<>());
                        }
                        dag.get(null).add(t);

                    }
                    super.visit(node, ordinal, parent);
                }

                @Override
                public void replaceRoot(RelNode node) {
                    super.replaceRoot(node);
                }

                @Override
                public RelNode go(RelNode p) {
                    return super.go(p);
                }
            };
            rv.go(relNode);
        }
        catch (SqlParseException | RelConversionException | ValidationException e) {
            e.printStackTrace();
        }

        return dag;
    }

    /**
     * get the Aggregate that is being used for
     * the query
     * @param sql - query
     * @param r - top relnode
     */
    private void getAgg(String sql, RelNode r) {
        if (r.getRelTypeName().equals("LogicalAggregate")) {
            LogicalAggregate aggNode = (LogicalAggregate) r;

            String aggName = aggNode.getAggCallList().get(0).getAggregation().getName();
            agg = aggName;

            boolean found = false;
            String unqualifiedVar = null;
            String[] spaceSplit = sql.split(" ");
            for (String i : spaceSplit) {
                if (i.toUpperCase().contains(aggName+"(")) {
                    String[] args = i.split("\\(|\\)");
                    String var = args[1];
                    if (var.contains(".")) {
                        unqualifiedVar = var.split("\\.")[1];
                        found = true;
                    }
                }
                if (found) {
                    condition = unqualifiedVar;
                    break;
                }
            }
        }
    }


    /**
     * topological order sorting
     * @param dag directed acyclic graph to sort
     * @return stack of topological ordering
     */
    public List<HybridTab> topologicalSort(Map<HybridTab,ArrayList<HybridTab>> dag) {
        Stack<HybridTab> stack = new Stack<>();

        Map<HybridTab,Boolean> visited = new HashMap<>();
        for (HybridTab element : dag.keySet()) {
            visited.put(element,false);
        }

        for (HybridTab el : visited.keySet()) {
            if (!visited.get(el)) {
                recursiveSort(stack,visited,dag,el);
            }
        }
        List<HybridTab> sortedNodes = new ArrayList<>();
        while (!stack.empty()) {
            HybridTab n = stack.pop();
            if (n != null) {
                sortedNodes.add(n);
            }
        }
        return sortedNodes;
    }

    /**
     * topological sort recursive call
     * @param s - the stack to add to
     * @param visited - map keeping track of if we have seen the node yet
     * @param dag - directed acyclic graph to sort
     * @param item - item we are currently looking at
     */
    private void recursiveSort(Stack<HybridTab> s, Map<HybridTab,Boolean> visited,
                               Map<HybridTab,ArrayList<HybridTab>> dag, HybridTab item ) {
        visited.replace(item,true);
        for (HybridTab node : dag.get(item)) {
            if (!visited.get(node)) {
                recursiveSort(s,visited,dag,node);
            }
        }
        s.push(item);
    }

    /**
     * find the root node
     * @param dag - directed acyclic graph
     * @return list of root nodes
     */
    public List<HybridTab> findRoots(Map<HybridTab,ArrayList<HybridTab>> dag) {
        List<HybridTab> roots = new ArrayList<>();
        for (HybridTab name : dag.keySet()) {
            if (name == null) {
                roots.addAll(dag.get(name));
            }
        }
        return roots;
    }

    public List<String> getTables(Map<HybridTab,ArrayList<HybridTab>> dag) {
        List<String> tables = new ArrayList<>();
        for (HybridTab name : dag.keySet()) {
            if (name != null) {
                if (name.name.getRelTypeName().equals("JdbcTableScan")) {
                    String tablename = name.name.getTable().getQualifiedName().get(1);
                    if (!tables.contains(tablename)) {
                        tables.add(tablename);
                    }
                }
            }
        }
        return tables;
    }

    /**
     * Get parent count for each node
     * @param dag directed acyclic graph of relnodes
     * @return parent count map
     */
    public Map<RelNode,Integer> getParentCount(Map<HybridTab,ArrayList<HybridTab>> dag) {
        Map<RelNode,Integer> counts = new HashMap<>();
        for (HybridTab entry : dag.keySet()) {
            for (HybridTab child : dag.get(entry)) {
                if (!counts.containsKey(child.name)) {
                    counts.put(child.name,0);
                }
                if (entry != null) {
                    counts.replace(child.name, counts.get(child.name) + 1);
                }
            }
        }
        return counts;
    }
}
