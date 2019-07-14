package WhyNot;

import Util.*;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.*;

import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

/**
 * Class to work with generating the DAG and getting specific pieces of this dag
 * @author Corie Both
 * Created Jun 5, 2019
 */
public class DAG {
    /**
     * create the dag by parsing the Apache Calcite parse tree
     * @param sql - query to parse
     * @return WhyNot.DAG of queries from bottom up
     */
    public Map<RelNode,ArrayList<RelNode>> generateDAG(String sql, DatabaseConnection conn) {
        Map<RelNode,ArrayList<RelNode>> dag = new HashMap<>();
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

            // rel visitor
            List<RelNodeLink> entries = new ArrayList<>();
            RelVisitor rv = new RelVisitor() {
                @Override
                public void visit(RelNode node, int ordinal, RelNode parent) {
                    RelNodeLink e = new RelNodeLink(node,parent);
                    entries.add(e);
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

            ArrayList<RelNode> notAllocated = new ArrayList<>();
            for (RelNodeLink e : entries) {
                RelNode entry = e.entry;
                RelNode parent = e.parent;
                notAllocated.add(entry);

                if (!dag.containsKey(entry)) {
                    dag.put(entry,new ArrayList<>());
                }

                if (parent != null) {
                    dag.get(entry).add(parent);
                }
                for (RelNode item : dag.keySet()) {
                    if (dag.get(item).contains(parent)) {
                        notAllocated.remove(parent);
                    }
                }


            }
            dag.put(null,notAllocated);

            // now that DAG is initialized, write to file for inspection
            try {
                FileWriter visualizations =
                        new FileWriter("/Users/Corie/Desktop/Summer_2019/Project/WhyNot/src/data_structures.txt");
                visualizations.write("Why Not DAG:\n");
                visualizations.write("------------------------\n");
                for (HashMap.Entry e : dag.entrySet()) {
                    visualizations.write(e.getKey()+ " -> "+e.getValue()+"\n");
                }
                visualizations.write("\n\n");
                visualizations.close();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
        catch (SqlParseException | RelConversionException | ValidationException e) {
            e.printStackTrace();
        }

        return dag;
    }

    /**
     * Get parent count for each node
     * @param dag directed acyclic graph of relnodes
     * @return parent count map
     */
    public Map<RelNode,Integer> getParentCount(Map<RelNode,ArrayList<RelNode>> dag) {
        Map<RelNode,Integer> counts = new HashMap<>();
        for (RelNode entry : dag.keySet()) {
            for (RelNode child : dag.get(entry)) {
                if (!counts.containsKey(child)) {
                    counts.put(child,0);
                }
                if (entry != null) {
                    counts.replace(child, counts.get(child) + 1);
                }
            }
        }
        return counts;
    }

    /**
     * find the root node
     * @param dag - directed acyclic graph
     * @return list of root nodes
     */
    public List<RelNode> findRoot(Map<RelNode,ArrayList<RelNode>> dag) {
        List<RelNode> roots = new ArrayList<>();
        roots.addAll(dag.get(null));
        return roots;
    }

    /**
     * topological order sorting
     * @param dag directed acyclic graph to sort
     * @return stack of topological ordering
     */
    public List<RelNode> topologicalSort(Map<RelNode,ArrayList<RelNode>> dag) {
        Stack<RelNode> stack = new Stack<>();

        Map<RelNode,Boolean> visited = new HashMap<>();
        for (RelNode element : dag.keySet()) {
            visited.put(element,false);
        }

        for (RelNode el : visited.keySet()) {
            if (!visited.get(el)) {
                recursiveSort(stack,visited,dag,el);
            }
        }
        List<RelNode> sortedNodes = new ArrayList<>();
        while (!stack.empty()) {
            RelNode n = stack.pop();
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
    private void recursiveSort(Stack<RelNode> s, Map<RelNode,Boolean> visited,
                               Map<RelNode,ArrayList<RelNode>> dag,RelNode item ) {
        visited.replace(item,true);
        for (RelNode node : dag.get(item)) {
            if (!visited.get(node)) {
                recursiveSort(s,visited,dag,node);
            }
        }
        s.push(item);
    }

}
