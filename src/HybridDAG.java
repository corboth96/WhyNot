import Util.*;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.*;

import java.util.*;

/**
 * Hybrid DAG for WhyNot/NedExplain integrated implementation
 * @author Corie Both
 * Date Created: Jul 8, 2019
 */
public class HybridDAG {
    public Map<HybridTab, ArrayList<HybridTab>> generateDAG(String sql, DatabaseConnection conn) {
        Map<HybridTab, ArrayList<HybridTab>> dag = new HashMap<>();
        try {
            SchemaPlus schema = conn.cc.getRootSchema().getSubSchema("DB");

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


            HashMap<RelNode, Integer> levels = new HashMap<>();
            RelVisitor rv = new RelVisitor() {
                @Override
                public void visit(RelNode node, int ordinal, RelNode parent) {
                    int level;
                    if (parent == null) {
                        level = 0;
                    } else {
                        level = levels.get(parent)+1;
                    }
                    levels.put(node,level);
                    HybridTab t = new HybridTab(level,node,parent);

                    if (parent == null) {
                        dag.put(t,new ArrayList<>());
                    }
                    else {
                        HybridTab parentTab = null;
                        for (HybridTab tab : dag.keySet()) {
                            if (tab.name != null) {
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
                        int nullLevel = levels.get(t.name)+1;
                        HybridTab nullTab = new HybridTab(nullLevel,null,null);
                        if (!dag.containsKey(nullTab)) {
                            dag.put(nullTab,new ArrayList<>());
                        }
                        dag.get(nullTab).add(t);

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
            if (n.name != null) {
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
                               Map<HybridTab,ArrayList<HybridTab>> dag,HybridTab item ) {
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
            if (name.name == null) {
                roots.addAll(dag.get(name));
            }
        }
        return roots;
    }

    public List<String> getTables(Map<HybridTab,ArrayList<HybridTab>> dag) {
        List<String> tables = new ArrayList<>();
        for (HybridTab name : dag.keySet()) {
            if (name.name != null) {
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

    public static void main(String[] args) {
        HybridDAG d = new HybridDAG();
        DatabaseConnection c = new DatabaseConnection();
        c.createConnection();
        String sql = "select m.movie_id,m.title,m.yearReleased from db.Movie m " +
                "left join db.MovieGenres mg on m.movie_id = mg.movie_id " +
                "left join db.Genre g on g.genre_id = mg.genre_id where m.movie_id in " +
                "(select movie_id from db.DirectedBy group by movie_id " +
                "having count(director_id)>=2) and g.genre = 'Action'";
        Map<HybridTab, ArrayList<HybridTab>> dag = d.generateDAG(sql,c);
        List<HybridTab> sorted = d.topologicalSort(dag);
        List<HybridTab> roots = d.findRoots(dag);
        for (HybridTab t : roots)
            System.out.println(t.name);

    }
}
