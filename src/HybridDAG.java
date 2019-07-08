import org.apache.calcite.adapter.java.Array;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.*;

import java.sql.Connection;
import java.util.*;

/**
 * Created by Corie on 7/8/19.
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
                    System.out.println(node);
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
                    /*else if (node.getRelTypeName().equals("JdbcTableScan")) {
                        int nullLevel = level+1;
                        HybridTab nullTab = new HybridTab(nullLevel,null,null);
                        if (!dag.containsKey(nullTab)) {
                            dag.put(nullTab,new ArrayList<>());
                        }
                        dag.get(nullTab).add(t);

                    }*/
                    else {
                        System.out.println("\tParent: " + parent);
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
                        int nullLevel = level+1;
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
        for (HybridTab t : sorted) {
            System.out.println(t.name);
        }
    }
}
