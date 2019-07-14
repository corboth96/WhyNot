import HybridWhyNot.HybridWhyNot;
import NedExplain.NedExplain;
import Util.*;
import WhyNot.WhyNot;

import java.util.HashMap;
import java.util.List;
import java.util.Scanner;

/**
 * Main class for running the three algorithms: WhyNot, NedExplain, HybridWhyNot
 * @author Corie Both
 * Date Created: Jul 13, 2019
 */
public class RunAlgorithms {
    public static void main(String[] args) {
        DatabaseConnection conn = new DatabaseConnection();

        // initialize our 3 algorithms
        NedExplain ne = new NedExplain(conn);
        WhyNot whyNot = new WhyNot(conn);
        HybridWhyNot hybridWhyNot = new HybridWhyNot(conn);

        // get input of which query to run
        Scanner scan = new Scanner(System.in);
        System.out.print("Enter input: ");
        int input = scan.nextInt();

        // get info
        Queries q = new Queries();
        String sql = q.getQuery(input);
        List<ConditionalTuple> predicates = q.getPredicate(input);
        HashMap<String,String> unpickeds = q.getUnpicked(input);

        System.out.println("---------------------------WhyNot---------------------------");
        whyNot.whyNot_Run(sql,unpickeds);
        System.out.println();

        System.out.println("-------------------------NedExplain-------------------------");
        ne.NedExplain_Run(sql,predicates);

        System.out.println("---------------------------Hybrid---------------------------");
        hybridWhyNot.HybridWhyNot_Run(sql, predicates);

        conn.closeConnection();
    }
}
