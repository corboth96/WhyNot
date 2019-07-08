import org.apache.calcite.rel.RelNode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;


/**
 * Created by Corie on 7/8/19.
 */
public class HybridWhyNot {
    private DatabaseConnection conn;
    private List<Tab> tabQ;
    private List<RelNode> nonPickyManip;
    private List<AnswerTuple> pickyManip;
    private List<RelNode> emptyOutput;

    private List<HashMap<String,Object>> dirTc;
    private List<HashMap<String,Object>> inDirTc;

    private String runWhyNot(String sql, ConditionalTuple tc) {
        HybridDAG hDAG = new HybridDAG();
        List<HybridTab> sorted = hDAG.topologicalSort(hDAG.generateDAG(sql, conn));




        return getDetailedAnswer();

    }


    private boolean successorExists() {

        return false;
    }

    /**
     * Helper function to print out detailed, condensed and secondary answers
     * @return always true
     */
   private String getDetailedAnswer() {
        String answerString = "----------Detailed Answer:----------\n";
        answerString += "{\n";
        boolean isFirst = true;
        for (AnswerTuple answer : pickyManip) {
            for (HashMap<String,Object> ans : answer.getDetailed().keySet()) {
                if (isFirst) {
                    answerString += "(";
                    answerString += ans;
                    answerString += "," + answer.getDetailed().get(ans);
                    answerString += ")";
                    isFirst = false;
                } else {
                    answerString += ", \n";
                    answerString += "(";
                    answerString += ans;
                    answerString += "," + answer.getDetailed().get(ans);
                    answerString += ")";
                }
            }
        }
        answerString += "\n";
        answerString += "}\n";

        answerString += "----------Condensed Answer:----------\n";
        answerString += "{";
        isFirst = true;
        for (AnswerTuple answer : pickyManip) {
            if (isFirst) {
                answerString += answer.manipulation;
                isFirst = false;
            } else {
                answerString += ", "+answer.manipulation;
            }
        }
        answerString += "}\n";

        answerString += "----------Secondary Answer:----------\n";
        answerString += "(";
        isFirst = true;
        for (RelNode m : emptyOutput) {
            if (isFirst) {
                answerString += m;
                isFirst = false;
            } else {
                answerString += ", "+m;
            }
        }
        answerString += ")\n";


        return answerString;
    }

    public static void main(String[] args) {
        HybridWhyNot hwn = new HybridWhyNot();
        hwn.conn = new DatabaseConnection();
        hwn.conn.createConnection();

        String sql = "select m.movie_id,m.title,m.yearReleased from db.Movie m " +
                "left join db.MovieGenres mg on m.movie_id = mg.movie_id " +
                "left join db.Genre g on g.genre_id = mg.genre_id where m.movie_id in " +
                "(select movie_id from db.DirectedBy group by movie_id " +
                "having count(director_id)>=2) and g.genre = 'Action'";

        List<ConditionalTuple> predicate = new ArrayList<>();
        ConditionalTuple ct = new ConditionalTuple();
        ct.addVTuple("Movie.title", "Aladdin");
        predicate.add(ct);
        for (ConditionalTuple tc : predicate) {

        }
    }
}
