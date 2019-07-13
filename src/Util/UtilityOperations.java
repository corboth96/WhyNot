package Util;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Utility operations for methods used by 2 or more of the different algorithms
 * @author Corie Both
 * Date Created: Jul 13, 2019
 */
public class UtilityOperations {
    /**
     * Helper function to convert RelNode into SQL string to run
     * @param query relnode to convert
     * @return sql string to run
     */
    public String convertToSqlString(RelNode query) {
        SqlDialect.Context c = SqlDialect.EMPTY_CONTEXT.withDatabaseProductName(
                SqlDialect.DatabaseProduct.MYSQL.name()).withDatabaseProduct(SqlDialect.DatabaseProduct.MYSQL);
        SqlDialect dialect = new SqlDialect(c);
        RelToSqlConverter relToSqlConverter = new RelToSqlConverter(dialect);
        RelToSqlConverter.Result res;
        res = relToSqlConverter.visitChild(0,query);
        SqlNode newNode = res.asSelect();
        return newNode.toSqlString(dialect,false).getSql();
    }

    public String convertManipulation(RelNode query) {
        String cond = "";


        SqlDialect.Context c = SqlDialect.EMPTY_CONTEXT.withDatabaseProductName(
                SqlDialect.DatabaseProduct.MYSQL.name()).withDatabaseProduct(SqlDialect.DatabaseProduct.MYSQL);
        SqlDialect dialect = new SqlDialect(c);
        RelToSqlConverter relToSqlConverter = new RelToSqlConverter(dialect);
        RelToSqlConverter.Result res;


        if (query.getRelTypeName().equals("LogicalFilter")) {
            Filter filter = (Filter) query;
            cond = filter.getCondition().toString();
        } else if (query.getRelTypeName().equals("LogicalJoin")) {
            LogicalJoin join = (LogicalJoin) query;
            cond = join.getCondition().toString();
        }
        return cond;
    }

    /**
     * Helper function to run query and save tuples
     * @param sql - query to run
     * @return list of tuples from that query
     */
    public List<HashMap<String,Object>> runQuery(DatabaseConnection conn, String sql) {
        Statement smt;
        List<HashMap<String,Object>> tuples = new ArrayList<>();
        try {
            smt = conn.createStatement();
            ResultSet rs = smt.executeQuery(sql);

            List<String> columns = new ArrayList<>();
            List<String> colTypes = new ArrayList<>();
            for (int i = 1; i<=rs.getMetaData().getColumnCount();i++) {
                columns.add(rs.getMetaData().getColumnName(i));
                colTypes.add(rs.getMetaData().getColumnTypeName(i));
            }

            while (rs.next()) {
                HashMap<String,Object> tuple = new HashMap<>();
                for (int i = 0; i<columns.size();i++) {
                    Object col;
                    switch (colTypes.get(i)) {
                        case "INTEGER":
                            col = rs.getInt(i + 1);
                            break;
                        case "DATE":
                            col = rs.getDate(i + 1);
                            break;
                        case "TIME":
                            col = rs.getTime(i + 1);
                            break;
                        case "LONG":
                            col = rs.getLong(i + 1);
                            break;
                        case "FLOAT":
                            col = rs.getFloat(i + 1);
                            break;
                        case "DOUBLE":
                            col = rs.getDouble(i + 1);
                            break;
                        case "BOOLEAN":
                            col = rs.getBoolean(i + 1);
                            break;
                        case "DECIMAL":
                            col = rs.getBigDecimal(i + 1);
                            break;
                        case "TIMESTAMP":
                            col = rs.getTimestamp(i + 1);
                            break;
                        default:
                            col = rs.getString(i + 1);
                    }
                    tuple.put(columns.get(i), col);
                }
                tuples.add(tuple);
            }
            rs.close();
            smt.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return tuples;
    }

    /**
     * Helper function to print out detailed, condensed and secondary answers
     * @return the string with all of the answers
     */
    public String getDetailedAnswer(List<AnswerTuple> pickyManip, List<RelNode> emptyOutput) {
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
                answerString += answer.getManipulation();
                isFirst = false;
            } else {
                answerString += ", "+answer.getManipulation();
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
}
