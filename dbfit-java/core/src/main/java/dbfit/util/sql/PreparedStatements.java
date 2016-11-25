package dbfit.util.sql;

import static dbfit.util.LangUtils.join;
import static dbfit.util.LangUtils.repeat;

public class PreparedStatements {
    private static String buildParamList(int numberOfParameters) {
        return join(repeat("?", numberOfParameters), ", ");
    }

    public static String buildStoredProcedureCall(String procName, int numberOfPararameters) {
System.out.println("PreparedStatements: buildStoredProcedureCall: name: " + procName);
        return "{ call " + procName + "(" + buildParamList(numberOfPararameters) + ")}";
    }

    public static String buildFunctionCall(String procName, int numberOfParameters) {
System.out.println("PreparedStatements: buildFunctionCall: name: " + procName);
        return "{ ? = call " + procName + "(" + buildParamList(numberOfParameters - 1) + ")}";
    }
}
