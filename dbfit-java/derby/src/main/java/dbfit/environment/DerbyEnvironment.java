package dbfit.environment;

import dbfit.annotations.DatabaseEnvironment;
import dbfit.api.AbstractDbEnvironment;
import dbfit.util.DbParameterAccessor;
import dbfit.util.NameNormaliser;

import static dbfit.util.Direction.RETURN_VALUE;

import java.math.BigDecimal;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import dbfit.util.Direction;

/**
 * Encapsulates support for the Derby database (also known as JavaDB). Operates
 * in Client mode.
 *
 * @see EmbeddedDerbyEnvironment
 * @author P&aring;l Brattberg, pal.brattberg@acando.com
 */
@DatabaseEnvironment(name="Derby", driver="org.apache.derby.jdbc.ClientDriver")
public class DerbyEnvironment extends AbstractDbEnvironment {
    private TypeMapper typeMapper = new DerbyTypeMapper();

    public DerbyEnvironment(String driverClassName) {
        super(driverClassName);
    }

    @Override
    protected String getConnectionString(String dataSource) {
        return String.format("jdbc:derby://%s", dataSource);
    }

    @Override
    protected String getConnectionString(String dataSource, String database) {
        return String.format("jdbc:derby://%s/%s", dataSource, database);
    }

    private static final String paramNamePattern = "@([A-Za-z0-9_]+)";
    private static final Pattern paramRegex = Pattern.compile(paramNamePattern);

    @Override
    public Pattern getParameterPattern() {
        return paramRegex;
    }

    public Map<String, DbParameterAccessor> getAllColumns(
            final String tableOrViewName) throws SQLException {
        String qry = "SELECT COLUMNNAME, COLUMNDATATYPE "
                + "FROM SYS.SYSCOLUMNS WHERE REFERENCEID = "
                + "(SELECT TABLEID FROM SYS.SYSTABLES WHERE TABLENAME = ?)";
        return readIntoParams(tableOrViewName.toUpperCase(), qry);
    }

    private Map<String, DbParameterAccessor> readIntoParams(
            String tableOrViewName, String query) throws SQLException {
        try (PreparedStatement dc = getConnection().prepareStatement(query)) {
            dc.setString(1, tableOrViewName);

            ResultSet rs = dc.executeQuery();
            Map<String, DbParameterAccessor> allParams = new HashMap<String, DbParameterAccessor>();
            int position = 0;
            while (rs.next()) {
                String columnName = rs.getString(1);
                String dataType = rs.getString(2);
                DbParameterAccessor dbp = createDbParameterAccessor(
                        columnName,
                        Direction.INPUT,
                        typeMapper.getJDBCSQLTypeForDBType(dataType),
                        getJavaClass(dataType), position++);
                allParams.put(NameNormaliser.normaliseName(columnName), dbp);
            }
            rs.close();
            return allParams;
        }
    }

    public Map<String, DbParameterAccessor> getAllProcedureParameters(String procName)
                                                                  throws SQLException {
        String[] qualifiers = procName.split("\\.");
        String schemaName, procedureName;
        if (qualifiers.length == 2) {
            schemaName = qualifiers[0];
            procedureName = qualifiers[1];
        } else {
            schemaName = getConnection().getSchema();
            procedureName = qualifiers[0];
        }
        procedureName = procedureName.toUpperCase();
        DatabaseMetaData dbmd = getConnection().getMetaData();
        ResultSet rs;
        rs = dbmd.getProcedures(null, schemaName, procedureName);
        boolean isFunction = false;
        if (rs.next()) {
            rs.close();
            rs = dbmd.getProcedureColumns(null, schemaName, procedureName, "%");
        } else {
            rs.close();
            rs = dbmd.getFunctions(null, schemaName, procedureName);
            if (rs.next()) {
                isFunction = true;
                rs.close();
                rs = dbmd.getFunctionColumns(null, schemaName, procedureName, "%");
            }
        }
        Map<String, DbParameterAccessor> allParams = new HashMap<String, DbParameterAccessor>();
        int position = 0;
        while (rs.next()) {
            String paramName = rs.getString("COLUMN_NAME");
            if (paramName == null)
                paramName = "";
            final String dataType = rs.getString("TYPE_NAME");
            Direction paramDirection;
            if (isFunction) {
                switch (rs.getInt("COLUMN_TYPE")) {            	
                    case DatabaseMetaData.functionColumnIn:
                        paramDirection = Direction.INPUT;
                        break;
                    case DatabaseMetaData.functionReturn:
                        paramDirection = Direction.RETURN_VALUE;
                        break; 
                    default:
                        throw new UnsupportedOperationException(
                            "Type " + rs.getInt("COLUMN_TYPE") + " is not supported in function parameters");
                }
            } else {
                switch (rs.getInt("COLUMN_TYPE")) {
                    case DatabaseMetaData.procedureColumnIn:
                        paramDirection = Direction.INPUT;
                        break;
                    case DatabaseMetaData.procedureColumnInOut:
                        paramDirection = Direction.INPUT_OUTPUT;
                        break;
                    case DatabaseMetaData.procedureColumnOut:
                        paramDirection = Direction.OUTPUT;
                        break;
                    case DatabaseMetaData.procedureColumnReturn:
                        paramDirection = Direction.RETURN_VALUE;
                        break;
                    default:
                        throw new UnsupportedOperationException(
                            "Type " + rs.getInt("COLUMN_TYPE") + " is not supported in procedure parameters");
                }
            }
            DbParameterAccessor dbp = createDbParameterAccessor(
                    paramName,
                    paramDirection, typeMapper.getJDBCSQLTypeForDBType(dataType),
                    getJavaClass(dataType),
                    paramDirection == RETURN_VALUE ? -1 : position++);
            allParams.put(NameNormaliser.normaliseName(paramName), dbp);
        }
        rs.close();
        return allParams;
    }

    public Class<?> getJavaClass(final String dataType) {
        return typeMapper.getJavaClassForDBType(dataType);
    }

    /**
     * Interface for mapping of db types to java types.
     */
    public static interface TypeMapper {
        Class<?> getJavaClassForDBType(final String dbDataType);

        int getJDBCSQLTypeForDBType(final String dbDataType);
    }

    /**
     * From http://db.apache.org/derby/docs/10.4/ref/ref-single.html
     */
    public static class DerbyTypeMapper implements TypeMapper {
        private static final List<String> stringTypes = Arrays
                .asList(new String[] { "CHAR", "CHARACTER", "LONG VARCHAR",
                        "VARCHAR", "XML", "CHAR VARYING", "CHARACTER VARYING",
                        "LONG VARCHAR FOR BIT DATA", "VARCHAR FOR BIT DATA" });
        private static final List<String> intTypes = Arrays
                .asList(new String[] { "INTEGER", "INT" });
        private static final List<String> longTypes = Arrays
                .asList(new String[] { "BIGINT", });
        private static final List<String> doubleTypes = Arrays
                .asList(new String[] { "DOUBLE", "DOUBLE PRECISION", "FLOAT" });
        private static final List<String> floatTypes = Arrays
                .asList(new String[] { "REAL" });
        private static final List<String> shortTypes = Arrays
                .asList(new String[] { "SMALLINT" });
        private static final List<String> decimalTypes = Arrays
                .asList(new String[] { "DECIMAL", "DEC", "NUMERIC" });
        private static final List<String> dateTypes = Arrays
                .asList(new String[] { "DATE" });
        private static final List<String> timestampTypes = Arrays
                .asList(new String[] { "TIMESTAMP", });
        private static final List<String> timeTypes = Arrays
                .asList(new String[] { "TIME" });

        public Class<?> getJavaClassForDBType(final String dbDataType) {
            String dataType = normaliseTypeName(dbDataType);
            if (stringTypes.contains(dataType))
                return String.class;
            if (decimalTypes.contains(dataType))
                return BigDecimal.class;
            if (intTypes.contains(dataType))
                return Integer.class;
            if (timeTypes.contains(dataType))
                return Time.class;
            if (dateTypes.contains(dataType))
                return java.sql.Date.class;
            if (floatTypes.contains(dataType))
                return Float.class;
            if (shortTypes.contains(dataType))
                return Short.class;
            if (doubleTypes.contains(dataType))
                return Double.class;
            if (longTypes.contains(dataType))
                return Long.class;
            if (timestampTypes.contains(dataType))
                return java.sql.Timestamp.class;
            throw new UnsupportedOperationException("Type '" + dbDataType
                    + "' is not supported for Derby");
        }

        public int getJDBCSQLTypeForDBType(final String dbDataType) {
            String dataType = normaliseTypeName(dbDataType);
            if (stringTypes.contains(dataType))
                return java.sql.Types.VARCHAR;
            if (decimalTypes.contains(dataType))
                return java.sql.Types.DECIMAL;
            if (intTypes.contains(dataType) || shortTypes.contains(dataType))
                return java.sql.Types.INTEGER;
            if (floatTypes.contains(dataType))
                return java.sql.Types.FLOAT;
            if (doubleTypes.contains(dataType))
                return java.sql.Types.DOUBLE;
            if (longTypes.contains(dataType))
                return java.sql.Types.BIGINT;
            if (timestampTypes.contains(dataType))
                return java.sql.Types.TIMESTAMP;
            if (timeTypes.contains(dataType))
                return java.sql.Types.TIME;
            if (dateTypes.contains(dataType))
                return java.sql.Types.DATE;
            throw new UnsupportedOperationException("Type '" + dbDataType
                    + "' is not supported for Derby");
        }

        private static String normaliseTypeName(String type) {
            if (type != null && !"".equals(type)) {
                String dataType = type.toUpperCase().trim();
                // remove any size declarations such as CHAR(nn)
                int idxLeftPara = dataType.indexOf('(');
                if (idxLeftPara > 0) {
                    dataType = dataType.substring(0, idxLeftPara);
                }
                // remove any modifiers such as CHAR NOT NULL, but keep support
                // for INTEGER UNSIGNED. Yes, I know this is funky coding, but
                // it works, just see the unit tests! ;)
                idxLeftPara = dataType.indexOf(" NOT NULL");
                if (idxLeftPara > 0) {
                    dataType = dataType.substring(0, idxLeftPara);
                }
                idxLeftPara = dataType.indexOf(" NULL");
                if (idxLeftPara > 0) {
                    dataType = dataType.substring(0, idxLeftPara);
                }
                return dataType;
            } else {
                throw new IllegalArgumentException(
                        "You must specify a valid type for conversions");
            }
        }
    }

    @Override
    public String getActualErrorCode(SQLException e) {
        if (e.getSQLState().equals("38000")) { // A database-side method has thrown an exception. 
            return e.getNextException().getSQLState();        	
        } else {
        	return e.getSQLState();
        }
    }
}
