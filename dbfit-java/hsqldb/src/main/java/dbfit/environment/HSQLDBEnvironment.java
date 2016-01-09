package dbfit.environment;

import dbfit.annotations.DatabaseEnvironment;
import dbfit.util.DbParameterAccessor;
import dbfit.util.NameNormaliser;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static dbfit.util.Direction.*;

/**
 * Provides support for testing HSQLDB databases.
 *
 * @author Jerome Mirc, jerome.mirc@gmail.com
 */
@DatabaseEnvironment(name="HSQLDB", driver="org.hsqldb.jdbcDriver")
public class HSQLDBEnvironment extends AbstractDbEnvironment {

    private TypeMapper typeMapper = new HsqldbTypeMapper();

    public HSQLDBEnvironment(String driverClassName) {
        super(driverClassName);
    }

    /**
     * This method has been overwrided as currently the
     * prepareStatement(java.lang.String sql, int autoGeneratedKeys) method is
     * not supported by HSQLDB
     *
     * see AbstractDbEnvironment#buildInsertPreparedStatement
     */
    public PreparedStatement buildInsertPreparedStatement(String tableName,
            DbParameterAccessor[] accessors) throws SQLException {
        return getConnection().prepareStatement(
                buildInsertCommand(tableName, accessors));
    }

    @Override
    protected String getConnectionString(String dataSource) {
        return String.format("jdbc:hsqldb:%s", dataSource);
    }

    @Override
    protected String getConnectionString(String dataSource, String database) {
        throw new UnsupportedOperationException();
    }

    private static final String paramNamePattern = "@([A-Za-z0-9_]+)";
    private static final Pattern paramRegex = Pattern.compile(paramNamePattern);

    @Override
    protected Pattern getParameterPattern() {
        return paramRegex;
    }

    public Map<String, DbParameterAccessor> getAllColumns(String tableOrViewName)
            throws SQLException {
        String qry = "SELECT COLUMN_NAME, TYPE_NAME FROM INFORMATION_SCHEMA.SYSTEM_COLUMNS\n"
                + "WHERE TABLE_NAME = ?";
        return readIntoParams(tableOrViewName, qry);
    }

    private Map<String, DbParameterAccessor> readIntoParams(
            String tableOrViewName, String query) throws SQLException {
        try (PreparedStatement dc = getConnection().prepareStatement(query)) {

            String tvname;
            if (tableOrViewName.trim().startsWith("\"") && tableOrViewName.trim().endsWith("\"")) {
                // Remove double quotes.
                tvname = tableOrViewName.replaceFirst("\"", "");
                int i = tvname.lastIndexOf("\"");
                tvname = tvname.substring(0, i) + tvname.substring(i + 1);
            } else {
                tvname = tableOrViewName.toUpperCase();
            }

            dc.setString(1, tvname);
            ResultSet rs = dc.executeQuery();
            Map<String, DbParameterAccessor> allParams = new HashMap<String, DbParameterAccessor>();
            int position = 0;
            while (rs.next()) {
                String columnName = rs.getString(1);
                String dataType = rs.getString(2);
                DbParameterAccessor dbp = createDbParameterAccessor(
                        columnName,
                        INPUT,
                        typeMapper.getJDBCSQLTypeForDBType(dataType),
                        getJavaClass(dataType), position++);
                allParams.put(NameNormaliser.normaliseName(columnName), dbp);
            }
            rs.close();
            return allParams;
        }
    }

    public Map<String, DbParameterAccessor> getAllProcedureParameters(String s)
            throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Class<?> getJavaClass(String dataType) {
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
     * From http://hsqldb.org/doc/guide/ch09.html#schemanaming-section
     */
    public static class HsqldbTypeMapper implements TypeMapper {
        private static final List<String> stringTypes = Arrays.asList(
                "VARCHAR", "VARCHAR_IGNORECASE", "CHAR", "CHARACTER",
                "LONGVARCHAR");
        private static final List<String> intTypes = Arrays.asList("INTEGER",
                "INT");
        private static final List<String> longTypes = Arrays.asList("BIGINT");
        private static final List<String> doubleTypes = Arrays.asList("DOUBLE",
                "DOUBLE PRECISION", "FLOAT", "REAL");
        private static final List<String> shortTypes = Arrays
                .asList("SMALLINT");
        private static final List<String> decimalTypes = Arrays.asList(
                "DECIMAL", "NUMERIC");
        private static final List<String> dateTypes = Arrays.asList("DATE");
        private static final List<String> timestampTypes = Arrays.asList(
                "TIMESTAMP", "DATETIME");
        private static final List<String> timeTypes = Arrays.asList("TIME");
        private static final List<String> booleanTypes = Arrays.asList(
                "BOOLEAN", "BIT");
        private static final List<String> byteTypes = Arrays.asList("BINARY",
                "VARBINARY", "LONGVARBINARY");

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
            if (shortTypes.contains(dataType))
                return Short.class;
            if (doubleTypes.contains(dataType))
                return Double.class;
            if (longTypes.contains(dataType))
                return Long.class;
            if (timestampTypes.contains(dataType))
                return java.sql.Timestamp.class;
            if (booleanTypes.contains(dataType))
                return Boolean.class;
            if (byteTypes.contains(dataType))
                return Byte.class;
            throw new UnsupportedOperationException("Type '" + dbDataType
                    + "' is not supported for HSQLDB");
        }

        public int getJDBCSQLTypeForDBType(final String dbDataType) {
            String dataType = normaliseTypeName(dbDataType);
            if (stringTypes.contains(dataType))
                return java.sql.Types.VARCHAR;
            if (decimalTypes.contains(dataType))
                return java.sql.Types.DECIMAL;
            if (intTypes.contains(dataType) || shortTypes.contains(dataType))
                return java.sql.Types.INTEGER;
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
            if (booleanTypes.contains(dataType))
                return java.sql.Types.BOOLEAN;
            if (byteTypes.contains(dataType))
                return java.sql.Types.BLOB;
            throw new UnsupportedOperationException("Type '" + dbDataType
                    + "' is not supported for HSQLDB");
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
}

