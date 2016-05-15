/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package soayjoni.jdbcutil.util;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import soayjoni.jdbcutil.exception.DuplicateIndexError;
import soayjoni.jdbcutil.sqlmaker.SQLMaker;

/**
 *
 * @author mashuk
 */
public class DBOperation {

    protected java.sql.Connection con;
    protected java.sql.PreparedStatement ps;
    protected java.sql.Statement stmt;
    protected java.sql.ResultSet rs;
    protected java.sql.ResultSetMetaData rsmd;

    protected SQLMaker sQLMaker = new SQLMaker();
    protected String sql = "";

    private final DBUtil dBUtil;
    private final DBType dBType;

    public DBOperation(String dbUrl, DBType dBType) {
        this.dBType = dBType;
        switch (dBType) {
            case MY_SQL:
                dBUtil = new MySQLDBUtil(dbUrl);
                break;
            default:
                throw new AssertionError("Unsupported database type.");
        }
    }

    private boolean checkDuplicateIndex(Param[] params) {
        if (params == null) {
            throw new NullPointerException("params can not be null");
        }
        String indexContainer = "";
        for (Param param : params) {
            String index = "[" + param.getIndex() + "]";
            if (indexContainer.contains(index)) {
                throw new DuplicateIndexError("Duplicate index".concat(index).concat("appears in parameter ".concat(param.getName())));
            } else {
                indexContainer = indexContainer.concat(index);
            }
        }
        return false;
    }

    /**
     *
     * @param params
     * @throws java.sql.SQLException
     */
    protected void setPreparedStatementParams(Param... params) throws SQLException {
        if (ps == null || params.length == 0) {
            return;
        }
        checkDuplicateIndex(params);

        for (Param param : params) {
            ParamType paramType = param.getParamType();
            int parameterIndex = param.getIndex();
            Object obj = param.getValue();
            switch (paramType) {
                case BYTE:
                    Byte byt = null;
                    if (obj instanceof Byte) {
                        byt = (Byte) obj;
                    }
                    ps.setByte(parameterIndex, byt);
                    break;
                case BYTES:
                    byte[] bytes = null;
                    if (obj instanceof byte[]) {
                        bytes = (byte[]) obj;
                    }
                    ps.setBytes(parameterIndex, bytes);
                    break;
                case SHORT:
                    Short short1 = null;
                    if (obj instanceof Short) {
                        short1 = (Short) obj;
                    }
                    ps.setShort(parameterIndex, short1);
                    break;
                case INTEGER:
                    Integer i = null;
                    if (obj instanceof Integer) {
                        i = (Integer) obj;
                    }
                    ps.setInt(parameterIndex, i);
                    break;
                case DOUBLE:
                    Double d = null;
                    if (obj instanceof Double) {
                        d = (Double) obj;
                    }
                    ps.setDouble(parameterIndex, d);
                    break;
                case FLOAT:
                    Float f = null;
                    if (obj instanceof Float) {
                        f = (Float) obj;
                    }
                    ps.setFloat(parameterIndex, f);
                    break;
                case LONG:
                    Long l = null;
                    if (obj instanceof Long) {
                        l = (Long) obj;
                    }
                    ps.setLong(parameterIndex, l);
                    break;
                case BOOLEAN:
                    Boolean b = null;
                    if (obj instanceof Boolean) {
                        b = (Boolean) obj;
                    }
                    ps.setBoolean(parameterIndex, b);
                    break;
                case STRING:
                    String x = null;
                    if (obj instanceof String) {
                        x = (String) obj;
                    }
                    ps.setString(parameterIndex, x);
                    break;
                case BIG_DECIMAL:
                    java.math.BigDecimal bigDecimal = null;
                    if (obj instanceof java.math.BigDecimal) {
                        bigDecimal = (BigDecimal) obj;
                    }
                    ps.setBigDecimal(parameterIndex, bigDecimal);
                    break;
                case SQL_DATE:
                    java.sql.Date date = null;
                    if (obj instanceof java.sql.Date) {
                        date = (java.sql.Date) obj;
                    }
                    ps.setDate(parameterIndex, date);
                    break;
                default:
                    throw new AssertionError("Unsupported type of parameter: " + paramType);
            }
        }
    }

    public boolean openDB() throws SQLException {
        boolean bool = dBUtil.openDB();
        con = dBUtil.getConnection();
        return bool;
    }

    public void closeDB() {
        dBUtil.closeDB();
    }

    public long insertData(String table, Param... params) throws SQLException {
        long rtValue = 0;
        int columnCount = params.length;
        String[] cols = new String[columnCount];
        for (Param param : params) {
            cols[param.getIndex()] = param.getName();
        }
        if (openDB()) {
            try {
                sql = sQLMaker.insert(table, cols).values(columnCount).make();
                ps = con.prepareStatement(sql, PreparedStatement.RETURN_GENERATED_KEYS);
                setPreparedStatementParams(params);
                int i = ps.executeUpdate();
                if (i > 0) {
                    rtValue = ps.getGeneratedKeys().getLong(1);
                }
            } finally {
                closeDB();
            }
        }
        return rtValue;
    }
}
