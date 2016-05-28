/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package soayjoni.jdbcutil;

import java.math.BigDecimal;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import soayjoni.jdbcutil.exception.DuplicateIndexError;
import soayjoni.jdbcutil.util.Param;
import soayjoni.jdbcutil.util.ParamType;

/**
 *
 * @author mashuk
 */
public class StatementUtil {

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

    private Param[] sortParamByIndex(Param[] params) {
        if (params == null) {
            throw new NullPointerException("params can not be null");
        }
        if (params.length == 0) {
            return params;
        }
        Param[] sortedParams = new Param[params.length];
        for (Param param : params) {
            int index = param.getIndex();
            sortedParams[index] = param;
        }
        return sortedParams;
    }

    /**
     *
     * @param ps
     * @param params
     * @throws java.sql.SQLException
     */
    public void setPreparedStatementParams(java.sql.PreparedStatement ps, Param... params) throws SQLException {
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

    public Param[] resultSetToNameValuePair(java.sql.ResultSet rs) throws SQLException {
        if (rs == null) {
            throw new IllegalArgumentException("ResultSet is null");
        }

        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();
        Param[] params = new Param[columnCount];
        for (int column = 1; column <= params.length; column++) {
            String columnLabel = metaData.getColumnLabel(column);
            String columnClassName = metaData.getColumnClassName(column);
            ParamType paramType = getParamType(columnClassName);
            switch (paramType) {
                case BYTE:
                    byte byte1 = rs.getByte(columnLabel);
                    params[column] = new Param(column, paramType, columnLabel, byte1);
                    break;
                case BYTES:
                    byte[] bytes = rs.getBytes(columnLabel);
                    params[column] = new Param(column, paramType, columnLabel, bytes);
                    break;
                case SHORT:
                    short short1 = rs.getShort(columnLabel);
                    params[column] = new Param(column, paramType, columnLabel, short1);
                    break;
                case INTEGER:
                    int in = rs.getInt(columnLabel);
                    params[column] = new Param(column, paramType, columnLabel, in);
                case LONG:
                    long ln = rs.getLong(columnLabel);
                    params[column] = new Param(column, paramType, columnLabel, ln);
                case FLOAT:
                    Float ft = rs.getFloat(columnLabel);
                    params[column] = new Param(column, paramType, columnLabel, ft);
                case DOUBLE:
                    double db = rs.getDouble(columnLabel);
                    params[column] = new Param(column, paramType, columnLabel, db);
                case BOOLEAN:
                    boolean bl = rs.getBoolean(columnLabel);
                    params[column] = new Param(column, paramType, columnLabel, bl);
                case STRING:
                    String st = rs.getString(columnLabel);
                    params[column] = new Param(column, paramType, columnLabel, st);
                case BIG_DECIMAL:
                    java.math.BigDecimal bd = rs.getBigDecimal(columnLabel);
                    params[column] = new Param(column, paramType, columnLabel, bd);
                case SQL_DATE:
                    java.sql.Date dt = rs.getDate(columnLabel);
                    params[column] = new Param(column, paramType, columnLabel, dt);
                case SQL_TIME:
                    java.sql.Time tm = rs.getTime(columnLabel);
                    params[column] = new Param(column, paramType, columnLabel, tm);
                case SQL_TIMESTAMP:
                    java.sql.Timestamp tmst = rs.getTimestamp(columnLabel);
                    params[column] = new Param(column, paramType, columnLabel, tmst);
                default:
                    throw new AssertionError("Unsupported column type: " + paramType);
            }
        }
        return params;
    }

    private ParamType getParamType(String columnClassName) throws SQLException {
        if (columnClassName.equals("java.lang.Boolean")) {
            return ParamType.BOOLEAN;
        } else if (columnClassName.equals("java.lang.Short")) {
            return ParamType.SHORT;
        } else if (columnClassName.equals("java.lang.Integer")) {
            return ParamType.INTEGER;
        } else if (columnClassName.equals("java.math.BigInteger")) {
            return ParamType.LONG;
        } else if (columnClassName.equals("java.lang.Long")) {
            return ParamType.LONG;
        } else if (columnClassName.equals("java.lang.Double")) {
            return ParamType.DOUBLE;
        } else if (columnClassName.equals("java.lang.Float")) {
            return ParamType.FLOAT;
        } else if (columnClassName.equals("java.math.BigDecimal")) {
            return ParamType.BIG_DECIMAL;
        } else if (columnClassName.equals("java.lang.String")) {
            return ParamType.STRING;
        } else if (columnClassName.equals("java.sql.Time")) {
            return ParamType.SQL_TIME;
        } else if (columnClassName.equals("java.sql.Date")) {
            return ParamType.SQL_DATE;
        } else if (columnClassName.equals("java.sql.Timestamp")) {
            return ParamType.SQL_TIMESTAMP;
        } else if (columnClassName.equals("[B")) {
            return ParamType.BYTES;
        } else {
            throw new UnsupportedOperationException("Unsupported column class type: " + columnClassName);
        }
    }
}
