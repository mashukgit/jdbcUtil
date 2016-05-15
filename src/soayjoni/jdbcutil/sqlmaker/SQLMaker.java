/*
 * Copyright (c) 2016 Soayjoni IT and/or its affiliates. All rights reserved.
 */
package soayjoni.jdbcutil.sqlmaker;

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 *
 * @author Mashukur Rahman
 */
public class SQLMaker {

    protected static final String SELECT = "SELECT ";
    protected static final String INSERT = "INSERT ";
    protected static final String UPDATE = "UPDATE ";
    protected static final String DELETE = "DELETE ";
    protected static final String FROM = " FROM ";
    protected static final String WHERE = " WHERE ";
    protected static final String JOIN = " JOIN ";
    protected static final String INNER_JOIN = " INNER JOIN ";
    protected static final String LEFT_JOIN = " LEFT JOIN ";
    protected static final String RIGHT_JOIN = " RIGHT JOIN ";
    protected static final String OUTER_JOIN = " OUTER JOIN ";
    protected static final String AND = " AND ";
    protected static final String OR = " OR ";
    protected static final String BETWEEN = " BETWEEN ";
    protected static final String LIKE = " LIKE ";
    protected static final String INTO = " INTO ";
    protected static final String VALUES = " VALUES ";
    protected static final String ORDER_BY = " ORDER BY ";
    protected static final String LIMIT = " LIMIT ";
    protected static final String DESC = " DESC";
    protected static final String ASC = " ASC";
    protected static final String AS = " AS ";
    protected static final String AVG = " AVG ";
    protected static final String IN = " IN ";
    protected static final String NOT_IN = " NOT IN ";
    protected static final String ANY = " ANY ";
    protected static final String ALL = " ALL ";
    protected static final String COMA = ", ";
    protected static final String STAR = " * ";
    protected static final String QUE = " ?";
    protected static final String EQ_QUE = "=?";
    protected static final String EQ = " = ";
    protected static final String NOT_EQ = " <> ";
    protected static final String GT_EQ = " >= ";
    protected static final String LT_EQ = " <= ";
    protected static final String GT = " > ";
    protected static final String LT = " < ";
    protected static final String SPACE = " ";
    protected static final String F_BRACE_START = " (";
    protected static final String F_BRACE_END = ") ";
    protected static final String DOUBLE_COTETION = "\"";
    protected static final String SINGLE_COTETION = "'";
    protected StringBuilder sql;

    private <P> SQLMaker addCondition(P p, String operator) {
        sql.append(operator);
        if (p instanceof Integer
                || p instanceof Double
                || p instanceof Long
                || p instanceof Float
                || p instanceof BigDecimal
                || p instanceof BigInteger) {
            sql.append(p);
        } else {
            sql.append(SINGLE_COTETION);
            sql.append(p.toString());
            sql.append(SINGLE_COTETION);
        }
        return this;
    }

    protected char lastChar() {
        return sql.charAt(sql.length() - 1);
    }

    public String make() {
        return sql.toString();
    }

    protected SQLMaker cols(String... cols) {
        for (int i = 0; i < cols.length; i++) {
            if (i > 0) {
                sql.append(COMA);
            }
            sql.append(cols[i]);
        }
        return this;
    }

    public SQLMaker col(String col) {
        if (lastChar() != ',') {
            sql.append(COMA);
        }
        sql.append(col);
        return this;
    }

    public SQLMaker from(String... tables) {
        sql.append(FROM);
        for (int i = 0; i < tables.length; i++) {
            if (i > 0) {
                sql.append(COMA);
            }
            sql.append(tables[i]);
        }
        return this;
    }

    public SQLMaker star() {
        if (lastChar() == '.') {
            return addCondition(SPACE, STAR.trim());
        }
        return addCondition("", STAR);
    }

    public SQLMaker where(String param) {
        return addCondition(param, WHERE);
    }

    public SQLMaker and(String param) {
        return addCondition(param, AND);
    }

    public SQLMaker or(String param) {
        return addCondition(param, OR);
    }

    public SQLMaker like(String param) {
        return addCondition(param, LIKE);
    }

    public SQLMaker in(String param) {
        return addCondition(param, IN);
    }

    public SQLMaker notIn(String param) {
        return addCondition(param, NOT_IN);
    }

    /**
     * This method create the equal condition. if p is Not a Number then the
     * toString method is used. For this, the custom class type must override or
     * define a toString method to represent the string value.
     *
     * @param <P>
     * @param param
     * @return
     */
    public <P> SQLMaker eq(P param) {
        return addCondition(param, EQ);
    }

    /**
     * This method create the not equal condition. if p is Not a Number then the
     * toString method is used. For this, the custom class type must override or
     * define a toString method to represent the string value.
     *
     * @param param
     * @return
     */
    public <P> SQLMaker notEq(P param) {
        return addCondition(param, NOT_EQ);
    }

    public SQLMaker gt(String param) {
        return addCondition(param, GT);
    }

    public SQLMaker lt(String param) {
        return addCondition(param, LT);
    }

    public SQLMaker gtOReq(String param) {
        return addCondition(param, GT_EQ);
    }

    public SQLMaker ltOReq(String param) {
        return addCondition(param, LT_EQ);
    }

    public SQLMaker join(String table) {
        return addCondition(table, JOIN);
    }

    public SQLMaker innerJoin(String table) {
        return addCondition(table, INNER_JOIN);
    }

    public SQLMaker leftJoin(String table) {
        return addCondition(table, LEFT_JOIN);
    }

    public SQLMaker rightJoin(String table) {
        return addCondition(table, RIGHT_JOIN);
    }

    public SQLMaker outerJoin(String table) {
        return addCondition(table, OUTER_JOIN);
    }
    
    public SQLMaker select(String... cols) {
        sql = new StringBuilder(SELECT);
        cols(cols);
        return this;
    }

    public SQLMaker insert(String table, String... cols) {
        if (table == null || table.trim().isEmpty()) {
            throw new IllegalArgumentException("Invalid table name.");
        }
        sql = new StringBuilder(INSERT).append(INTO).append(table);
        if (cols.length > 0) {
            sql.append(F_BRACE_START);
            cols(cols);
        }
        return this;
    }

    /**
     * This method can be used for making insert query with PreparedStatement.
     * Question mark is inserted in values parentesis.
     *
     * @param length
     * @return
     */
    public SQLMaker values(int length) {
        if (length <= 0) {
            throw new IllegalArgumentException("Parameter length must be greater than 0.");
        }
        if (lastChar() != ')' && lastChar() != ' ') {
            sql.append(F_BRACE_END);
        } else if (lastChar() == ' ' && sql.charAt(sql.length() - 2) != ')') {
            sql.append(F_BRACE_END);
        }
        sql.append(VALUES).append(F_BRACE_START);
        for (int i = 0; i < length; i++) {
            if (i > 0) {
                sql.append(COMA);
            }
            sql.append(QUE);
        }
        sql.append(F_BRACE_END);
        return this;
    }

    /**
     * This method is suitable for statement while creating insert query.
     *
     * @param <V>
     * @param values
     * @return
     */
    public <V> SQLMaker values(V... values) {
        if (values.length <= 0) {
            throw new IllegalArgumentException("Parameter length must be greater than 0.");
        }
        if (lastChar() != ')' && lastChar() != ' ') {
            sql.append(F_BRACE_END);
        } else if (lastChar() == ' ' && sql.charAt(sql.length() - 2) != ')') {
            sql.append(F_BRACE_END);
        }
        sql.append(VALUES).append(F_BRACE_START);
        for (int i = 0; i < values.length; i++) {
            if (i > 0) {
                sql.append(COMA);
            }
            V v = values[i];
            if (v instanceof Integer
                    || v instanceof Double
                    || v instanceof Long
                    || v instanceof Float
                    || v instanceof BigDecimal
                    || v instanceof BigInteger) {
                sql.append(values[i]);
            } else {
                sql.append(SINGLE_COTETION);
                sql.append(v.toString());
                sql.append(SINGLE_COTETION);
            }
        }
        sql.append(F_BRACE_END);
        return this;
    }

    public SQLMaker delete() {
        sql = new StringBuilder(DELETE);
        return this;
    }
    public SQLMaker update() {
        sql = new StringBuilder(UPDATE);
        return this;
    }
}
