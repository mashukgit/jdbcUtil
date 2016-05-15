/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package soayjoni.jdbcutil.util;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author mashuk
 */
public abstract class AbstractDBUtil implements DBUtil {

    protected final String dbUrl;

    protected java.sql.Connection con;
    

    protected int connectionTryCounter;

    public AbstractDBUtil(String dbUrl) {
        this.dbUrl = dbUrl;
    }

    @Override
    public java.sql.Connection getConnection() {
        return con;
    }

    @Override
    public void closeDB() {
        try {
            if (con != null || !con.isClosed()) {
                con.close();
                con = null;
            }
        } catch (java.sql.SQLException ex) {
            Logger.getLogger(AbstractDBUtil.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
