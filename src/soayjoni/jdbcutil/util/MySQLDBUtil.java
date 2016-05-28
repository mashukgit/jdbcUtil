/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package soayjoni.jdbcutil.util;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author mashuk
 */
public class MySQLDBUtil extends AbstractDBUtil {

    public MySQLDBUtil(String dbUrl) {
        super(dbUrl);
    }

    @Override
    public boolean openDB() throws SQLException {
        if (connectionTryCounter >= 60) {
            return hasConnected();
        }
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(MySQLDBUtil.class.getName()).log(Level.SEVERE, null, ex);
            con = null;
        }
        con = DriverManager.getConnection(dbUrl);
        if (hasConnected()) {
            return true;
        }
        return tryAgain();
    }
}
