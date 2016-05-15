/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package soayjoni.jdbcutil.util;

/**
 *
 * @author mashuk
 */
public interface DBUtil {

    java.sql.Connection getConnection();

    boolean openDB() throws java.sql.SQLException;

    void closeDB();
}
