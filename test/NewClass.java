
import java.sql.SQLException;
import soayjoni.jdbcutil.util.DBOperation;
import soayjoni.jdbcutil.util.DBType;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
/**
 *
 * @author mashuk
 */
public class NewClass extends DBOperation {

    public static void main(String[] args) throws SQLException {
        NewClass dbo = new NewClass("jdbc:mysql://localhost/test?user=root&password=samiha", DBType.MY_SQL);
        boolean openDB = dbo.openDB();
        System.out.println("openDB = " + openDB);
        if (openDB) {
            try {
                dbo.sql = "select * from Object";
                dbo.ps = dbo.con.prepareStatement(dbo.sql);
                dbo.rs = dbo.ps.executeQuery();
                dbo.rsmd = dbo.rs.getMetaData();
                int columnCount = dbo.rsmd.getColumnCount();
                System.out.println("columnCount = " + columnCount);
                for (int i = 1; i <= columnCount; i++) {
                    int columnType = dbo.rsmd.getColumnType(i);
                    System.out.println("columnType = " + columnType);
                    String columnTypeName = dbo.rsmd.getColumnTypeName(i);
                    System.out.println("columnTypeName = " + columnTypeName);
                    String columnClassName = dbo.rsmd.getColumnClassName(i);
                    System.out.println("columnClassName = " + columnClassName);
                    int columnDisplaySize = dbo.rsmd.getColumnDisplaySize(i);
                    System.out.println("columnDisplaySize = " + columnDisplaySize);
                    int precision = dbo.rsmd.getPrecision(i);
                    System.out.println("precision = " + precision);
                    System.out.println();
                }
            } finally {
                dbo.closeDB();
            }
        }
    }

    public NewClass(String dbUrl, DBType dBType) {
        super(dbUrl, dBType);
    }
}
