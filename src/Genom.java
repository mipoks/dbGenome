import java.io.*;
import java.sql.*;

public class Genom {

    private static final String JDBC_DRIVER = "org.postgresql.Driver";
    private static final String DATABASE_URL = "jdbc:postgresql://localhost:5432/postgres";
    private static final String DATABASE_USER = "postgres";
    private static final String DATABASE_PASSWORD = "postgres";

    public static Connection getConnection() throws SQLException {
        try {
            Class.forName(JDBC_DRIVER);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return DriverManager.getConnection(DATABASE_URL, DATABASE_USER, DATABASE_PASSWORD);
    }

    private String tableName;
    private String fileName;
    public Genom(String tableName, String fileName) {
        this.fileName = fileName;
        this.tableName = tableName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    private void init(Connection connection) {
        String tables = "create table " + tableName + " (splited INTEGER );";
        execute(tables, connection);
    }

    private int execute(String sql, Connection connection) {
        try {
            PreparedStatement pstm = connection.prepareStatement(sql);
            return pstm.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return 0;
    }

    private int writeToBd(String str, Connection connection) {
        String sql = "INSERT INTO " + tableName + " VALUES ('" + str.hashCode() + "');";
        return execute(sql, connection);
    }

    public void run(int countSplit) {
        long startTime = System.currentTimeMillis();
        try {
            Connection connection = getConnection();
            init(connection);
            try (DataInputStream dataInputStream = new DataInputStream(new FileInputStream(fileName))) {
                connection.setAutoCommit(false);
                String temp;
                char c;
                byte t;
                byte[] bytes = new byte[countSplit];
                dataInputStream.read(bytes);
                temp = new String(bytes);
                writeToBd(temp, connection);
                while ((t = dataInputStream.readByte()) != -1) {
                    temp = temp.substring(1) + (char) t;
                    writeToBd(temp, connection);
                }
                dataInputStream.close();
            } catch (IOException | SQLException e) {
                e.printStackTrace();
            }
            connection.commit();
            connection.setAutoCommit(true);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        long timeSpent = System.currentTimeMillis() - startTime;
        System.out.println("Successfully added to " + tableName + ". Time passed " + timeSpent + " millisec");
    }

    public float getAnswer(String tableName1, String tableName2) {
        String sql = "SELECT (" +
                "(SELECT count(*) from " +
                "(SELECT splited " +
                        "FROM " + tableName1 +
                        " INTERSECT SELECT splited " +
                        "FROM " + tableName2 + " ) x) :: FLOAT" +
                " / " +
                "(SELECT count(*) from " +
                "( SELECT splited " +
                        "FROM " + tableName1 +
                        " UNION SELECT splited FROM " + tableName2 + " ) y " +
               ") :: FLOAT " +
           ");";
        try {
            Connection connection = getConnection();
            PreparedStatement pstm = connection.prepareStatement(sql);
            ResultSet resultSet = pstm.executeQuery();
            if (resultSet.next()) {
                return resultSet.getFloat(1);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return -1;
    }
}
