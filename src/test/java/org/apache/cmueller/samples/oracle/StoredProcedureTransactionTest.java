package org.apache.cmueller.samples.oracle;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;

import static junit.framework.TestCase.assertEquals;

public class StoredProcedureTransactionTest {

    private Connection con;

    @Before
    public void setUp() throws SQLException {
        DriverManager.registerDriver (new oracle.jdbc.OracleDriver());

        con = DriverManager.getConnection("jdbc:oracle:thin:cmueller/password@//localhost:1521/orcl");
        con.setAutoCommit(false);

        try(PreparedStatement statement = con.prepareStatement("CREATE TABLE TEST (NAME VARCHAR2(100))")) {
            statement.execute();
        } catch (SQLException sqle) {
            if (955 == sqle.getErrorCode()) {
                // Table aready exists
            }else {
                System.out.println(sqle);
                throw sqle;
            }
        }

        try(PreparedStatement statement = con.prepareStatement("DELETE FROM TEST")) {
            statement.execute();
        }

        try(PreparedStatement statement = con.prepareStatement(
                "CREATE OR REPLACE PROCEDURE ADD_TEST_ENTRY (NAME IN VARCHAR2) AS " +
                        "BEGIN " +
                        "INSERT INTO TEST VALUES (ADD_TEST_ENTRY.NAME); " +
                        "END ADD_TEST_ENTRY;")) {

            statement.execute();
        } catch (SQLException sqle) {
            if (955 == sqle.getErrorCode()) {
                // Stored procedure aready exists
            } else {
                System.out.println(sqle);
                throw sqle;
            }
        }

        con.commit();
    }

    @After
    public void tearDown() throws SQLException {
        if (con != null) {
            con.close();
        }
    }

    @Test
    public void testCommit() throws SQLException {
        assertEquals(0, getTestTableRowCount());

        executeStoredProcedure();
        con.commit();

        assertEquals(1, getTestTableRowCount());
    }

    @Test
    public void testRollback() throws SQLException {
        assertEquals(0, getTestTableRowCount());

        executeStoredProcedure();
        con.rollback();

        assertEquals(0, getTestTableRowCount());
    }

    private void executeStoredProcedure() throws SQLException {
        try (CallableStatement statement = con.prepareCall("{call ADD_TEST_ENTRY(?)}")) {
            statement.setString(1, "CMUELLER");
            statement.execute();
        }
    }

    private int getTestTableRowCount() throws SQLException {
        try (PreparedStatement statement = con.prepareStatement("SELECT COUNT(*) FROM TEST")) {
            statement.executeUpdate();

            try (ResultSet rs = statement.getResultSet()) {
                rs.next();
                return rs.getInt(1);
            }
        }
    }
}