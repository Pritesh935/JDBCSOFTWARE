package com.nit.pc.javasql;

import java.sql.*;
import java.util.Properties;
import java.io.*;
import java.util.Scanner;

public class JavaSqlPlus1 {
    
    public static void main(String[] args) throws FileNotFoundException, IOException, ClassNotFoundException {
        System.out.println("************ Welcome to Java SQLPlus Software ********** ");

        // Redirecting STDERR to a file
        try {
            System.setErr(new PrintStream(new FileOutputStream("error.log")));
        } catch (IOException e) {
            e.printStackTrace();
        }

        Scanner scn = new Scanner(System.in);
        Connection con = null;

        try {
            // Loading properties from properties file
            Properties props = new Properties();
            props.load(new FileReader("driverinfo.properties"));

            // Reading driver properties from the Properties object
            final String DRIVER = props.getProperty("DRIVER");
            final String DB_URL = props.getProperty("DB_URL");

            // Loading JDBC driver
            Class.forName(DRIVER);

            // Reading DB schema user-name and password 
            String DB_USN = "";
            String DB_PWD = "";
            
            while (true) {
                System.out.print("\nEnter user-name: ");
                String username = scn.nextLine();
                String[] usnpwdArray = username.split("/");

                if (usnpwdArray.length == 1) {
                    DB_USN = usnpwdArray[0];

                    System.out.print("Enter password: ");
                    DB_PWD = scn.next();
                    scn.nextLine();

                } else if (usnpwdArray.length == 2) {
                    DB_USN = usnpwdArray[0];
                    DB_PWD = usnpwdArray[1];

                } else {
                    System.out.println("Error: Invalid option, Enter only username/password ");
                    continue;
                }

                try {
                    // Establishing connection
                    con = DriverManager.getConnection(DB_URL, DB_USN, DB_PWD);
                    System.out.println("\nConnected to the DB Schema " + DB_USN);
                    con.setAutoCommit(false);
                } catch (SQLException e) {
                    System.out.println("\nError: Invalid username/password");
                    continue;
                }
                break;
            }

            System.out.println("\n**** Type any SQL Query ends with ; and press Enter to run");
            while (true) {
                try {
                    System.out.print("\nJava SQLPlus> ");
                    String query = scn.nextLine();
                    if (query.toLowerCase().contains("exit")) {
                        System.out.println("\nThank You, Visit Again");
                        break;
                    }

                    // Removing ; from the query if present
                    if (query.endsWith(";")) {
                        query = query.substring(0, query.length() - 1);
                    }

                    // Execute the query in PL/SQL or as an SQL statement
                    if (query.toUpperCase().contains("SELECT")) {
                        executeSelectQuery(con, query);
                    } else {
                        executeDMLQuery(con, query);
                    }
                } catch (SQLException e) {
                    System.out.println("SQL Error: " + e.getMessage());
                    e.printStackTrace();
                }
            }
        }// catch (ClassNotFoundException | IOException | SQLException e) {
         //   e.printStackTrace();
        //}
        finally {
            try {
                if (con != null) {
                    con.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    private static void executeSelectQuery(Connection con, String query) throws SQLException {
        Statement stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery(query);
        ResultSetMetaData rsmd = rs.getMetaData();
        int columnCount = rsmd.getColumnCount();

        // Displaying column names
        for (int i = 1; i <= columnCount; i++) {
            System.out.print(rsmd.getColumnName(i) + "\t");
        }
        System.out.println("\n----------------------------------------------");

        // Displaying rows
        while (rs.next()) {
            for (int i = 1; i <= columnCount; i++) {
                System.out.print(rs.getString(i) + "\t");
            }
            System.out.println();
        }

        rs.close();
        stmt.close();
    }

    private static void executeDMLQuery(Connection con, String query) throws SQLException {
        CallableStatement cs = con.prepareCall("{call execute_sql_query(?)}");
        cs.setString(1, query);
        cs.execute();
        con.commit();

        System.out.println("Query executed successfully.");
        cs.close();
    }
}
