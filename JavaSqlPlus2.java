package com.nit.pc.javasql;

import java.util.Properties;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.SQLException;

import java.util.Scanner;

public class JavaSqlPlus2 {
    
    public static void main(String[] args) {
        System.out.println("************ Welcome to Java SQLPlus Software ********** ");

        // Redirecting STDERR to a file
        try {
            System.setErr(new PrintStream(new FileOutputStream("error.log")));
        } catch (IOException e) {
            e.printStackTrace();
        }

        Scanner scn = new Scanner(System.in);
        Connection con = null;
        Statement stmt = null;

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
                    int count = 2;

                    // Reading query from the console (collecting all parts until ;)
                    String query = scn.nextLine();
                    if (query.toLowerCase().contains("exit")) {
                        System.out.println("\nThank You, Visit Again");
                        break;
                    }

                    while (true) {

                        if (!query.endsWith(";")) {
                            System.out.print(" " + count++ + " ");
                            query = query + "\n" + scn.nextLine();
                            continue;
                        }

                        break;
                    }

                    // Removing ; from the query
                    query = query.substring(0, query.length() - 1);

                    // The user-entered query may be a DDL, DCL, DML, DQL, TCL
                    // hence we must run the query by using stmt.execute(-) method
                    boolean resultSet = stmt.execute(query);

                    // stmt.execute(-) method may return ResultSet or updatedCount
                    // if it returns true, the returned result is RS
                    // if it returns false, the returned result is UC
                    // for retrieving RS, we must call stmt.getResultSet() method
                    // for retrieving UC, we must call stmt.getUpdateCount() method
                    // as shown in the below code

                    if (resultSet) {
                        try (ResultSet rs = stmt.getResultSet()) {
                            ResultSetMetaData rsmd = rs.getMetaData();

                            // Fetching Columns and Rows RSMD and RS
                            count = 0;
                            if (rs.next()) {
                                int columnCount = rsmd.getColumnCount();
                                for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                                    System.out.print(rsmd.getColumnName(i) + "\t");
                                }
                                System.out.println("\n-----------------------------------------------");

                                do {
                                    for (int i = 1; i <= columnCount; i++) {
                                        System.out.print(rs.getString(i) + "\t");
                                    }
                                    System.out.println();

                                    count++;
                                } while (rs.next());
                            }

                            System.out.println("\n" + count + " rows selected");

                        } catch (SQLException e) {
                            e.printStackTrace();
                        }

                    } else {
                        int updatedCount = stmt.getUpdateCount();
                        System.out.println();

                        query = query.toUpperCase();

                        // Use switch-case to handle different SQL query types
                        String operationType = null;

                        if (query.contains("INSERT")) {
                            operationType = "INSERT";
                        } else if (query.contains("UPDATE")) {
                            operationType = "UPDATE";
                        } else if (query.contains("DELETE")) {
                            operationType = "DELETE";
                        } else if (query.contains("COMMIT")) {
                            operationType = "COMMIT";
                        } else if (query.contains("ROLLBACK")) {
                            operationType = "ROLLBACK";
                        } else if (query.contains("CREATE TABLE")) {
                            operationType = "CREATE TABLE";
                        } else if (query.contains("ALTER TABLE")) {
                            operationType = "ALTER TABLE";
                        } else if (query.contains("DROP TABLE")) {
                            operationType = "DROP TABLE";
                        } else {
                            operationType = "OTHER";
                        }

                        switch (operationType) {
                            case "INSERT":
                                System.out.println("1 row inserted");
                                break;
                            case "UPDATE":
                                if (updatedCount == 1)
                                    System.out.println(updatedCount + " row updated");
                                else
                                    System.out.println(updatedCount + " rows updated");
                                break;
                            case "DELETE":
                                if (updatedCount == 1)
                                    System.out.println(updatedCount + " row deleted");
                                else
                                    System.out.println(updatedCount + " rows deleted");
                                break;
                            case "COMMIT":
                                System.out.println("Commit complete.");
                                break;
                            case "ROLLBACK":
                                System.out.println("Rollback complete.");
                                break;
                            case "CREATE TABLE":
                                System.out.println("Table created.");
                                break;
                            case "ALTER TABLE":
                                System.out.println("Table altered.");
                                break;
                            case "DROP TABLE":
                                System.out.println("Table dropped.");
                                break;
                            default:
                                // Handle other queries or print update count
                                System.out.println(updatedCount);
                                break;
                        }
                    }
                } catch (SQLException e) {

                    // Printing only exception error message on console
                    System.out.println(e.getMessage());

                    // Complete Exception information is stored in the error.log file 
                    // because of STDERR redirection at the beginning of the program
                    e.printStackTrace();

                }
            }

        } catch (ClassNotFoundException e) {
            System.out.println("Driver class is not found");

        } catch (IOException e) {
            e.printStackTrace();
            System.out.println(e);

        } finally {
            try {
                if (stmt != null)
                    stmt.close();
            } catch (SQLException e) {
            }

            try {
                if (con != null)
                    con.close();
            } catch (SQLException e) {
            }
        }
    }
}
