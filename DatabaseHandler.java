/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package library.database;


import java.sql.DriverManager; // Gestion des pilotes pour la base de données 
import java.sql.Connection; // créer la connexion avec la BD
import java.sql.ResultSet; // Pour Collecter les données
import java.sql.SQLException; // Gérer les Erreurs
import java.sql.Statement; // Gérer les Modfications
import java.sql.DatabaseMetaData; // Gestion generale de la BD
import javax.swing.JOptionPane; // fenetres pour afficher les messages


/**
 *
 * @author you1-
 */
public final class DatabaseHandler {
    
    
    
    private static DatabaseHandler handler;

    private static final String DB_URL = "jdbc:derby:database;create=true";
    private static Connection conn = null;

    private static Statement stmt = null;

    public DatabaseHandler() {
        createConnection();
        setupProjectTable();
        setupPHTable();   
    }

    @SuppressWarnings("deprecation")
    private static void createConnection() {
        try {
            Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
            conn = DriverManager.getConnection(DB_URL);
        }
        catch (ClassNotFoundException | SQLException e) {
            JOptionPane.showMessageDialog(null, "Cant load database", "Database"
                    + " Error", JOptionPane.ERROR_MESSAGE);
            System.exit(0);
        }
    }
    void setupProjectTable() {
        String TABLE_NAME = "PROJECTS";
        try {
            stmt = conn.createStatement();
            DatabaseMetaData dbm = conn.getMetaData();
            ResultSet tables = dbm.getTables(null, null,
                    TABLE_NAME.toUpperCase(), null);
            if (tables.next()) {
                System.out.println("Table " + TABLE_NAME + " already exists. "
                        + "Ready for go!");
            } else {
                stmt.execute("CREATE TABLE " + TABLE_NAME + "("
                        + "      id INT,\n"
                        + "      name varchar(200),\n"
                        + "      type varchar(200),\n"
                        + "      numShifts INT,\n"
                        + "      hoursPerMonth DOUBLE,\n"
                        + "      layUpRatio DOUBLE,\n"
                        + "      Linelength INT,\n"
                        + "      Line_Config varchar(200),\n"
                        + "      Utilized_sides INT,\n"
                        + "      charge DOUBLE,\n"
                        + "      capacity DOUBLE,\n"
                        + "      numOfVerticalLines DOUBLE,\n"
                        + "      supportLength DOUBLE,\n"
                        + "      NumFinTab DOUBLE,\n"
                        + "      numPackStat DOUBLE,\n"
                        + "      numKitRack DOUBLE,\n"
                        + "      numWRack DOUBLE,\n"
                        + "      numKanRack DOUBLE,\n"
                        + "      numMecTable DOUBLE,\n"
                        + "      surSup2m DOUBLE,\n"
                        + "      surFinTab DOUBLE,\n"
                        + "      surPackStat DOUBLE,\n"
                        + "      surKitRack DOUBLE,\n"
                        + "      surWRack DOUBLE,\n"
                        + "      surKanRack DOUBLE,\n"
                        + "      surAsseyBH DOUBLE,\n"
                        + "      surMecTable DOUBLE,\n"
                        + "      month DATE,\n"
                        + "      Productivity DOUBLE,\n"
                        + "      Backlog DOUBLE,\n"
                        + "      numOfSupp_R DOUBLE,\n"
                        + "      NumFinTab_R DOUBLE,\n"
                        + "      numPackStat_R DOUBLE,\n"
                        + "      numKitRack_R DOUBLE,\n"
                        + "      numWRack_R DOUBLE,\n"
                        + "      numKanRack_R DOUBLE,\n"
                        + "      numMecTable_R DOUBLE,\n"
                        + "      numAsseyBH_R DOUBLE,\n"
                        + "      surSup2m_R DOUBLE,\n"
                        + "      surFinTab_R DOUBLE,\n"
                        + "      surPackStat_R DOUBLE,\n"
                        + "      surKitRack_R DOUBLE,\n"
                        + "      surWRack_R DOUBLE,\n"
                        + "      surKanRack_R DOUBLE,\n"
                        + "      surAsseyBH_R DOUBLE,\n"
                        + "      surMecTable_R DOUBLE,\n"
                        + "      total_surface_needed DOUBLE"
                        + " )");
                System.out.println("Table"+TABLE_NAME+" created");
            }
            
            
            
        } catch (SQLException e) {
           JOptionPane.showMessageDialog(null, "Error:" + e.getMessage(), 
                   "Error Occured", JOptionPane.ERROR_MESSAGE);
            System.out.println("Exception at execQuery:dataHandler" +
                    e.getLocalizedMessage());
        } finally {
        }
    }
//    void setupProjPivotT(){
//             String TABLE_NAME = "pivot_table";
//        try {
//            stmt = conn.createStatement();
//            DatabaseMetaData dbm = conn.getMetaData();
//            ResultSet tables = dbm.getTables(null, null, TABLE_NAME.toUpperCase(), null);
//            if (tables.next()) {
//                System.out.println("Table " + TABLE_NAME + " already exists. Ready for go!");
//            } else {
//                stmt.execute( "SELECT * FROM   \n"
//               + "(\n"
//               + "    SELECT \n"
//               + "        month, \n"
//               + "        total_surface_needed,\n"
//               + "        name\n"
//               + "    FROM \n"
//               + "        PROJECTS\n"
//               + ") t \n"
//               + "pivot(\n"
//               + "    total_surface_needed\n"
//               + "    FOR month IN (\n"
//               + "        [2022-01-01]) \n"
//               + ") AS pivot_table;");
//                System.out.println("pivot_table");
//         }
//        } catch (SQLException e) {
//            System.err.println(e.getMessage() + " --- setupDatabase");
//        } finally {
//        }
//    }
    void setupPHTable(){
         String TABLE_NAME = "PH";
        try {
            stmt = conn.createStatement();
            DatabaseMetaData dbm = conn.getMetaData();
            ResultSet tables = dbm.getTables(null, null, TABLE_NAME.toUpperCase
        (), null);
            if (tables.next()) {
                System.out.println("Table " + TABLE_NAME + " already exists."
                        + " Ready for go!");
            } else {
                stmt.execute("CREATE TABLE " + TABLE_NAME + "("
                        + "      P_id INT ,\n"
                        + "      PH_id varchar(200),\n"
                        + "      zone varchar(200),\n"
                        + "      phts INT,\n"
                        + "      At_H DOUBLE,\n"
                        + "      numOp INT,\n"
                        + "      numShift INT,\n"
                        + "      lengthPH DOUBLE,\n"
                        + "      numBoard DOUBLE"
                        + " )");
                System.out.println("PH Table created");
         }
        } catch (SQLException e) {
            System.err.println(e.getMessage() + " --- setupDatabase");
        } finally {
        }
    }
    void dropTable(String T){
        try{
         Statement stmt = conn.createStatement();
      		      
         String sql = "DROP TABLE "+T;
         stmt.executeUpdate(sql);
         System.out.println("Table deleted in given database...");   	  
      } catch (SQLException e) {
         e.printStackTrace();
    }
    }

    public ResultSet execQuery(String query) {
        ResultSet result;
        try {
            stmt = conn.createStatement();
            result = stmt.executeQuery(query);
        } catch (SQLException ex) {
            System.out.println("Exception at execQuery:dataHandler" + 
                    ex.getLocalizedMessage());
            return null;
        } finally {
        }
        return result;
    }

    public boolean execAction(String qu) {
        try {
            stmt = conn.createStatement();
            stmt.execute(qu);
            return true;
        } catch (SQLException ex) {
            JOptionPane.showMessageDialog(null, "Error:" + ex.getMessage(), 
                    "Error Occured", JOptionPane.ERROR_MESSAGE);
            System.out.println("Exception at execQuery:dataHandler" + 
                    ex.getLocalizedMessage());
            return false;
        } finally {
        }
    
    }
    public static DatabaseHandler getInstance() {
        if (handler == null) {
            handler = new DatabaseHandler();
        }
        return handler;
    }
    public static void main(String[] args) throws Exception {
        DatabaseHandler.getInstance();
    }
    public Connection getConnection() {
        return conn;
    }
    public static void shutdownDB()
    {
        try
        {
            if (stmt != null)
            {
                stmt.close();
            }
            if (conn != null)
            {
                DriverManager.getConnection(DB_URL + ";shutdown=true");
                conn.close();
            }           
        }
        catch (SQLException sqlExcept)
        {
            
        }

    }
}

