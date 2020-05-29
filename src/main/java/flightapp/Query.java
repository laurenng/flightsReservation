package flightapp;

import java.io.*;
import java.sql.*;
import java.util.*;
import java.security.*;
import java.security.spec.*;
import javax.crypto.*;
import javax.crypto.spec.*;
import javax.xml.transform.Result;

/**
 * Runs queries against a back-end database
 */
public class Query {
  // DB Connection
  private Connection conn;

  // Password hashing parameter constants
  private static final int HASH_STRENGTH = 65536;
  private static final int KEY_LENGTH = 128;

  // Canned queries
  private static final String CHECK_FLIGHT_CAPACITY = "SELECT capacity FROM Flights WHERE fid = ?";
  private PreparedStatement checkFlightCapacityStatement;

  // For check dangling
  private static final String TRANCOUNT_SQL = "SELECT @@TRANCOUNT AS tran_count";
  private PreparedStatement tranCountStatement;


  // TODO: YOUR CODE HERE
  public static String getUserIdFromUser =  "SELECT Userid FROM Users WHERE Userid = ?";
  private PreparedStatement userIdFromUser;

  public static String getUserMoney = "SELECT balance FROM Users WHERE Userid = ?";
  private PreparedStatement userMoney;

  public static String updateUserMoney = "UPDATE Users SET balance = ? WHERE Userid = ?";
  private PreparedStatement updateMoney;



  public static String insertNewUser = "INSERT INTO Users VALUES (?, ?, ?, ?)";
  private PreparedStatement insertNewUserStatement;

  public static String getSingleUserPassword = "SELECT salt, password FROM Users WHERE Userid=?";
  private PreparedStatement singleUserPassword;
  // Search function strings


  // Keep track of users logged in or not
  public boolean userLoggedIn = false; // start with no users
  public String currentUser = null; // what user is logged in
  
  // Keeping track of the SearchID 
  public String searchID = null;

  public static String searchDirectFlight = "SELECT TOP(?) " +
          "fid, day_of_month, carrier_id, flight_num, origin_city, dest_city, actual_time, capacity, price " +
          "FROM Flights " +
          "WHERE origin_city =? " +
          "AND dest_city=? " +
          "AND day_of_month = ? " +
          "AND canceled = 0 " +
          "ORDER BY actual_time, fid ASC";
  private PreparedStatement directFlightInfo;

  public static String searchNonDirectFlight = "SELECT TOP(?) F1.fid AS F1_fid, F2.fid AS F2_fid, " +
          "F1.day_of_month, " +
          "F1.carrier_id AS F1carrier, F1.flight_num AS F1Flightnum, F1.origin_city AS F1origin_city, " +
          "F1.dest_city AS F1Dest, F1.actual_time AS F1duration, F1.capacity AS F1capacity, F1.price AS F1price, " +
          "F2.carrier_id AS F2carrier, F2.flight_num AS F2Flightnum, F2.origin_city AS F2origin_city, " +
          "F2.dest_city AS F2Dest, F2.actual_time AS F2duration, F2.capacity AS F2capacity, F2.price AS F2price, " +
          "(F1.actual_time + F2.actual_time) AS totalTime " +
          "FROM Flights AS F1, Flights AS F2 " +
          "WHERE F1.origin_city = ? " +
          "AND F1.dest_city = F2.origin_city " +
          "AND F2.dest_city = ? " +
          "AND F1.day_of_month = ? " +
          "AND F2.day_of_month=F1.day_of_month " +
          "AND F2.actual_time > 0 " +
          "AND F1.actual_time > 0 " +
          "AND F1.canceled = 0 " +
          "AND F2.canceled = 0 " +
          "ORDER BY (F1.actual_time + F2.actual_time), F1.fid, F2.fid ASC";
  private PreparedStatement nonDirectFlightInfo;

  public static String getFidInfo =  "SELECT carrier_id, flight_num, origin_city, dest_city, capacity, price, day_of_month, actual_time FROM Flights WHERE fid =?";
  private PreparedStatement fidInfo;


  public static String getItinerary = "SELECT * FROM Itineraries WHERE id = ? AND searchID = ?";
  private PreparedStatement single_itinerary;

  public static String insertItin = "INSERT INTO Itineraries VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
  private PreparedStatement insertIntoItin;


  public static String getReservationCount = "SELECT COUNT(*) AS count FROM Reservations";
  private PreparedStatement reservationCount;

  public static String getReservation = "SELECT * FROM Reservations WHERE rid = ? AND username = ?";
  private PreparedStatement reservation;

  public static String getReservationAll = "SELECT * FROM Reservations WHERE username = ? ORDER BY rid ASC";
  private PreparedStatement reservationAll;

  public static String getInsertIntoRes = "INSERT INTO Reservations VALUES (?, ?, ?, ?, ?, ?, ?)";
  private PreparedStatement InsertIntoRes;

  public static String serialize = "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE; BEGIN TRANSACTION;";
  private PreparedStatement serializeTransaction;

  public static String repeatSerialize = "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ; BEGIN TRANSACTION;";
  private PreparedStatement repeatTransaction;

  public Query() throws SQLException, IOException {
    this(null, null, null, null);
  }

  protected Query(String serverURL, String dbName, String adminName, String password)
          throws SQLException, IOException {
    conn = serverURL == null ? openConnectionFromDbConn()
            : openConnectionFromCredential(serverURL, dbName, adminName, password);

    prepareStatements();
  }

  /**
   * Return a connecion by using dbconn.properties file
   *
   * @throws SQLException
   * @throws IOException
   */
  public static Connection openConnectionFromDbConn() throws SQLException, IOException {
    // Connect to the database with the provided connection configuration
    Properties configProps = new Properties();
    configProps.load(new FileInputStream("dbconn.properties"));
    String serverURL = configProps.getProperty("flightapp.server_url");
    String dbName = configProps.getProperty("flightapp.database_name");
    String adminName = configProps.getProperty("flightapp.username");
    String password = configProps.getProperty("flightapp.password");
    return openConnectionFromCredential(serverURL, dbName, adminName, password);
  }

  /**
   * Return a connecion by using the provided parameter.
   *
   * @param serverURL example: example.database.widows.net
   * @param dbName    database name
   * @param adminName username to login server
   * @param password  password to login server
   *
   * @throws SQLException
   */
  protected static Connection openConnectionFromCredential(String serverURL, String dbName,
                                                           String adminName, String password) throws SQLException {
    String connectionUrl =
            String.format("jdbc:sqlserver://%s:1433;databaseName=%s;user=%s;password=%s", serverURL,
                    dbName, adminName, password);
    Connection conn = DriverManager.getConnection(connectionUrl);

    // By default, automatically commit after each statement
    conn.setAutoCommit(true);

    // By default, set the transaction isolation level to serializable
    conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);

    return conn;
  }

  /**
   * Get underlying connection
   */
  public Connection getConnection() {
    return conn;
  }

  /**
   * Closes the application-to-database connection
   */
  public void closeConnection() throws SQLException {
    conn.close();
  }

  /**
   * Clear the data in any custom tables created.
   *
   * WARNING! Do not drop any tables and do not clear the flights table.
   */
  public void clearTables() {
    try {
      String clearIt = "DELETE FROM Itineraries";
      PreparedStatement itins = conn.prepareStatement(clearIt);
      itins.executeUpdate();

      String clearRes = "DELETE FROM Reservations";
      PreparedStatement res = conn.prepareStatement(clearRes);
      res.executeUpdate();

      String clearUsers = "DELETE FROM Users";
      PreparedStatement usersAreCleared = conn.prepareStatement(clearUsers);
      usersAreCleared.executeUpdate();

    } catch (Exception e) {
      e.printStackTrace();
    }
  }


  public void clearItinerariesTable() {
    try {
      String clearSQL = "DELETE FROM Itineraries";
      PreparedStatement ps2 = conn.prepareStatement(clearSQL);
      //ps2.clearParameters();
      //ps2.setString(1, table);
      ps2.executeUpdate();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /*
   * prepare all the SQL statements in this method.
   */
  private void prepareStatements() throws SQLException {
    checkFlightCapacityStatement = conn.prepareStatement(CHECK_FLIGHT_CAPACITY);

    tranCountStatement = conn.prepareStatement(TRANCOUNT_SQL);

    insertNewUserStatement = conn.prepareStatement(insertNewUser);

    singleUserPassword = conn.prepareStatement(getSingleUserPassword);

    directFlightInfo = conn.prepareStatement(searchDirectFlight);

    nonDirectFlightInfo = conn.prepareStatement(searchNonDirectFlight);

    insertIntoItin = conn.prepareStatement(insertItin);

    fidInfo = conn.prepareStatement(getFidInfo);

    single_itinerary = conn.prepareStatement(getItinerary);

    reservationCount = conn.prepareStatement(getReservationCount);

    InsertIntoRes = conn.prepareStatement(getInsertIntoRes);

    reservation = conn.prepareStatement(getReservation);

    userMoney = conn.prepareStatement(getUserMoney);

    reservationAll = conn.prepareStatement(getReservationAll);

    updateMoney = conn.prepareStatement(updateUserMoney);

    userIdFromUser = conn.prepareStatement(getUserIdFromUser);

    serializeTransaction = conn.prepareStatement(serialize);

    repeatTransaction = conn.prepareStatement(repeatSerialize);
  }

  /**
   * Takes a user's username and password and attempts to log the user in.
   *
   * @param username user's username
   * @param password user's password
   *
   * @return If someone has already logged in, then return "User already logged in\n" For all other
   *         errors, return "Login failed\n". Otherwise, return "Logged in as [username]\n".
   */
  public String transaction_login(String username, String password) {
    try {
      try {
        // Get user's hash password and salt
        singleUserPassword.clearParameters();       // Clears parameters from previous use
        String usernameLower = username.toLowerCase();
        singleUserPassword.setString(1, usernameLower);
        // START transaction
        conn.setAutoCommit(false);
        ResultSet rs = singleUserPassword.executeQuery();

        byte[] salt = null;
        byte[] Data_hash = null;

        if (!rs.next()) { // No user under that username
          // STOP transaction
          conn.rollback();
          conn.setAutoCommit(true);
          return "Login failed\n";
        }
        // There is a user under that usernames
        Data_hash = rs.getBytes("password");
        salt = rs.getBytes("salt");

        // Get the hash password
        // Specify the hash parameters
        KeySpec spec = new PBEKeySpec(password.toCharArray(), salt, HASH_STRENGTH, KEY_LENGTH);

        // Generate the hash
        SecretKeyFactory factory = null;
        byte[] hash = null;
        try {
          factory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA1");
          hash = factory.generateSecret(spec).getEncoded();
        } catch (NoSuchAlgorithmException | InvalidKeySpecException ex) {
          throw new IllegalStateException();
        }

        // Check if the hashed password is equal to hashed password from database
        boolean passwordsMatched =  Arrays.equals(Data_hash, hash);
        //System.out.println("Hashed passwords match? "+ passwordsMatched);

        // DIFFERENT CASES WE NEED TO THINK ABOUT!:
        // 1) password matches and user is NOT already signed in = succeeds
        if (passwordsMatched && !userLoggedIn) {
          userLoggedIn = true;
          currentUser = usernameLower;
          // System.out.println("userloggedIn boolean " + userLoggedIn );
          // System.out.println("currentUser " + currentUser );
          // clearItinerariesTable();
          // END TRANSACTION WITH SUCCESS
          conn.commit();
          conn.setAutoCommit(true);
          return "Logged in as " + usernameLower + "\n";
        }

        // 2) user is already signed in = fails
        if (userLoggedIn) { //|| currentUser.equals(usernameLower)) {
          // END TRANSACTION WITH FAIL
          conn.rollback();
          conn.setAutoCommit(true);
          //System.out.println("says user is still logged in");
          return "User already logged in\n";
        }
        conn.rollback();
        conn.setAutoCommit(true);
        return "Login failed\n";

      } catch (SQLException e) {
        try {
          conn.rollback();
          conn.setAutoCommit(true);
          return "Login failed\n";
        } catch (SQLException ex) {
          return "Login failed\n";
        }
        //e.printStackTrace();
      }
    } finally {
      checkDanglingTransaction();
    }
  }

  /**
   * Implement the create user function.
   *
   * @param username   new user's username. User names are unique the system.
   * @param password   new user's password.
   * @param initAmount initial amount to deposit into the user's account, should be >= 0 (failure
   *                   otherwise).
   *
   * @return either "Created user {@code username}\n" or "Failed to create user\n" if failed.
   */
  public String transaction_createCustomer(String username, String password, int initAmount) {
    try {
      // First step: Check if amount is < 0
      if (initAmount < 0) {
        return "Failed to create user\n";
      }
      try {
        // Second step: Check if username exists in User Table
        String name = username.toLowerCase();
        userIdFromUser.clearParameters();
        userIdFromUser.setString(1, name);
        ResultSet rs = userIdFromUser.executeQuery();

        if (rs.next()) {
          // there's already a username in the User's table
          // END TRANSACTION WITH FAIL
          //conn.rollback();
          //conn.setAutoCommit(true);
          return "Failed to create user\n";
        }
        //rs.close();

        // Third step: Getting the user into the database
        // Generate a random cryptographic salt
        // AT THIS POINT: there's no username in the User's table
        SecureRandom random = new SecureRandom();
        byte[] salt = new byte[16];
        random.nextBytes(salt);

        // Specify the hash parameters
        KeySpec spec = new PBEKeySpec(password.toCharArray(), salt, HASH_STRENGTH, KEY_LENGTH);

        // Generate the hash
        SecretKeyFactory factory = null;
        byte[] hash = null;
        try {
          factory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA1");
          hash = factory.generateSecret(spec).getEncoded();
        } catch (NoSuchAlgorithmException | InvalidKeySpecException ex) {
          throw new IllegalStateException();
        }

        // System.out.println("PASSWORD: " + hash);
        // System.out.println("SALT: " + salt);
        // System.out.println("USER NAME: " + name);
        // System.out.println("AMOUNT: " + initAmount);
        // Step 4: prepared statement is ready to be put into database
        // Put the username, salt, hash into the User's table
        insertNewUserStatement.clearParameters();       // Clears parameters from previous use
        insertNewUserStatement.setString(1, name);      // Sets the first parameter (the first “?”) to the value of the variable “name” which is lower case
        insertNewUserStatement.setBytes(2, salt);       // Sets the salt for the password
        insertNewUserStatement.setBytes(3, hash);      // Sets the fourth parameter with “hash” which is hashed password
        insertNewUserStatement.setInt(4, initAmount);  // sets initial amount
        // START transaction
        conn.setAutoCommit(false);
        insertNewUserStatement.execute(); // Executes the query and stores the ResultSet in the variable “rs”

        // END TRANSACTION WITH SUCCESS
        conn.commit();
        conn.setAutoCommit(true);
        return "Created user " + username + "\n";

      } catch(SQLException e) {
        try {
          conn.rollback();
          conn.setAutoCommit(true);
          return "Failed to create user\n";
        } catch (SQLException ex) {
          return "Failed to create user\n";
        }
        //e.printStackTrace();
      }
      //return "Failed to create user: ended\n";
    } finally {
      checkDanglingTransaction();
    }
  }


  /**
   * Implement the search function.
   *
   * Searches for flights from the given origin city to the given destination city, on the given day
   * of the month. If {@code directFlight} is true, it only searches for direct flights, otherwise
   * is searches for direct flights and flights with two "hops." Only searches for up to the number
   * of itineraries given by {@code numberOfItineraries}.
   *
   * The results are sorted based on total flight time.
   *
   * @param originCity
   * @param destinationCity
   * @param directFlight        if true, then only search for direct flights, otherwise include
   *                            indirect flights as well
   * @param dayOfMonth
   * @param numberOfItineraries number of itineraries to return
   *
   * @return If no itineraries were found, return "No flights match your selection\n". If an error
   *         occurs, then return "Failed to search\n".
   *
   *         Otherwise, the sorted itineraries printed in the following format:
   *
   *         Itinerary [itinerary number]: [number of flights] flight(s), [total flight time]
   *         minutes\n [first flight in itinerary]\n ... [last flight in itinerary]\n
   *
   *         Each flight should be printed using the same format as in the {@code Flight} class.
   *         Itinerary numbers in each search should always start from 0 and increase by 1.
   *
   * @see Flight#toString()
   */
  public String transaction_search(String originCity, String destinationCity, boolean directFlight,
                                   int dayOfMonth, int numberOfItineraries) {
    try {
      StringBuffer sb = new StringBuffer();
      try {

        if (numberOfItineraries < 0) {
          return "No flights match your selection\n";
        }
        // setting unique identifier for this specific search
        String group = originCity + destinationCity + directFlight + dayOfMonth + numberOfItineraries;
        // System.out.println(group);
        searchID = group;
        boolean itinerariesLocated = false;



        // START TRANSACTION
        conn.setAutoCommit(false);
        // clearItinerariesTable();

        // finding if unique search id already exists in iteneraries table
        String searchExist = "SELECT * FROM Itineraries WHERE searchID = ? ORDER BY id ASC";
        PreparedStatement searchFound = conn.prepareStatement(searchExist);
        searchFound.clearParameters();
        searchFound.setString(1, group);
        ResultSet itinerariesFound = searchFound.executeQuery();  

        while(itinerariesFound.next()) {
          itinerariesLocated = true;
          String stopover = itinerariesFound.getString("stopover");
          int totalTime = itinerariesFound.getInt("flightTime");
          int id = itinerariesFound.getInt("id");

          // CHECK IF DIRECT OR INDIRECT
          if (stopover != null) { // notnull = indirect
            // get fid first flight
            int fid1 = itinerariesFound.getInt("fidFirst");
            String info1 = itineraryInfo(fid1, dayOfMonth, originCity, stopover);
            // get fid second flight
            int fid2 = itinerariesFound.getInt("fidSecond");
            String info2 = itineraryInfo(fid2, dayOfMonth, stopover, destinationCity);

            sb.append("Itinerary " + id + ": 2 flight(s), " + totalTime + " minutes\n");
            sb.append(info1);
            sb.append(info2);
          } else { // null = direct
            // get fidFirst
            int fid1 = itinerariesFound.getInt("fidFirst");
            // get information for the one flight
            String info = itineraryInfo(fid1, dayOfMonth, originCity, destinationCity);
            sb.append("Itinerary " + id + ": 1 flight(s), " + totalTime + " minutes\n");
            sb.append(info);
          }
          conn.commit();
          conn.setAutoCommit(true);
        } 


        if (!itinerariesLocated) {
          // FINDING DIRECT FLIGHTS from flights table
          directFlightInfo.clearParameters();       // Clears parameters from previous use
          directFlightInfo.setInt(1, numberOfItineraries);      // Sets the first parameter to number of rows needed
          directFlightInfo.setString(2, originCity);       // set the origin city
          directFlightInfo.setString(3, destinationCity);      // sets the destination city
          directFlightInfo.setInt(4, dayOfMonth);  // sets day of month
          ResultSet oneHopResults = directFlightInfo.executeQuery();

          //System.out.println("DONE executing DIRECT flights");
          //System.out.println("ONE HOP RESults" + oneHopResults.toString());

          // Keep track of the number of itineraries
          int itineraryNum = 0;

          // Get direct flight itineraries
          while (oneHopResults.next()) {
            // System.out.println(itineraryNum);
            int flightID = oneHopResults.getInt("fid");
            String result_originCity = oneHopResults.getString("origin_city");
            String result_destCity = oneHopResults.getString("dest_city");
            int result_time = oneHopResults.getInt("actual_time");
            // System.out.println("Itinerary number count: " + itineraryNum);
            // Put the direct flight into itineraries
            insertIntoItin.clearParameters();
            insertIntoItin.setString(1, group);
            insertIntoItin.setInt(2, itineraryNum);
            insertIntoItin.setString(3, result_originCity);      // Sets origin city
            insertIntoItin.setString(4, result_destCity);       // set the dest city
            insertIntoItin.setString(5, null);      // direct = null, indirect = city
            insertIntoItin.setInt(6, flightID);   // fid first
            insertIntoItin.setInt(7, -1);      // fid second
            insertIntoItin.setInt(8, result_time); // flight time
            try {
              insertIntoItin.execute();
            } catch (SQLException e) {
              conn.rollback();
              conn.setAutoCommit(true);
              return transaction_search(originCity, destinationCity, directFlight, dayOfMonth, numberOfItineraries);
            }
            itineraryNum += 1;

            // Put the information into the sb
            if (directFlight) { // THEY ONLY WANT DIRECT FLIGHTS
              int result_dayOfMonth = oneHopResults.getInt("day_of_month");
              String result_carrierId = oneHopResults.getString("carrier_id");
              String result_flightNum = oneHopResults.getString("flight_num");
              int result_capacity = oneHopResults.getInt("capacity");
              int result_price = oneHopResults.getInt("price");
              // adding results to the string buffer
              sb.append("Itinerary " + (itineraryNum - 1) + ": 1 flight(s), " + result_time + " minutes" + "\n");
              sb.append("ID: " + flightID + " Day: " + result_dayOfMonth + " Carrier: " + result_carrierId + " Number: "
                      + result_flightNum + " Origin: " + result_originCity + " Dest: "
                      + result_destCity + " Duration: " + result_time + " Capacity: " + result_capacity
                      + " Price: " + result_price + "\n");
            }
          }
          if (directFlight) { // can close the transaction
            conn.commit();
            conn.setAutoCommit(true);
          }
          oneHopResults.close();

          // CHECK IF THEY WANT INDIRECT FLIGHTS TOO
          //System.out.println("direct flight: 0 = false 1 = true: " + directFlight);
          int itinerariesLeft = numberOfItineraries - itineraryNum;
          //System.out.println("itinerariesLeft: " + itinerariesLeft);
          if (!directFlight && itinerariesLeft >= 0) { // want indirect also
            //System.out.println("Itineraries we need for indirect after direct: " + itinerariesLeft);

            // Call the indirect flights SQL
            // Indirect itineraries to fill the rest if direct flights are not enough
            nonDirectFlightInfo.clearParameters();       // Clears parameters from previous use
            nonDirectFlightInfo.setInt(1, itinerariesLeft);     // only get the number of itineraries we need after direct flight
            nonDirectFlightInfo.setString(2, originCity);       // set the origin city
            nonDirectFlightInfo.setString(3, destinationCity);      // sets the destination city
            nonDirectFlightInfo.setInt(4, dayOfMonth);  // sets day of month
            ResultSet twoHopResults = nonDirectFlightInfo.executeQuery();

            // Put the indirect flights into the itineraries database
            while (twoHopResults.next()) {
              // ADD indirect itineraries to Itineraries table
              int total_flightTime = twoHopResults.getInt("totalTime");
              // // information from flight one
              int id_F1 = twoHopResults.getInt("F1_fid");
              int id_F2 = twoHopResults.getInt("F2_fid");
              String originCity_F1 = twoHopResults.getString("F1origin_city");
              String stopover = twoHopResults.getString("F1Dest");
              String destF2 = twoHopResults.getString("F2Dest");

              // Inserting two hop flights into itinerary table
              // System.out.println("Itinerary number count: " + itineraryNum);
              insertIntoItin.clearParameters();       // Clears parameters from previous use
              insertIntoItin.setString(1, group);
              insertIntoItin.setInt(2, itineraryNum);
              insertIntoItin.setString(3, originCity_F1);      // Sets origin city
              insertIntoItin.setString(4, destF2);       // set the dest city
              insertIntoItin.setString(5, stopover);      // direct = null, indirect = city
              insertIntoItin.setInt(6, id_F1);   // fid first
              insertIntoItin.setInt(7, id_F2);      // fid second
              insertIntoItin.setInt(8, total_flightTime); // flight time
              insertIntoItin.execute();
              itineraryNum += 1;
            }
            // PRINT OUT THE INFORMATION FOR THE FLIGHT
            // Get the itineraries
            String getItins = "SELECT * FROM Itineraries WHERE searchID = ? ORDER BY flightTime ASC";
            PreparedStatement getItinerariesALL = conn.prepareStatement(getItins);
            getItinerariesALL.setString(1, searchID);
            ResultSet allItineraries = getItinerariesALL.executeQuery();
            conn.commit();
            conn.setAutoCommit(true);

            int i = 0;
            // ADD all the itineraries into the sb string
            while (allItineraries.next()) {
              //System.out.println("printing out the itineraries"); // debug
              //System.out.println(i); // debug purpose
              String stopover = allItineraries.getString("stopover");
              int totalTime = allItineraries.getInt("flightTime");

              // CHECK IF DIRECT OR INDIRECT
              if (stopover != null) { // notnull = indirect
                // get fid first flight
                int fid1 = allItineraries.getInt("fidFirst");
                String info1 = itineraryInfo(fid1, dayOfMonth, originCity, stopover);
                // get fid second flight
                int fid2 = allItineraries.getInt("fidSecond");
                String info2 = itineraryInfo(fid2, dayOfMonth, stopover, destinationCity);

                sb.append("Itinerary " + i + ": 2 flight(s), " + totalTime + " minutes\n");
                sb.append(info1);
                sb.append(info2);

                String update = "UPDATE Itineraries SET id = ? WHERE fidFirst = ? AND fidSecond = ?";
                PreparedStatement updateStatement = conn.prepareStatement(update);
                updateStatement.clearParameters();       // Clears parameters from previous use
                updateStatement.setInt(1, i);
                updateStatement.setInt(2, fid1);
                updateStatement.setInt(3, fid2);
                updateStatement.execute();
              } else { // null = direct
                // get fidFirst
                int fid1 = allItineraries.getInt("fidFirst");
                // get information for the one flight
                String info = itineraryInfo(fid1, dayOfMonth, originCity, destinationCity);
                sb.append("Itinerary " + i + ": 1 flight(s), " + totalTime + " minutes\n");
                sb.append(info);

                String update = "UPDATE Itineraries SET id = ? WHERE fidFirst = ?";
                PreparedStatement updateStatement = conn.prepareStatement(update);
                updateStatement.clearParameters();       // Clears parameters from previous use
                updateStatement.setInt(1, i);
                updateStatement.setInt(2, fid1);
                updateStatement.execute();
              }
              i++;
            }
            allItineraries.close();
            twoHopResults.close();
          }
        }
        
      } catch (SQLException e) {
        try {
          conn.rollback();
          conn.setAutoCommit(true);
          e.printStackTrace();
          return "Failed to search\n";
        } catch (SQLException ex) {
          return "Failed to search\n";
        }
      }
      // outside catch statement
      // Does have stuff to return
      if (sb.length() > 0) {
        return sb.toString();
      } else {
        return "No flights match your selection\n";
      }
    } finally {
      checkDanglingTransaction();
    }
  }

  // returns string with the itinerary information
  private String itineraryInfo(int fidNum, int day, String origin, String dest){
    try {
      fidInfo.clearParameters();
      fidInfo.setInt(1, fidNum);
      ResultSet results = fidInfo.executeQuery();
      results.next();

      // Get information about
      String carrier = results.getString("carrier_id");
      String flightNum = results.getString("flight_num");
      int cap = results.getInt("capacity");
      int price = results.getInt("price");
      int time = results.getInt("actual_time");

      String r = ("ID: " + fidNum + " Day: "+ day + " Carrier: " + carrier
              + " Number: " + flightNum + " Origin: "+ origin +
              " Dest: "+ dest + " Duration: " + time +
              " Capacity: " + cap+ " Price: " + price + "\n");
      return r;

    } catch(SQLException e){
      e.printStackTrace();
      return "Failed to search: itineraries\n";
    }
  }

  /**
   * Implements the book itinerary function.
   *
   * @param itineraryId ID of the itinerary to book. This must be one that is returned by search in
   *                    the current session.
   *
   * @return If the user is not logged in, then return "Cannot book reservations, not logged in\n".
   *         If the user is trying to book an itinerary with an invalid ID or without having done a
   *         search, then return "No such itinerary {@code itineraryId}\n". If the user already has
   *         a reservation on the same day as the one that they are trying to book now, then return
   *         "You cannot book two flights in the same day\n". For all other errors, return "Booking
   *         failed\n".
   *
   *         And if booking succeeded, return "Booked flight(s), reservation ID: [reservationId]\n"
   *         where reservationId is a unique number in the reservation system that starts from 1 and
   *         increments by 1 each time a successful reservation is made by any user in the system.
   */
  public String transaction_book(int itineraryId) {
    try {

      if (!userLoggedIn) { // user is not logged in
        return "Cannot book reservations, not logged in\n";
      }

      try {
        // User is logged in
        single_itinerary.clearParameters();
        single_itinerary.setInt(1, itineraryId);
        single_itinerary.setString(2, searchID);
        conn.setAutoCommit(false);
        serializeTransaction.execute();
        ResultSet itinerary = single_itinerary.executeQuery(); // get the itinerary

        if (itinerary.next()) { // if the itinerary exists
          //System.out.println("has itinerary");

          // check if direct or indirect flight
          String stopover = itinerary.getString("stopover");
          boolean flightsAvailable = false;
          int flight1 = itinerary.getInt("fidFirst"); // get fid of first flight
          int flight2 = 0;

          // conn.setAutoCommit(false);
          // serializeTransaction.execute();
          if (stopover != null) { // notnull = indirect
            //System.out.println("indirect");
            flight2 = itinerary.getInt("fidSecond"); // get fid of second flight
            // See if both flights have capacity for another person
            //System.out.println(hasCapacity(flight1) && hasCapacity(flight2));
            if (hasCapacity(flight1) && hasCapacity(flight2)) { // both flights are available
              flightsAvailable = true;
            }
          } else { // null = direct
            //System.out.println("direct");
            //System.out.println(hasCapacity(flight1));
            flightsAvailable = hasCapacity(flight1);
          }

          if (!flightsAvailable) { // flights don't have seats open
            //System.out.println("no flight available");
            conn.rollback();
            conn.setAutoCommit(true);
            return "Booking failed\n";
          }

          // check if this flight is on same day as another flight they reserved
          // flights table column "day_of_month"
          fidInfo.clearParameters();
          fidInfo.setInt(1, flight1);
          ResultSet flightInfoDay = fidInfo.executeQuery();
          flightInfoDay.next();
          int day = flightInfoDay.getInt("day_of_month");
          //System.out.println("day: " + day);
          String test = "SELECT * FROM Reservations WHERE username = ? AND day = ?";
          PreparedStatement previousReserv = conn.prepareStatement(test);
          previousReserv.clearParameters();
          previousReserv.setString(1, currentUser);
          previousReserv.setInt(2, day);
          ResultSet usersReservations = previousReserv.executeQuery();
          if (usersReservations.next()) {
            // there is a reservation on that day by this user
            conn.rollback();
            conn.setAutoCommit(true);
            return "You cannot book two flights in the same day\n";
          }
          //flightInfoDay.close();
          //usersReservations.close();
          // User is logged in, valid itinerary #, seats are available, and no previous reservation on day
          ResultSet resCount = reservationCount.executeQuery();
          resCount.next();
          int reservationNum = resCount.getInt("count");
          //System.out.println("reservation number: " + reservationNum);
          // INSERT RESERVATION
          InsertIntoRes.clearParameters();
          InsertIntoRes.setInt(1, (reservationNum + 1));
          InsertIntoRes.setString(2, currentUser);
          InsertIntoRes.setInt(3, day);
          InsertIntoRes.setInt(4, flight1);
          InsertIntoRes.setInt(5, flight2);
          InsertIntoRes.setInt(6, 0);
          InsertIntoRes.setInt(7, 0);
          // Deadlock case:
          try {
            InsertIntoRes.execute();
          } catch (SQLException e) {
            conn.rollback();
            conn.setAutoCommit(true);
            return transaction_book(itineraryId);
          }
          //resCount.close();
          //itinerary.close();
          conn.commit();
          conn.setAutoCommit(true);
          return "Booked flight(s), reservation ID: " + (reservationNum + 1) + "\n";

        } else { // no itinerary exists
          conn.rollback();
          conn.setAutoCommit(true);
          return "No such itinerary " + itineraryId + "\n";
        }

      } catch(SQLException e){
        try {
          conn.rollback();
          conn.setAutoCommit(true);
          e.printStackTrace();
          return "Booking failed\n";
        } catch (SQLException ex) {
          return "Booking failed\n";
        }
        //e.printStackTrace();
      }
      //return "Booking failed\n";
    } finally {
      checkDanglingTransaction();
    }
  }

  // Takes in a flight number,
  // and checks if the flight has capacity or not
  private boolean hasCapacity(int flightNum) {
    try {
      // set variables to compare
      int planeCap = 0;
      int currCount = 0;

      // gets capacity of flights from Flights table
      fidInfo.clearParameters();
      fidInfo.setInt(1, flightNum);
      ResultSet flightInfo = fidInfo.executeQuery();
      flightInfo.next();
      planeCap = flightInfo.getInt("capacity");
      //System.out.println("capacity: " + planeCap);

      // Counts how many seats are already booked on the flight (look at current reservations)
      // only count the non-canceled reservations
      String reservationCount1 = "SELECT COUNT(*) AS Count FROM Reservations WHERE (flight1 = ? OR flight2 = ?) AND cancelled = 0";
      PreparedStatement reserveTaken = conn.prepareStatement(reservationCount1);
      reserveTaken.clearParameters();
      reserveTaken.setInt(1, flightNum);
      reserveTaken.setInt(2, flightNum);
      ResultSet reserveCount = reserveTaken.executeQuery();

      reserveCount.next();
      currCount = reserveCount.getInt("Count");
      //System.out.println("RESERVATION combine TAKEN: " +  currCount);

      // returns true if there are seats available
      // returns false if there are no seats available
      //flightInfo.close();
      return (planeCap > currCount);

    } catch(SQLException e){
      e.printStackTrace();
    }
    return false;
  }


  /**
   * Implements the pay function.
   *
   * @param reservationId the reservation to pay for.
   *
   * @return If no user has logged in, then return "Cannot pay, not logged in\n" If the reservation
   *         is not found / not under the logged in user's name, then return "Cannot find unpaid
   *         reservation [reservationId] under user: [username]\n" If the user does not have enough
   *         money in their account, then return "User has only [balance] in account but itinerary
   *         costs [cost]\n" For all other errors, return "Failed to pay for reservation
   *         [reservationId]\n"
   *
   *         If successful, return "Paid reservation: [reservationId] remaining balance:
   *         [balance]\n" where [balance] is the remaining balance in the user's account.
   */
  public String transaction_pay(int reservationId) {
    try {
      // If user is not logged in
      if (!userLoggedIn) {
        return "Cannot pay, not logged in\n";
      }

      try {
        // check if they reservationID exists for that user
        reservation.clearParameters();
        reservation.setInt(1, reservationId);
        reservation.setString(2, currentUser);
        ResultSet reservationInfo = reservation.executeQuery();
        if (!reservationInfo.next()){ // no reservation found
          return  "Cannot find unpaid reservation " + reservationId + " under user: " +  currentUser + "\n";
        }
        // Make sure that the reservation isn't already paid for
        int paidFor = reservationInfo.getInt("paid");
        if (paidFor == 1) { // they already paid
          return  "Cannot find unpaid reservation " + reservationId + " under user: " +  currentUser + "\n";
        }
        // check if user has money for both flights
        // get user's money
        userMoney.clearParameters();
        userMoney.setString(1, currentUser);
        // Start transaction
        conn.setAutoCommit(false);
        ResultSet userBalance = userMoney.executeQuery();
        userBalance.next();
        int currBalance = userBalance.getInt("balance");
        // get flight(s) and get the cost
        int flight1 = reservationInfo.getInt("flight1");
        int flight2 = reservationInfo.getInt("flight2");
        int flightCost = getFlightCost(flight1) + getFlightCost(flight2);
        // System.out.println("Flight cost: " + flightCost);
        if (currBalance < flightCost) { // user does not have enough money
          // Transaction end and rollback failed
          conn.rollback();
          conn.setAutoCommit(true);
          return "User has only " + currBalance + " in account but itinerary costs " + flightCost + "\n";
        } else { // user has enough money
          int moneyLeft = currBalance - flightCost;
          // update user's balance
          updateMoney.clearParameters();
          updateMoney.setInt(1, moneyLeft);
          updateMoney.setString(2, currentUser);
          updateMoney.execute();
          // update reservation saying that it's paid for now
          String updateResPaid = "UPDATE Reservations SET paid = ? WHERE rid = ?";
          PreparedStatement updatePaid = conn.prepareStatement(updateResPaid);
          updatePaid.clearParameters();
          updatePaid.setInt(1, 1); // 1 = paid
          updatePaid.setInt(2, reservationId);
          updatePaid.execute();
          // End transaction in success
          conn.commit();
          conn.setAutoCommit(true);
          return "Paid reservation: " + reservationId + " remaining balance: " + moneyLeft + "\n";
        }
      } catch (SQLException e) {
        try {
          conn.rollback();
          conn.setAutoCommit(true);
          e.printStackTrace();
          return "Failed to pay for reservation " + reservationId + "\n";
        } catch (SQLException ex) {
          return "Failed to pay for reservation " + reservationId + "\n";
        }
        //e.printStackTrace();
      }
    } finally {
      checkDanglingTransaction();
    }
  }



  // Takes in a flight number and returns the price
  // of that flight
  private int getFlightCost (int fid) {
    try {
      if (fid != 0) {
        fidInfo.clearParameters();
        fidInfo.setInt(1, fid);
        ResultSet flightInfo = fidInfo.executeQuery();
        flightInfo.next();
        // returns flight's price
        int flightPrice = flightInfo.getInt("price");
        //flightInfo.close();
        return flightPrice;
      }
    } catch(SQLException e) {
      e.printStackTrace();
    }
    return 0;
  }

  /**
   * Implements the reservations function.
   *
   * @return If no user has logged in, then return "Cannot view reservations, not logged in\n" If
   *         the user has no reservations, then return "No reservations found\n" For all other
   *         errors, return "Failed to retrieve reservations\n"
   *
   *         Otherwise return the reservations in the following format:
   *
   *         Reservation [reservation ID] paid: [true or false]:\n
   *         [flight 1 under the
   *         reservation]\n [flight 2 under the reservation]\n Reservation [reservation ID] paid:
   *         [true or false]:\n [flight 1 under the reservation]\n [flight 2 under the
   *         reservation]\n ...
   *
   *         Each flight should be printed using the same format as in the {@code Flight} class.
   *
   * @see Flight#toString()
   */
  public String transaction_reservations() {
    try {
      if (!userLoggedIn) { // user is not logged in
        return "Cannot view reservations, not logged in\n";
      }
      StringBuffer sb = new StringBuffer();

      try {
        // Get reservations where username = currentUser
        reservationAll.clearParameters();
        reservationAll.setString(1, currentUser);
        conn.setAutoCommit(false);
        serializeTransaction.execute();
        ResultSet usersReservation = reservationAll.executeQuery();


        while (usersReservation.next()) { // loop through all of the reservations
          //System.out.println("in the while loop");
          int cancelled = usersReservation.getInt("cancelled");
          if (cancelled == 0) { // only print is not canceled
            int resNum = usersReservation.getInt("rid");
            int resDay = usersReservation.getInt("day");
            int flight1 = usersReservation.getInt("flight1");
            int flight2 = usersReservation.getInt("flight2");
            int paid = usersReservation.getInt("paid"); // 0 = not paid, 1 = paid
            boolean booleanPaid = (paid == 1);
            // flight 1 info
            fidInfo.clearParameters();
            fidInfo.setInt(1, flight1);
            ResultSet f1 = fidInfo.executeQuery();
            f1.next();
            String f1Origin = f1.getString("origin_city");
            String f1Dest = f1.getString("dest_city");
            //String f1Stuff = itineraryInfo(flight1, resDay, f1Origin, f2Dest);

            sb.append("Reservation " + resNum + " paid: " + booleanPaid + ":\n");
            sb.append(itineraryInfo(flight1, resDay, f1Origin, f1Dest));
            // flight 2 info
            if (flight2 != 0) { // there is a flight 2
              fidInfo.clearParameters();
              fidInfo.setInt(1, flight2);
              ResultSet f2 = fidInfo.executeQuery();
              f2.next();
              String f2Origin = f2.getString("origin_city");
              String f2Dest = f2.getString("dest_city");
              sb.append(itineraryInfo(flight2, resDay, f2Origin, f2Dest));
            }
          }
        }
        conn.commit();
        conn.setAutoCommit(true);
        //System.out.println("outside of while loop");
        //System.out.println("length of sb: " + sb.length());
        if (sb.length() > 0) {
          return sb.toString();
        } else {
          // conn.rollback();
          // conn.setAutoCommit(true);
          return "No reservations found\n";
        }
      } catch(SQLException e){
        try {
          conn.rollback();
          conn.setAutoCommit(true);
          e.printStackTrace();
          return "Failed to retrieve reservations\n";
        } catch (SQLException ex) {
          return "Failed to retrieve reservations\n";
        }
      }
      //return "Failed to retrieve reservations\n";
    } finally {
      checkDanglingTransaction();
    }
  }

  /**
   * Implements the cancel operation.
   *
   * @param reservationId the reservation ID to cancel
   *
   * @return If no user has logged in, then return "Cannot cancel reservations, not logged in\n" For
   *         all other errors, return "Failed to cancel reservation [reservationId]\n"
   *
   *         If successful, return "Canceled reservation [reservationId]\n"
   *
   *         Even though a reservation has been canceled, its ID should not be reused by the system.
   */
  public String transaction_cancel(int reservationId) {
    try {
      if (!userLoggedIn) { // user is not logged in
        return "Cannot cancel reservations, not logged in\n";
      }
      try {
        reservation.clearParameters();
        reservation.setInt(1, reservationId);
        reservation.setString(2, currentUser);
        conn.setAutoCommit(false);
        ResultSet usersReservation = reservation.executeQuery();
        if (usersReservation.next()) { // there is one
          int cancelled = usersReservation.getInt("cancelled");
          if (cancelled == 0) { // they didnt already canceled
            int paid = usersReservation.getInt("paid");
            if (paid == 1) { // person paid and needs refund
              System.out.println("refund the user");
              // get total flights cost
              int fid1 = usersReservation.getInt("flight1");
              int fid2 = usersReservation.getInt("flight2");
              int totalCost = getFlightCost(fid1) + getFlightCost(fid2);
              System.out.println("total cost: " + totalCost);
              // get user balance currently
              userMoney.clearParameters();
              userMoney.setString(1, currentUser);
              ResultSet userCur = userMoney.executeQuery();
              userCur.next();
              int curMoney = userCur.getInt("balance");
              int returnCost = curMoney + totalCost;
              // put money back into user account
              updateMoney.clearParameters();
              updateMoney.setInt(1, returnCost);
              updateMoney.setString(2, currentUser);
              updateMoney.execute();
            }
            // Now, cancel the reservation
            String update = "UPDATE Reservations SET cancelled = 1 WHERE rid = ?";
            PreparedStatement updateRes = conn.prepareStatement(update);
            updateRes.clearParameters();
            updateRes.setInt(1, reservationId);
            updateRes.execute();
            //usersReservation.close();
            conn.commit();
            conn.setAutoCommit(true);
            return "Canceled reservation " + reservationId + "\n";
          }
        }
        conn.rollback();
        conn.setAutoCommit(true);
      } catch(SQLException e) {
        e.printStackTrace();
      }

      return "Failed to cancel reservation " + reservationId + "\n";
    } finally {
      checkDanglingTransaction();
    }
  }




  /**
   * Example utility function that uses prepared statements
   */
  private int checkFlightCapacity(int fid) throws SQLException {
    checkFlightCapacityStatement.clearParameters();
    checkFlightCapacityStatement.setInt(1, fid);
    ResultSet results = checkFlightCapacityStatement.executeQuery();
    results.next();
    int capacity = results.getInt("capacity");
    results.close();

    return capacity;
  }

  /**
   * Throw IllegalStateException if transaction not completely complete, rollback.
   *
   */
  private void checkDanglingTransaction() {
    try {
      try (ResultSet rs = tranCountStatement.executeQuery()) {
        rs.next();
        int count = rs.getInt("tran_count");
        if (count > 0) {
          throw new IllegalStateException(
                  "Transaction not fully commit/rollback. Number of transaction in process: " + count);
        }
      } finally {
        conn.setAutoCommit(true);
      }
    } catch (SQLException e) {
      throw new IllegalStateException("Database error", e);
    }
  }

  private static boolean isDeadLock(SQLException ex) {
    return ex.getErrorCode() == 1205;
  }

  /**
   * A class to store flight information.
   */
  class Flight {
    public int fid;
    public int dayOfMonth;
    public String carrierId;
    public String flightNum;
    public String originCity;
    public String destCity;
    public int time;
    public int capacity;
    public int price;

    @Override
    public String toString() {
      return "ID: " + fid + " Day: " + dayOfMonth + " Carrier: " + carrierId + " Number: "
              + flightNum + " Origin: " + originCity + " Dest: " + destCity + " Duration: " + time
              + " Capacity: " + capacity + " Price: " + price;
    }
  }
}