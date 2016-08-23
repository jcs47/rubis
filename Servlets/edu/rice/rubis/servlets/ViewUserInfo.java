
package edu.rice.rubis.servlets;

import bftsmart.tom.util.TOMUtil;
import static edu.rice.rubis.servlets.RubisHttpServlet.getRepository;
import static edu.rice.rubis.servlets.RubisHttpServlet.storeSignatures;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import lasige.steeldb.jdbc.BFTPreparedStatement;
import merkletree.Leaf;
import merkletree.MerkleTree;
import merkletree.TreeCertificate;
import org.json.JSONArray;

/**
 * This servlets displays general information about a user. It must be called
 * this way :
 * 
 * <pre>
 * 
 *  http://..../ViewUserInfo?userId=xx where xx is the id of the user
 *  
 * </pre>
 */

public class ViewUserInfo extends RubisHttpServlet
{

  public int getPoolSize()
  {
    return Config.ViewUserInfoPoolSize;
  }
  
  //Keep track of which users/comments were already asked from the database
  protected HashSet<Integer> users_aux = new HashSet();
  protected HashSet<Integer> comments_aux = new HashSet();
  
 private void dropTables() throws ClassNotFoundException, IOException {
      try {
          Statement s = getRepository().createStatement();
          s.executeUpdate("DROP TABLE users");
          s.close();

          s = getRepository().createStatement();
          s.executeUpdate("DROP TABLE comments");
          s.close();
          
          s = getRepository().createStatement();
          s.executeUpdate("DROP TABLE leafHashes");
          s.close();
          
          s = getRepository().createStatement();
          s.executeUpdate("DROP TABLE signatures");
          s.close();
      } catch (SQLException ex) {
          Logger.getLogger(BrowseCategories.class.getName()).log(Level.SEVERE, null, ex);
      }
  }
 
  private boolean verifyCache(ResultSet cachedUsers, ResultSet cachedComments, ServletPrinter sp) {
      
    try {
      
      //sp.printHTML("<p>Verifiyng...</p>");
        
      cachedUsers.beforeFirst();
      if (!cachedUsers.next()) {
          
          sp.printHTML("<p>Empty users resultset!</p>");
          return false;
      }
      
      cachedComments.beforeFirst();
      if (!cachedComments.next()) {
          
          sp.printHTML("<p>Empty comments resultset!</p>");
          return false;
      }
      //sp.printHTML("<p>Verifiyng...</p>");
      
      Timestamp ts = cachedUsers.getTimestamp("timestamp");
      PreparedStatement stmt = null, stmt2 = null;
      Statement s = null;
            
     //copy categories, signatures and leafs to local repository
     // this will make verification easier
     
     // tables...
     
     //drop tables used in repository
     dropTables();
     
     //sp.printHTML("<p>Tables...</p>");
     s = getRepository().createStatement();
      s.executeUpdate("CREATE TABLE users (\n" +
            "   id            INT,\n" +
            "   firstname     VARCHAR(20),\n" +
            "   lastname      VARCHAR(20),\n" +
            "   nickname      VARCHAR(20) NOT NULL UNIQUE,\n" +
            "   email         VARCHAR(50) NOT NULL,\n" +
            "   rating        INTEGER,\n" +
            "   creation_date TIMESTAMP,\n" +
            "   timestamp     TIMESTAMP," +
            "   position      INT," +
            "   index         INT," +
            "   PRIMARY KEY(id)\n" +
            ")");
     s.close();
     
     s = getRepository().createStatement();
     s.executeUpdate("CREATE TABLE comments (\n" +
                "   id           INT,\n" +
                "   nickname     VARCHAR(20) NOT NULL UNIQUE,\n" +
                "   from_user_id INTEGER NOT NULL,\n" +
                "   to_user_id   INTEGER NOT NULL,\n" +
                "   item_id      INTEGER NOT NULL,\n" +
                "   rating       INTEGER,\n" +
                "   date         TIMESTAMP,\n" +
                "   comment      LONG VARCHAR,\n" +
                "   timestamp     TIMESTAMP," +
                "   position      INT," +
                "   index         INT," +
                "   PRIMARY KEY(id)\n" +
            ")");
     s.close();
      
     s = getRepository().createStatement();
     s.executeUpdate("CREATE TABLE signatures (timestamp TIMESTAMP, replica INT, value VARCHAR (128) FOR BIT DATA NOT NULL)");
     s.close();
     
     s = getRepository().createStatement();
     s.executeUpdate("CREATE TABLE leafHashes (timestamp TIMESTAMP, position INT, index INT, value VARCHAR (20) FOR BIT DATA NOT NULL)");
     s.close();

     // insert values...
     //sp.printHTML("<p>Values...</p>");
     cachedUsers.beforeFirst();
     
     while (cachedUsers.next())
     {     

        String sql = "INSERT INTO users VALUES ("
                    + cachedUsers.getInt("id") + ","
                    + "'" + cachedUsers.getString("firstname") + "',"
                    + "'" + cachedUsers.getString("lastname") + "',"
                    + "'" + cachedUsers.getString("nickname") + "',"
                    + "'" + cachedUsers.getString("email") + "',"
                    + cachedUsers.getInt("rating") + ","
                    + "?,"
                    + "?,"
                    + cachedUsers.getInt("position") + ","
                    + "0)";

        stmt = getRepository().prepareStatement(sql);
        stmt.setTimestamp(1, cachedUsers.getTimestamp("creation_date"));
        stmt.setTimestamp(2, ts);
        
        stmt.executeUpdate();
        stmt.close();

     }
     
     cachedUsers.beforeFirst();

     cachedComments.beforeFirst();
     
     while (cachedComments.next())
     {     

        String sql = "INSERT INTO comments VALUES ("
                    + cachedComments.getInt("id") + ","
                    + "'" + cachedComments.getString("nickname") + "',"
                    + cachedComments.getInt("from_user_id") + ","
                    + cachedComments.getInt("to_user_id") + ","
                    + cachedComments.getInt("item_id") + ","
                    + cachedComments.getInt("rating") + ","
                    + "?,"
                    + "'" + cachedComments.getString("comment") + "',"
                    + "?,"
                    + cachedComments.getInt("position") + ","
                    + "1)";

        stmt = getRepository().prepareStatement(sql);
        stmt.setTimestamp(1, cachedComments.getTimestamp("date"));
        stmt.setTimestamp(2, ts);
        
        stmt.executeUpdate();
        stmt.close();

     }
     
     cachedComments.beforeFirst();
     
     stmt = getCache().prepareStatement("SELECT * from signatures WHERE timestamp = ?", ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
     stmt.setTimestamp(1, ts);
     ResultSet rs = stmt.executeQuery();
                
     while (rs.next())
     {
              
      int replica = rs.getInt("replica");
      Timestamp t = rs.getTimestamp("timestamp");
      byte[] b = rs.getBytes("value");

      stmt2 = getRepository().prepareStatement("INSERT INTO signatures VALUES (?," + replica + ",?)");
      stmt2.setTimestamp(1, t);
      stmt2.setBytes(2, b);
      stmt2.executeUpdate();
      stmt2.close();

     }
     stmt.close();
     rs.close();
     
     stmt = getCache().prepareStatement("SELECT * from leafHashes WHERE timestamp = ?", ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
     stmt.setTimestamp(1, ts);
     rs = stmt.executeQuery();
                
     while (rs.next())
     {
              
      int position = rs.getInt("position");
      int index = rs.getInt("index");
      Timestamp t = rs.getTimestamp("timestamp");
      byte[] b = rs.getBytes("value");

      stmt2 = getRepository().prepareStatement("INSERT INTO leafHashes VALUES (?," + position + "," + index + ",?)");
      stmt2.setTimestamp(1, t);
      stmt2.setBytes(2, b);
      stmt2.executeUpdate();
      stmt2.close();

     }
     stmt.close();
     rs.close();
     
     // Fetch categories
     //sp.printHTML("<p>Fetch categories...</p>");
     s = getRepository().createStatement();
     ResultSet users = s.executeQuery("SELECT * FROM users");
          
     // Check respective leafs
     while (users.next())
     {
                 
      JSONArray row = new JSONArray();
      List<byte[]> l = new LinkedList<>();
              
      row.put(1, users.getObject("id"));
      row.put(2, users.getObject("firstname"));
      row.put(3, users.getObject("lastname"));
      row.put(4, users.getObject("nickname"));
      row.put(5, users.getObject("email"));
      row.put(6, users.getObject("rating"));
      row.put(7, users.getTimestamp("creation_date").getTime());
        
      for (int i = 1; i <= 7; i++) {
            l.add(row.get(i).toString().getBytes(StandardCharsets.UTF_8));
      }
      
      byte[] b = TreeCertificate.getLeafHashes(new Leaf(l));

      //sp.printHTML("hash for item " + items.getObject("nickname") + ": " + Arrays.toString(b));
            
      stmt = getRepository().prepareStatement("SELECT count(*) AS total FROM leafHashes WHERE timestamp = ? AND index = " + users.getInt("index") + " AND position = " +users.getInt("position") + " AND value = ?", ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
      //stmt = getRepository().prepareStatement("SELECT * FROM leafHashes WHERE value = ?", ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);

      stmt.setTimestamp(1, users.getTimestamp("timestamp"));
      stmt.setBytes(2, b);
      //stmt.setBytes(1, b);
      rs = stmt.executeQuery();
      rs.next();
      
      /*sp.printHTML("position of hash: " + rs.getInt("position"));
      sp.printHTML("position of item: " + items.getInt("position"));
      sp.printHTML("index of hash: " + rs.getInt("index"));
      sp.printHTML("index of item: " + items.getInt("index"));
      sp.printHTML("timestamp of hash: " + rs.getTimestamp("timestamp"));
      sp.printHTML("timestamp of item: " + items.getTimestamp("timestamp"));*/

      int total = rs.getInt("total");
      //int total = 0;
      stmt.close();
      rs.close();
      if (total == 0 /*&& !Arrays.equals(b, rs.getBytes("value"))*/) {
          s.close();
          users.close();

          sp.printHTML("<p>Leaf hash for user not found!</p>");
          
          //drop tables used in repository
          dropTables();
          return false;
      }

      //sp.printHTML("<p>total number of leaf hashes: " + total + "</p>");
     }
     s.close();
     users.close();
           
     s = getRepository().createStatement();
     ResultSet comments = s.executeQuery("SELECT * FROM comments");
     
     List<byte[]> temp = new LinkedList();
     
     // Check respective leafs
     while (comments.next())
     {
                 
      JSONArray row = new JSONArray();
      List<byte[]> l = new LinkedList<>();
              
      row.put(1, comments.getObject("id"));
      row.put(2, comments.getObject("from_user_id"));
      row.put(3, comments.getObject("to_user_id"));
      row.put(4, comments.getObject("item_id"));
      row.put(5, comments.getObject("rating"));
      row.put(6, comments.getTimestamp("date").getTime());
      row.put(7, comments.getObject("comment"));
      row.put(8, comments.getObject("nickname"));
        
      for (int i = 1; i <= 8; i++) {
            l.add(row.get(i).toString().getBytes(StandardCharsets.UTF_8));
      }
      
      byte[] b = TreeCertificate.getLeafHashes(new Leaf(l));
      temp.add(b);

      //sp.printHTML("hash for item " + items.getObject("nickname") + ": " + Arrays.toString(b));
            
      stmt = getRepository().prepareStatement("SELECT count(*) AS total FROM leafHashes WHERE timestamp = ? AND index = " + comments.getInt("index") + " AND position = " +comments.getInt("position") + " AND value = ?", ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
      //stmt = getRepository().prepareStatement("SELECT * FROM leafHashes WHERE value = ?", ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);

      stmt.setTimestamp(1, comments.getTimestamp("timestamp"));
      stmt.setBytes(2, b);
      //stmt.setBytes(1, b);
      rs = stmt.executeQuery();
      rs.next();
      
      /*sp.printHTML("position of hash: " + rs.getInt("position"));
      sp.printHTML("position of item: " + items.getInt("position"));
      sp.printHTML("index of hash: " + rs.getInt("index"));
      sp.printHTML("index of item: " + items.getInt("index"));
      sp.printHTML("timestamp of hash: " + rs.getTimestamp("timestamp"));
      sp.printHTML("timestamp of item: " + items.getTimestamp("timestamp"));*/

      int total = rs.getInt("total");
      //int total = 0;
      stmt.close();
      rs.close();
      if (total == 0 /*&& !Arrays.equals(b, rs.getBytes("value"))*/) {
          s.close();
          comments.close();

          sp.printHTML("<p>Leaf hash for comment not found!</p>");
          
          //drop tables used in repository
          dropTables();
          return false;
      }

      //sp.printHTML("<p>total number of leaf hashes: " + total + "</p>");
     }
     s.close();
     comments.close();
     
     // re-create first level branches from the certificates
     stmt = getRepository().prepareStatement("SELECT * from leafHashes WHERE timestamp = ? AND index = 0 ORDER BY position", ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
     stmt.setTimestamp(1, ts);
     rs = stmt.executeQuery();
     
     rs.last();
     byte[][] hashes = new byte[rs.getRow()][];
     rs.beforeFirst();
           
     int position = 0;
     while (rs.next()) {
         byte[] data = rs.getBytes("value");
         //sp.printHTML("<p> Value a: " + Arrays.toString(data) + "</p>");
         //if (position < temp.size()) sp.printHTML("<p> Value b: " + Arrays.toString(temp.get(position)) + "</p>");
         hashes[position] = data;
         position++;
     }
     stmt.close();
     rs.close();
     
     MerkleTree[] branches = TreeCertificate.getFirstLevel(hashes);
     
     //sp.printHTML("<p>From table o hashes</p>");
     //for (MerkleTree b: branches)
     //    sp.printHTML("<p>" + Arrays.toString(b.digest()) + "</p>");
     
    //////////////////////////////////////////////////////////
    /*stmt = getRepository().prepareStatement("SELECT name, id, description, initial_price, quantity, reserve_price, buy_now, nb_of_bids, max_bid, start_date, end_date, seller, category, nickname, region FROM items ORDER BY end_date ASC", ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
    rs = stmt.executeQuery();

    JSONArray json = TreeCertificate.getJSON((new ResultSetData(rs)).getRows());

    Leaf[] leafs = TreeCertificate.jsonToLeafs(json);
    
    sp.printHTML("<p>From items' result set</p>");
    for (Leaf b : leafs) {
        sp.printHTML("<p>" + Arrays.toString(TreeCertificate.getLeafHashes(b)) + "</p>");
    }
    
    branches = TreeCertificate.getFirstLevel(leafs);

    sp.printHTML("<p>From items' result set</p>");
    for (MerkleTree b: branches)
         sp.printHTML("<p>" + Arrays.toString(b.digest()) + "</p>");*/
    
     //////////////////////////////////////////////////////////
     
     while (branches.length > 1)
          branches = TreeCertificate.getNextLevel(branches);
      
     //sp.printHTML("<p> MerkleTree root for users:" + Arrays.toString(branches[0].digest())  + "</p>");
      
     LinkedList<byte[]> l = new LinkedList<>();
     for (MerkleTree b : branches) {
         l.add(b.digest());
     }
     
     //Get second root (from comments)
     stmt = getRepository().prepareStatement("SELECT * from leafHashes WHERE timestamp = ? AND index = 1 ORDER BY position", ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
     stmt.setTimestamp(1, ts);
     rs = stmt.executeQuery();
      
     rs.last();
     hashes = new byte[rs.getRow()][];
     rs.beforeFirst();
           
     position = 0;
     while (rs.next()) {
         byte[] data = rs.getBytes("value");
         //sp.printHTML("<p> Value a: " + Arrays.toString(data) + "</p>");
         //if (position < temp.size()) sp.printHTML("<p> Value b: " + Arrays.toString(temp.get(position)) + "</p>");
         hashes[position] = data;
         position++;
     }
     stmt.close();
     rs.close();
     
     branches = TreeCertificate.getFirstLevel(hashes);
     
     while (branches.length > 1)
          branches = TreeCertificate.getNextLevel(branches);
      
     //sp.printHTML("<p> MerkleTree root for comments:" + Arrays.toString(branches[0].digest())  + "</p>");
      
     for (MerkleTree b : branches) {
         l.add(b.digest());
     }
 
     
     //Verifiyng signatures
     stmt = getRepository().prepareStatement("SELECT * FROM signatures WHERE timestamp = ?", ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
     stmt.setTimestamp(1, ts);
     rs = stmt.executeQuery();
      
     int count = 0;
      
     while (rs.next()) {
          
         byte[] buffer = TreeCertificate.concatenate(rs.getInt("replica"), ts.getTime(), l);
         
         //sp.printHTML("<p>Concatenation: " + Arrays.toString(buffer) + "</p>");
         boolean verify = TOMUtil.verifySignature(getReplicaKey(rs.getInt("replica")), buffer, rs.getBytes("value"));
          
         //sp.printHTML("<p>Verified: " + rs.getInt("replica") + ": " + verify + "</p>");
          
         if (verify) count++;
          
     }
     stmt.close();
     rs.close();
     
     //drop tables used in repository
     dropTables();
          
     return count > 2*RubisHttpServlet.F;
      
    } catch (Exception ex) {
        printException(ex, sp);
        
        return false;    
    }    
  
  }

  private void commentList(ResultSet rs, PreparedStatement stmt,
      Connection conn, ServletPrinter sp)
  {
    //ResultSet rs = null;
    String date, comment;
    int authorId;
    
    try
    {

      sp.printHTML("<br><hr><br><h3>Comments for this user</h3><br>");

      sp.printCommentHeader();
      // Display each comment and the name of its author
      while (rs.next())
      {
        comment = rs.getString("comment");
        date = rs.getString("date");
        authorId = rs.getInt("from_user_id");
        String authorName = rs.getString("nickname");

        /*String authorName = "none";
        ResultSet authorRS = null;
        PreparedStatement authorStmt = null;
        try
        {
          authorStmt = conn
              .prepareStatement("SELECT nickname FROM users WHERE id=?",
		ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
          authorStmt.setInt(1, authorId);
          authorRS = authorStmt.executeQuery();
          if (authorRS.first())
            authorName = authorRS.getString("nickname");
          authorStmt.close();
        }
        catch (Exception e)
        {
          printError("Failed to execute Query for the comment author.", sp);
          printException(e, sp);
          conn.rollback();
          authorStmt.close();
          closeConnection(stmt, conn);
          return false;
        }*/
        sp.printComment(authorName, authorId, date, comment);
      }
      
      sp.printCommentFooter();
      //conn.commit();
      rs.beforeFirst();
    }
    catch (Exception e)
    {
      printError("Exception getting comment list.", sp);
      printException(e, sp);
      try
      {
        conn.rollback();
        closeConnection(stmt, conn);
        return;
      }
      catch (Exception se)
      {
        printError("Transaction rollback failed.", sp);
        printException(e, sp);
        closeConnection(stmt, conn);
        return;
      }
    }
    return;
  }

  public void doGet(HttpServletRequest request, HttpServletResponse response)
      throws IOException, ServletException
  {
    doPost(request, response);
  }

  public void doPost(HttpServletRequest request, HttpServletResponse response)
      throws IOException, ServletException
  {
    String value = request.getParameter("userId");
    Integer userId;
    ResultSet users = null, comments = null;
    ServletPrinter sp = null;
    PreparedStatement usersStmt = null, commentsStmt = null;
        
    Connection conn = getConnection();
    
    sp = new ServletPrinter(response, "ViewUserInfo");
    sp.printHTMLheader("RUBiS: View user information");
    
    if ((value == null) || (value.equals("")))
    {
      printError("You must provide a user identifier!", sp);
      sp.printHTMLfooter();
      return;
    }
    else
      userId = new Integer(value);

    boolean fromCache = false;
    
    // Try to find the user corresponding to the userId
    try
    {
      conn.setAutoCommit(false);

      usersStmt = getCache().prepareStatement("SELECT * FROM users WHERE id=?",
	ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
      usersStmt.setInt(1, userId.intValue());
      users = usersStmt.executeQuery();
    }
    catch (Exception e)
    {
      printError("Failed to execute Query for user.", sp);
      printException(e, sp);
      closeConnection(usersStmt, conn);
      sp.printHTMLfooter();
      return;
    }
    try
    {
        if (!users.first() && users_aux.contains(userId.intValue()))
        {
          printError("This user does not exist!", sp);
          closeConnection(usersStmt, conn);
          sp.printHTMLfooter();
          return;
        } else if (users.first()/* && verifyCacheUsers(users, sp)*/) {
            //sp.printHTML("Successfully fetched users from cache!");
            fromCache = true;
            users.next();
        }
        else {
            sp.printHTML("Fetching users from database");

            users_aux.add(userId.intValue());
                        
            usersStmt = conn.prepareStatement("SELECT id, firstname, lastname, nickname, email, rating, creation_date FROM users WHERE id=?",
                  ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
            usersStmt.setInt(1, userId.intValue());
            users = usersStmt.executeQuery();
            
            if (!users.first())
            {
              printError("This user does not exist!", sp);
              closeConnection(usersStmt, conn);
              sp.printHTMLfooter();
              return;
            }

        }

        // Try to find the comment corresponding to the user
        try
        {
          if (fromCache) {
            commentsStmt = getCache().prepareStatement("SELECT * FROM comments WHERE to_user_id=?",
                    ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
            commentsStmt.setInt(1, userId.intValue());
            comments = commentsStmt.executeQuery();
          }
        }
        catch (Exception e)
        {
          printError("Failed to execute Query for list of comments.", sp);
          printException(e, sp);
          usersStmt.close();
          closeConnection(commentsStmt, conn);
          sp.printHTMLfooter();
          return;
        }
        
        if (comments != null && !comments.first() && comments_aux.contains(userId.intValue()))
        {
            
          commentsStmt.close();
          comments.close();
          comments = null;
          
          //sp.printHTML("<h3>There is no comment yet for this user.</h3><br>");
          //conn.commit();
          //usersStmt.close();
          //closeConnection(commentsStmt, conn);
          //sp.printHTMLfooter();
          //return;
        }
        else if (comments != null && comments.first() && verifyCache(users, comments, sp)) {
            sp.printHTML("Successfully fetched from cache!");
            //users.next();
        }
        else {
            sp.printHTML("Fetching comments from database");
            fromCache = false;

            comments_aux.add(userId.intValue());

            commentsStmt = conn.prepareStatement("SELECT comments.id, comments.from_user_id, comments.to_user_id, comments.item_id, comments.rating, comments.date, comments.comment, users.nickname FROM comments,users WHERE comments.from_user_id=users.id AND to_user_id=?",
                  ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
            commentsStmt.setInt(1, userId.intValue());
            comments = commentsStmt.executeQuery();

        }

        users.beforeFirst();
        if (comments != null) comments.beforeFirst();
        
        users.next();
        
        String firstname = users.getString("firstname");
        String lastname = users.getString("lastname");
        String nickname = users.getString("nickname");
        String email = users.getString("email");
        Timestamp date = users.getTimestamp("creation_date");
        int rating = users.getInt("rating");
        
        String result = new String();

        result = result + "<h2>Information about " + nickname + "<br></h2>";
        result = result + "Real life name : " + firstname + " " + lastname
            + "<br>";
        result = result + "Email address  : " + email + "<br>";
        result = result + "User since     : " + date.toString() + "<br>";
        result = result + "Current rating : <b>" + rating + "</b><br>";
        sp.printHTML(result);

        if (comments != null) commentList(comments, commentsStmt, conn, sp);
        else sp.printHTML("<h3>There is no comment yet for this user.</h3><br>");
      
        if (!fromCache)
        {

          //PreparedStatement sellerStmt = null;
          try
          {         

            conn.commit();

            // get certificates
            TreeCertificate[] cert  = ((BFTPreparedStatement) usersStmt).getCertificates(); 

            storeSignatures(cert);
            
            storeBranches(new Timestamp(cert[0].getTimestamp()), users, 0);
            
            usersStmt.close();

            String sql = "INSERT INTO users VALUES ("
                      + userId + ","
                      + "'" + firstname + "',"
                      + "'" + lastname + "',"
                      + "'" + nickname + "',"
                      + "'" + email + "',"
                      + rating + ","
                      + "?,"
                      + "?,"
                      + "0,"
                      + "0)";

            PreparedStatement cache = getCache().prepareStatement(sql);
            cache.setTimestamp(1, date);
            cache.setTimestamp(2, new Timestamp(cert[0].getTimestamp()));

            cache.executeUpdate();
            cache.close();

            storeBranches(new Timestamp(cert[0].getTimestamp()), comments, 1);

            while (comments != null && comments.next()) {
                //store in cache
                sql = "INSERT INTO comments VALUES ("
                      + comments.getInt("id") + ","
                      + "'" + comments.getString("nickname") + "',"
                      + comments.getInt("from_user_id") + ","
                      + comments.getInt("to_user_id") + ","
                      + comments.getInt("item_id") + ","
                      + comments.getInt("rating") + ","
                      + "?,"
                      + "'" + comments.getString("comment") + "',"
                      + "?,"
                      + (comments.getRow() - 1) + ","
                      + "1)";

                cache = getCache().prepareStatement(sql);
                cache.setTimestamp(1, comments.getTimestamp("date"));
                cache.setTimestamp(2, new Timestamp(cert[0].getTimestamp()));

                cache.executeUpdate();
                cache.close();
            }

          }
          catch (Exception e)
          {
            printError("Error while storing cache.", sp);
            printException(e, sp);
            sp.printHTMLfooter();
            //sellerStmt.close();
            usersStmt.close();
            closeConnection(commentsStmt, conn);
          }
        }
        
    }
    catch (Exception e)
    {
      printError("Failed to get general information about the user.", sp);
      printException(e, sp);
      closeConnection(usersStmt, conn);
      sp.printHTMLfooter();
      return;
    }
    closeConnection(commentsStmt, conn);
    sp.printHTMLfooter();
  }

  /**
   * Clean up the connection pool.
   */
  public void destroy()
  {
    super.destroy();
  }

}
