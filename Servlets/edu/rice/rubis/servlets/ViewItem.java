package edu.rice.rubis.servlets;

import bftsmart.tom.util.TOMUtil;
import static edu.rice.rubis.servlets.RubisHttpServlet.storeSignatures;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
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

/** This servlets displays the full description of a given item
 * and allows the user to bid on this item.
 * It must be called this way :
 * <pre>
 * http://..../ViewItem?itemId=xx where xx is the id of the item
 * /<pre>
 * @author <a href="mailto:cecchet@rice.edu">Emmanuel Cecchet</a> and <a href="mailto:julie.marguerite@inrialpes.fr">Julie Marguerite</a>
 * @version 1.0
 */

public class ViewItem extends RubisHttpServlet
{


  public int getPoolSize()
  {
    return Config.ViewItemPoolSize;
  }
 private void dropTables() throws ClassNotFoundException, IOException {
      try {
          Statement s = getRepository().createStatement();
          s.executeUpdate("DROP TABLE items");
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

  private boolean verifyCache(ResultSet cachedRS, ServletPrinter sp) {
      
    try {
      
      //sp.printHTML("<p>Verifiyng...</p>");
        
      cachedRS.beforeFirst();
      if (!cachedRS.next()) {
          
          sp.printHTML("<p>Empty resultset!</p>");
          return false;
      }
      //sp.printHTML("<p>Verifiyng...</p>");
      
      Timestamp ts = cachedRS.getTimestamp("timestamp");
      PreparedStatement stmt = null, stmt2 = null;
      Statement s = null;
            
     //copy categories, signatures and leafs to local repository
     // this will make verification easier
     
     // tables...
     
     //drop tables used in repository
     dropTables();
     
     //sp.printHTML("<p>Tables...</p>");
     s = getRepository().createStatement();
      s.executeUpdate("CREATE TABLE items (" +
        "   id            INT," +
        "   name          VARCHAR(100)," +
        "   description   LONG VARCHAR," +
        "   initial_price FLOAT NOT NULL," +
        "   quantity      INT NOT NULL," +
        "   reserve_price FLOAT DEFAULT 0," +
        "   buy_now       FLOAT DEFAULT 0," +
        "   nb_of_bids    INT DEFAULT 0," +
        "   max_bid       FLOAT DEFAULT 0," +
        "   start_date    TIMESTAMP," +
        "   end_date      TIMESTAMP," +
        "   seller        INT NOT NULL," +
        "   nickname      VARCHAR(100) NOT NULL," +
        "   category      INT NOT NULL," +
        "   region        INT NOT NULL," +
        "   timestamp     TIMESTAMP," +
        "   position      INT," +
        "   index         INT," +
        "   PRIMARY KEY(id)" +
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
     cachedRS.beforeFirst();
     
     while (cachedRS.next())
     {     

        String sql = "INSERT INTO items VALUES ("
                    + cachedRS.getInt("id") + ","
                    + "'" + cachedRS.getString("name") + "',"
                    + "'" + cachedRS.getString("description") + "',"
                    + cachedRS.getFloat("initial_price") + ","
                    + cachedRS.getInt("quantity") + ","
                    + cachedRS.getFloat("reserve_price") + ","
                    + cachedRS.getFloat("buy_now") + ","
                    + cachedRS.getInt("nb_of_bids") + ","
                    + cachedRS.getFloat("max_bid") + ","
                    + "?,"
                    + "?,"
                    + cachedRS.getInt("seller") + ","
                    + "'" + cachedRS.getString("nickname") + "',"
                    + cachedRS.getInt("category") + ","
                    + cachedRS.getInt("region") + ","
                    + "?,"
                    + cachedRS.getInt("position") + ","
                    + "0)";

        stmt = getRepository().prepareStatement(sql);
        stmt.setTimestamp(1, cachedRS.getTimestamp("start_date"));
        stmt.setTimestamp(2, cachedRS.getTimestamp("end_date"));
        stmt.setTimestamp(3, ts);
        
        stmt.executeUpdate();
        stmt.close();

     }
     
     cachedRS.beforeFirst();
     
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
     ResultSet items = s.executeQuery("SELECT * FROM items");
     
     List<byte[]> temp = new LinkedList();
     
     // Check respective leafs
     while (items.next())
     {
                 
      JSONArray row = new JSONArray();
      List<byte[]> l = new LinkedList<>();

      row.put(1, items.getObject("name"));
      row.put(2, items.getObject("id"));
      row.put(3, items.getObject("description"));
      row.put(4, items.getObject("initial_price"));
      row.put(5, items.getObject("quantity"));
      row.put(6, items.getObject("reserve_price"));
      row.put(7, items.getObject("buy_now"));
      row.put(8, items.getObject("nb_of_bids"));
      row.put(9, items.getObject("max_bid"));
      row.put(10, items.getTimestamp("start_date").getTime());
      row.put(11, items.getTimestamp("end_date").getTime());
      row.put(12, items.getObject("seller"));
      row.put(13, items.getObject("category"));
      row.put(14, items.getObject("nickname"));
      row.put(15, items.getObject("region"));
        
      for (int i = 1; i <= 15; i++) {
            l.add(row.get(i).toString().getBytes(StandardCharsets.UTF_8));
      }
      
      byte[] b = TreeCertificate.getLeafHashes(new Leaf(l));
      temp.add(b);

      //sp.printHTML("hash for item " + items.getObject("nickname") + ": " + Arrays.toString(b));
            
      stmt = getRepository().prepareStatement("SELECT count(*) AS total FROM leafHashes WHERE timestamp = ? AND index = " + items.getInt("index") + " AND position = " +items.getInt("position") + " AND value = ?", ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
      //stmt = getRepository().prepareStatement("SELECT * FROM leafHashes WHERE value = ?", ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);

      stmt.setTimestamp(1, items.getTimestamp("timestamp"));
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
          items.close();

          sp.printHTML("<p>Leaf hash not found!</p>");
          
          //drop tables used in repository
          dropTables();
          return false;
      }

      //sp.printHTML("<p>total number of leaf hashes: " + total + "</p>");
     }
     s.close();
     items.close();
           
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
      
     //sp.printHTML("<p> MerkleTree root:" + Arrays.toString(branches[0].digest())  + "</p>");
      
     //Verifiyng signatures
     LinkedList<byte[]> l = new LinkedList<>();
     for (MerkleTree b : branches) {
         l.add(b.digest());
     }
      
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


  public void doGet(HttpServletRequest request, HttpServletResponse response)
    throws IOException, ServletException
  {
    ServletPrinter sp = null;
    PreparedStatement stmt = null;
    Connection conn = null;
    
    sp = new ServletPrinter(response, "ViewItem");
    sp.printHTMLheader("RUBiS: View item");
    
    ResultSet rs = null;

    String value = request.getParameter("itemId");
    if ((value == null) || (value.equals("")))
    {
      printError("No item identifier received - Cannot process the request.", sp);
      sp.printHTMLfooter();
      return;
    }
    Integer itemId = new Integer(value);    
    boolean fromCache = false;
        
    // get the item
    try
    {
      
      stmt = getCache().prepareStatement("SELECT * FROM items WHERE id= ?", ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
      stmt.setInt(1, itemId.intValue());
      rs = stmt.executeQuery();
      
      if (rs.first() && verifyCache(rs, sp)) {
          sp.printHTML("Successfully fetched from cache!");
          fromCache = true;
      }
      else {
          sp.printHTML("Fetching from database");
          
          conn = getConnection();
          stmt = conn.prepareStatement("SELECT items.name, items.id, items.description, items.initial_price, items.quantity, items.reserve_price, items.buy_now, items.nb_of_bids, items.max_bid, items.start_date, items.end_date, items.seller, items.category, users.nickname, users.region FROM items,users WHERE items.seller=users.id AND id= ?",
		ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
          stmt.setInt(1, itemId.intValue());
          rs = stmt.executeQuery();
      
      }
    }
    catch (Exception e)
    {
      printError("Failed to execute Query for item.", sp);
      printException(e, sp);
      sp.printHTMLfooter();
      closeConnection(stmt, conn);
      return;
    }
    /**
    try
    {
      if (!rs.first())
      {
        stmt.close();
        stmt = conn.prepareStatement("SELECT * FROM old_items WHERE id=?",
		ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        stmt.setInt(1, itemId.intValue());
        rs = stmt.executeQuery();
      }
    }
    catch (Exception e)
    {
      sp.printHTML("Failed to execute Query for item in table old_items: " + e);
      closeConnection(stmt, conn);
      return;
    }
    */
    try
    {
      if (!rs.first())
      {
        printError("This item does not exist!", sp);
        sp.printHTMLfooter();
        closeConnection(stmt, conn);
        return;
      }
      String itemName, endDate, startDate, description, sellerName;
      float maxBid, initialPrice, buyNow, reservePrice;
      int quantity, sellerId, nbOfBids = 0;
      Timestamp ts_start, ts_end;
      
      itemName = rs.getString("name");
      description = rs.getString("description");
      endDate = rs.getString("end_date");
      ts_end = rs.getTimestamp("end_date");
      startDate = rs.getString("start_date");
      ts_start = rs.getTimestamp("start_date");
      initialPrice = rs.getFloat("initial_price");
      reservePrice = rs.getFloat("reserve_price");
      buyNow = rs.getFloat("buy_now");
      quantity = rs.getInt("quantity");
      sellerId = rs.getInt("seller");
      sellerName = rs.getString("nickname");

      maxBid = rs.getFloat("max_bid");
      nbOfBids = rs.getInt("nb_of_bids");
      if (maxBid < initialPrice)
        maxBid = initialPrice;
      
      int category = rs.getInt("category");
      int region = rs.getInt("region");
      int nRow = rs.getRow();
      
      if (!fromCache)
      {
        
        conn.commit();

        // get certificates
        TreeCertificate[] cert  = ((BFTPreparedStatement) stmt).getCertificates(); 

        storeSignatures(cert);
        storeBranches(new Timestamp(cert[0].getTimestamp()), rs, 0);
        
        //PreparedStatement sellerStmt = null;
        try
        {         
          /*sellerStmt =
            conn.prepareStatement("SELECT nickname FROM users WHERE id=?",
                  ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
          sellerStmt.setInt(1, sellerId);
          ResultSet sellerResult = sellerStmt.executeQuery();
          // Get the seller's name		 
          if (sellerResult.first())
            sellerName = sellerResult.getString("nickname");
          else
          {
            sp.printHTML("Unknown seller");
            sellerStmt.close();
            closeConnection(stmt, conn);
            return;
          }
          sellerStmt.close();*/

          //store in cache
            
          getCache().setAutoCommit(false);
          
          eraseFromCache("items", "id", rs.getInt("id"), "items_aux");
          
          String sql = "INSERT INTO items VALUES ("
                + rs.getInt("id") + ","
                + "'" + itemName + "',"
                + "'" + description + "',"
                + initialPrice + ","
                + quantity + ","
                + reservePrice + ","
                + buyNow + ","
                + nbOfBids + ","
                + maxBid + ","
                + "?,"
                + "?,"
                + sellerId + ","
                + "'" + sellerName + "',"
                + category + ","
                + region + ","
                + "?,"
                + nRow + ","
                + "0)";

          PreparedStatement cache = getCache().prepareStatement(sql);
          cache.setTimestamp(1, ts_start);
          cache.setTimestamp(2, ts_end);
          cache.setTimestamp(3, new Timestamp(cert[0].getTimestamp()));

          cache.executeUpdate();
          cache.close();
          
          getCache().commit();
          getCache().setAutoCommit(true);

        }
        catch (Exception e)
        {
          printError("Failed to executeQuery for seller.", sp);
          printException(e, sp);
          sp.printHTMLfooter();
          //sellerStmt.close();
          closeConnection(stmt, conn);
          return;
        }
      }
      
      
      sp.printItemDescription(
        itemId.intValue(),
        itemName,
        description,
        initialPrice,
        reservePrice,
        buyNow,
        quantity,
        maxBid,
        nbOfBids,
        sellerName,
        sellerId,
        startDate,
        endDate,
        -1,
        (fromCache ? null : conn));
    }
    catch (Exception e)
    {
      printError("Exception getting item list.", sp);
      printException(e, sp);
      sp.printHTMLfooter();
      closeConnection(stmt, conn);
      return;
    }
    closeConnection(stmt, conn);
    sp.printHTMLfooter();
  }

  public void doPost(HttpServletRequest request, HttpServletResponse response)
    throws IOException, ServletException
  {
    doGet(request, response);
  }

  /**
  * Clean up the connection pool.
  */
  public void destroy()
  {
    super.destroy();
  }
}
