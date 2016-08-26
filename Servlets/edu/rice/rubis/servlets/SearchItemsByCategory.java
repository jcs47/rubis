package edu.rice.rubis.servlets;

import bftsmart.tom.util.TOMUtil;
import static edu.rice.rubis.servlets.RubisHttpServlet.getCache;
import static edu.rice.rubis.servlets.RubisHttpServlet.getRepository;
import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import lasige.steeldb.jdbc.BFTPreparedStatement;
import lasige.steeldb.jdbc.ResultSetData;
import merkletree.Leaf;
import merkletree.MerkleTree;
import merkletree.TreeCertificate;
import org.json.JSONArray;

/** This servlets displays a list of items belonging to a specific category.
 * It must be called this way :
 * <pre>
 * http://..../SearchItemsByCategory?category=xx&categoryName=yy 
 *    where xx is the category id
 *      and yy is the category name
 * /<pre>
 * @author <a href="mailto:cecchet@rice.edu">Emmanuel Cecchet</a> and <a href="mailto:julie.marguerite@inrialpes.fr">Julie Marguerite</a>
 * @version 1.0
 */

public class SearchItemsByCategory extends RubisHttpServlet
{


  public int getPoolSize()
  {
    return Config.SearchItemsByCategoryPoolSize;
  }

  private void dropTables(String id) throws ClassNotFoundException, IOException {
      try {
          Statement s = getRepository().createStatement();
          s.executeUpdate("DROP TABLE items" + id);
          s.close();
      } catch (SQLException ex) {
          //Logger.getLogger(BrowseCategories.class.getName()).log(Level.SEVERE, null, ex);
      }
      try {
          Statement s = getRepository().createStatement();
          s.executeUpdate("DROP TABLE leafHashes" + id);
          s.close();
      } catch (SQLException ex) {
          //Logger.getLogger(BrowseCategories.class.getName()).log(Level.SEVERE, null, ex);
      }
      try {
          Statement s = getRepository().createStatement();
          s.executeUpdate("DROP TABLE signatures" + id);
          s.close();
      } catch (SQLException ex) {
          //Logger.getLogger(BrowseCategories.class.getName()).log(Level.SEVERE, null, ex);
      }
  }

  private boolean verifyCache(ResultSet cachedRS, ServletPrinter sp) {
      
    String id = UUID.randomUUID().toString().replace('-', '_');
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
     dropTables(id);
     
     //sp.printHTML("<p>Tables...</p>");
     s = getRepository().createStatement();
      s.executeUpdate("CREATE TABLE items"+ id + " (" +
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
     s.executeUpdate("CREATE TABLE signatures"+ id + " (timestamp TIMESTAMP, replica INT, value VARCHAR (128) FOR BIT DATA NOT NULL)");
     s.close();
     
     s = getRepository().createStatement();
     s.executeUpdate("CREATE TABLE leafHashes"+ id + " (timestamp TIMESTAMP, position INT, index INT, value VARCHAR (20) FOR BIT DATA NOT NULL)");
     s.close();

     // insert values...
     //sp.printHTML("<p>Values...</p>");
     cachedRS.beforeFirst();
     
     while (cachedRS.next())
     {     

        String sql = "INSERT INTO items"+ id + " VALUES ("
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

      stmt2 = getRepository().prepareStatement("INSERT INTO signatures"+ id + " VALUES (?," + replica + ",?)");
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

      stmt2 = getRepository().prepareStatement("INSERT INTO leafHashes"+ id + " VALUES (?," + position + "," + index + ",?)");
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
     ResultSet items = s.executeQuery("SELECT * FROM items"+ id);
     
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
            
      stmt = getRepository().prepareStatement("SELECT count(*) AS total FROM leafHashes"+ id + " WHERE timestamp = ? AND index = " + items.getInt("index") + " AND position = " +items.getInt("position") + " AND value = ?", ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
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
          dropTables(id);
          return false;
      }

      //sp.printHTML("<p>total number of leaf hashes: " + total + "</p>");
     }
     s.close();
     items.close();
           
     // re-create first level branches from the certificates
     stmt = getRepository().prepareStatement("SELECT * from leafHashes"+ id + " WHERE timestamp = ? AND index = 0 ORDER BY position", ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
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
      
     stmt = getRepository().prepareStatement("SELECT * FROM signatures"+ id + " WHERE timestamp = ?", ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
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
     dropTables(id);
          
     return count > 2*RubisHttpServlet.F;
      
    } catch (Exception ex) {
        printException(ex, sp);
        
        try {
            dropTables(id);
        } catch (Exception ex1) {
            printException(ex, sp);
        }
        
        return false;    
    }    
  
  }
  
  private void itemList(
    Integer categoryId,
    String categoryName,
    int page,
    int nbOfItems,
    ServletPrinter sp)
  {
    
    PreparedStatement stmt = null;
    Connection conn = null;
    
    String itemName, endDate;
    int itemId;
    float maxBid;
    int nbOfBids = 0;
    ResultSet rs = null;

    // get the list of items
    try
    {
        
      int first = page * nbOfItems;
      int last = first + nbOfItems;
      
        
      stmt = getRepository().prepareStatement("SELECT COUNT(*) AS total FROM items_aux WHERE category = ? AND region = -1 AND first_item >= ? AND last_item <= ?");
      stmt.setInt(1, categoryId.intValue());
      stmt.setInt(2, first);
      stmt.setInt(3, last);
      
      rs = stmt.executeQuery();
      rs.next();
      
      int total = rs.getInt("total");
      boolean fromCache = false;
      
      rs.close();
      stmt.close();
            
      if (total > 0) {                    
          stmt = getCache().prepareStatement("SELECT * FROM items WHERE category= ? AND end_date>= ? ORDER BY end_date ASC { LIMIT ? OFFSET ? }",
                  ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);

          Timestamp ts = new Timestamp(System.currentTimeMillis());
      
          stmt.setInt(1, categoryId.intValue());
          stmt.setTimestamp(2, ts);
          stmt.setInt(3, nbOfItems);
          stmt.setInt(4, page * nbOfItems);
          rs = stmt.executeQuery();
      
          fromCache = verifyCache(rs, sp);
          
      }
      
      if (!fromCache) {
      
        sp.printHTML("Fetching data from database...");
      
        conn = getConnection();
        //conn.setAutoCommit(false);

        stmt =
          conn.prepareStatement(
            "SELECT items.name, items.id, items.description, items.initial_price, items.quantity, items.reserve_price, items.buy_now, items.nb_of_bids, items.max_bid, items.start_date, items.end_date, items.seller, items.category, users.nickname, users.region FROM items, users WHERE items.category=? AND items.seller=users.id AND end_date>= ? ORDER BY items.end_date ASC LIMIT ? OFFSET ?",
                  ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);

          Timestamp ts = new Timestamp(System.currentTimeMillis());

          stmt.setInt(1, categoryId.intValue());
          stmt.setTimestamp(2, ts);
          stmt.setInt(3, nbOfItems);
          stmt.setInt(4, page * nbOfItems);
          rs = stmt.executeQuery();
          
          stmt.close();
               
          conn.commit();
          
          if (total == 0) {
               // get certificates
               TreeCertificate[] cert  = ((BFTPreparedStatement) stmt).getCertificates(); 

               storeSignatures(cert);
               storeBranches(new Timestamp(cert[0].getTimestamp()), rs, 0);

               PreparedStatement cache = getRepository().prepareStatement("INSERT INTO items_aux VALUES(?,?,?,?,?)");

               cache.setInt(1, first);
               cache.setInt(2, last);
               cache.setInt(3, categoryId.intValue());
               cache.setInt(4, -1);
               cache.setTimestamp(5, ts);

               cache.executeUpdate();
               cache.close();

               getCache().setAutoCommit(false);
               while (rs.next()) {
                   
                  eraseFromCache("items", "id", rs.getInt("id"), "items_aux");
                  
                  String sql = "INSERT INTO items VALUES ("
                            + rs.getInt("id") + ","
                            + "'" + rs.getString("name") + "',"
                            + "'" + rs.getString("description") + "',"
                            + rs.getFloat("initial_price") + ","
                            + rs.getInt("quantity") + ","
                            + rs.getFloat("reserve_price") + ","
                            + rs.getFloat("buy_now") + ","
                            + rs.getInt("nb_of_bids") + ","
                            + rs.getFloat("max_bid") + ","
                            + "?,"
                            + "?,"
                            + rs.getInt("seller") + ","
                            + "'" + rs.getString("nickname") + "',"
                            + rs.getInt("category") + ","
                            + rs.getInt("region") + ","
                            + "?,"
                            + (rs.getRow() - 1) + ","
                            + "0)";

                  //sp.printHTML("<br>" + sql + "<br>");
                  cache = getCache().prepareStatement(sql);
                  cache.setTimestamp(1, rs.getTimestamp("start_date"));
                  cache.setTimestamp(2, rs.getTimestamp("end_date"));
                  cache.setTimestamp(3, new Timestamp(cert[0].getTimestamp()));

                  cache.executeUpdate();
                  cache.close();

               }

              getCache().commit();
              getCache().setAutoCommit(true);
              rs.beforeFirst();
          }
      } else {
          sp.printHTML("Successfully fetched from cache!");
      }
      
    }
    
    catch (Exception e)
    {
      printError("Failed to executeQuery for item.", sp);
      printException(e, sp);
      closeConnection(stmt, conn);
      return;
    }
    try
    {
      if (!rs.first())
      {
        if (page == 0)
        {
          sp.printHTML(
            "<h2>Sorry, but there are no items available in this category !</h2>");
        }
        else
        {
          sp.printHTML(
            "<h2>Sorry, but there are no more items available in this category !</h2>");
          sp.printItemHeader();
          sp.printItemFooter(
            "<a href=\"/rubis_servlets/servlet/edu.rice.rubis.servlets.SearchItemsByCategory?category="
              + categoryId
              + "&categoryName="
              + URLEncoder.encode(categoryName)
              + "&page="
              + (page - 1)
              + "&nbOfItems="
              + nbOfItems
              + "\">Previous page</a>",
            "");
        }
        closeConnection(stmt, conn);
        return;
      }

      sp.printItemHeader();
      do
      {
        itemName = rs.getString("name");
        itemId = rs.getInt("id");
        endDate = rs.getString("end_date");
        maxBid = rs.getFloat("max_bid");
        nbOfBids = rs.getInt("nb_of_bids");
        float initialPrice = rs.getFloat("initial_price");
        if (maxBid < initialPrice)
          maxBid = initialPrice;
        sp.printItem(itemName, itemId, maxBid, nbOfBids, endDate);
      }
      while (rs.next());
      if (page == 0)
      {
        sp.printItemFooter(
          "",
          "<a href=\"/rubis_servlets/servlet/edu.rice.rubis.servlets.SearchItemsByCategory?category="
            + categoryId
            + "&categoryName="
            + URLEncoder.encode(categoryName)
            + "&page="
            + (page + 1)
            + "&nbOfItems="
            + nbOfItems
            + "\">Next page</a>");
      }
      else
      {
        sp.printItemFooter(
          "<a href=\"/rubis_servlets/servlet/edu.rice.rubis.servlets.SearchItemsByCategory?category="
            + categoryId
            + "&categoryName="
            + URLEncoder.encode(categoryName)
            + "&page="
            + (page - 1)
            + "&nbOfItems="
            + nbOfItems
            + "\">Previous page</a>",
          "<a href=\"/rubis_servlets/servlet/edu.rice.rubis.servlets.SearchItemsByCategory?category="
            + categoryId
            + "&categoryName="
            + URLEncoder.encode(categoryName)
            + "&page="
            + (page + 1)
            + "&nbOfItems="
            + nbOfItems
            + "\">Next page</a>");
      }
      //conn.commit();
      closeConnection(stmt, conn);
    }
    catch (Exception e)
    {
      printError("Exception getting item list.", sp);
      printException(e, sp);
      //       try
      //       {
      //         conn.rollback();
      //       }
      //       catch (Exception se) 
      //       {
      //         printError("Transaction rollback failed: " + e +"<br>");
      //       }
      closeConnection(stmt, conn);
    }
  }

  public void doGet(HttpServletRequest request, HttpServletResponse response)
    throws IOException, ServletException
  {
    Integer page;
    Integer nbOfItems;
    String value = request.getParameter("category");
    ;
    Integer categoryId;
    String categoryName = request.getParameter("categoryName");

    ServletPrinter sp = null;
    sp = new ServletPrinter(response, "SearchItemsByCategory");
    sp.printHTMLheader("RUBiS: Search items by category");

    if ((value == null) || (value.equals("")))
    {
      printError("You must provide a category identifier!", sp);
      sp.printHTMLfooter();
      return;
    }
    else
      categoryId = new Integer(value);

    value = request.getParameter("page");
    if ((value == null) || (value.equals("")))
      page = new Integer(0);
    else
      page = new Integer(value);

    value = request.getParameter("nbOfItems");
    if ((value == null) || (value.equals("")))
      nbOfItems = new Integer(25);
    else
      nbOfItems = new Integer(value);

    if (categoryName == null)
    {
      printError("You must provide a category name!", sp);
    }
    else
    {
      sp.printHTML("<h2>Items in category " + categoryName + "</h2><br><br>");
      itemList(categoryId, categoryName, page.intValue(), nbOfItems.intValue(), sp);
    }
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
