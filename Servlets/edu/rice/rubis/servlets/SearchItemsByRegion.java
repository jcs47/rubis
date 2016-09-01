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
import merkletree.Leaf;
import merkletree.MerkleTree;
import merkletree.TreeCertificate;
import org.json.JSONArray;

/**
 * Build the html page with the list of all items for given category and region.
 * @author <a href="mailto:cecchet@rice.edu">Emmanuel Cecchet</a> and <a href="mailto:julie.marguerite@inrialpes.fr">Julie Marguerite</a>
 * @version 1.0
 */
public class SearchItemsByRegion extends RubisHttpServlet
{

  public int getPoolSize()
  {
    return Config.SearchItemsByRegionPoolSize;
  }
  private void dropTables(String id) throws ClassNotFoundException, IOException {
      try {
          Statement s = getRepository().createStatement();
          s.executeUpdate("DROP TABLE items" + id);
          s.close();
      } catch (Exception ex) {
          //Logger.getLogger(BrowseCategories.class.getName()).log(Level.SEVERE, null, ex);
      }
      try {
          Statement s = getRepository().createStatement();
          s.executeUpdate("DROP TABLE leafHashes" + id);
          s.close();
      } catch (Exception ex) {
          //Logger.getLogger(BrowseCategories.class.getName()).log(Level.SEVERE, null, ex);
      }
      try {
          Statement s = getRepository().createStatement();
          s.executeUpdate("DROP TABLE signatures" + id);
          s.close();
      } catch (Exception ex) {
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
      
     getRepository().setAutoCommit(false);
     
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
     
     getCache().setAutoCommit(false);
     
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
     
     getCache().commit();
     getCache().setAutoCommit(true);
     
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
          getRepository().commit();
          getRepository().setAutoCommit(true);
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
     getRepository().commit();
     getRepository().setAutoCommit(true);
          
     return count > 2*RubisHttpServlet.F;
      
    } catch (Exception ex) {
        printError("Error while verifying cache.", sp);
        printException(ex, sp);
        try {
            dropTables(id);
            getRepository().commit();
            getRepository().setAutoCommit(true);
        } catch (Exception ex1) {
            printError("Error while dropping cache auxiliar tables.", sp);
            printException(ex, sp);
        }
                
        return false;    
    }    
  
  }

  /** List items in the given category for the given region */
  private void itemList(
    Integer categoryId,
    Integer regionId,
    int page,
    int nbOfItems,
    ServletPrinter sp)
  {
    String itemName, endDate;
    int itemId, nbOfBids = 0;
    float maxBid;
    ResultSet rs = null;
    PreparedStatement stmt = null;
    Connection conn = null;

    // get the list of items
    try
    {

      //int first = page * nbOfItems;
      //int last = first + nbOfItems;
      
      getRepository().setAutoCommit(true);
        
      stmt = getRepository().prepareStatement("SELECT COUNT(*) AS total FROM items_aux WHERE region = ? AND category = ? AND page = ? AND nbOfItems = ?");
      stmt.setInt(1, regionId);
      stmt.setInt(2, categoryId);
      stmt.setInt(3, page);
      stmt.setInt(4, nbOfItems);
      
      rs = stmt.executeQuery();
      rs.next();
      
      int total = rs.getInt("total");
      boolean fromCache = false;
      
      rs.close();
      stmt.close();
      
      if (total > 0) {
          
          stmt = getRepository().prepareStatement("SELECT timestamp FROM items_aux WHERE region = ? AND category = ? AND page = ? AND nbOfItems = ?");
          
          stmt.setInt(1, regionId);
          stmt.setInt(2, categoryId);
          stmt.setInt(3, page);
          stmt.setInt(4, nbOfItems);
      
          rs = stmt.executeQuery();
          rs.next();
      
          Timestamp ts = rs.getTimestamp("timestamp");
                    
          rs.close();
          stmt.close();
          
          stmt = getCache().prepareStatement("SELECT * FROM items WHERE timestamp = ? AND region = ? ORDER BY position ASC",
                  ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
          stmt.setTimestamp(1, ts);
          stmt.setInt(2, regionId);
          
          rs = stmt.executeQuery();
      
          fromCache = verifyCache(rs, sp);
      
      }
      
      if (!fromCache) {
          
          rs.close();
          stmt.close();
          
          sp.printHTML("Fetching data from database...");
      
          conn = getConnection();
          stmt = conn.prepareStatement("SELECT items.name, items.id, items.description, items.initial_price, items.quantity, items.reserve_price, items.buy_now, items.nb_of_bids, items.max_bid, items.start_date, items.end_date, items.seller, items.category, users.nickname, users.region FROM items,users WHERE items.seller=users.id AND users.region= ? AND items.category= ? AND end_date>=? ORDER BY items.end_date ASC LIMIT ? OFFSET ?",
                  ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
          
          Timestamp ts = new Timestamp(System.currentTimeMillis());

          stmt.setInt(1, regionId.intValue());
          stmt.setInt(2, categoryId.intValue());
          stmt.setTimestamp(3, ts);
          stmt.setInt(4, nbOfItems);
          stmt.setInt(5, page * nbOfItems);
          rs = stmt.executeQuery();
          
          stmt.close();
               
          conn.commit();
      
          if (total == 0) {
              // get certificates
              TreeCertificate[] cert  = ((BFTPreparedStatement) stmt).getCertificates(); 

              ts = new Timestamp(cert[0].getTimestamp());
               
              storeSignatures(cert);
              storeLeafHashes(new Timestamp(cert[0].getTimestamp()), rs, 0, null);

              PreparedStatement cache = getRepository().prepareStatement("INSERT INTO items_aux VALUES(?,?,?,?,?)");

              cache.setInt(1, page);
              cache.setInt(2, nbOfItems);
              cache.setInt(3, categoryId.intValue());
              cache.setInt(4, regionId.intValue());
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
                    cache.setTimestamp(3, ts);

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
      printError("Failed to execute Query for items in region.", sp);
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
            "<h3>Sorry, but there is no items in this category for this region.</h3><br>");
        }
        else
        {
          sp.printHTML(
            "<h3>Sorry, but there is no more items in this category for this region.</h3><br>");
          sp.printItemHeader();
          sp.printItemFooter(
            "<a href=\"/rubis_servlets/servlet/edu.rice.rubis.servlets.SearchItemsByRegion?category="
              + categoryId
              + "&region="
              + regionId
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
          "<a href=\"/rubis_servlets/servlet/edu.rice.rubis.servlets.SearchItemsByRegion?category="
            + categoryId
            + "&region="
            + regionId
            + "&page="
            + (page + 1)
            + "&nbOfItems="
            + nbOfItems
            + "\">Next page</a>");
      }
      else
      {
        sp.printItemFooter(
          "<a href=\"/rubis_servlets/servlet/edu.rice.rubis.servlets.SearchItemsByRegion?category="
            + categoryId
            + "&region="
            + regionId
            + "&page="
            + (page - 1)
            + "&nbOfItems="
            + nbOfItems
            + "\">Previous page</a>",
          "<a href=\"/rubis_servlets/servlet/edu.rice.rubis.servlets.SearchItemsByRegion?category="
            + categoryId
            + "&region="
            + regionId
            + "&page="
            + (page + 1)
            + "&nbOfItems="
            + nbOfItems
            + "\">Next page</a>");
      }
      closeConnection(stmt, conn);
    }
    catch (Exception e)
    {
      printError("Exception getting item list.", sp);
      printException(e, sp);
      closeConnection(stmt, conn);
    }
  }

  /* Read the parameters, lookup the remote category and region  and build the web page with
     the list of items */
  public void doGet(HttpServletRequest request, HttpServletResponse response)
    throws IOException, ServletException
  {
    Integer categoryId, regionId;
    Integer page;
    Integer nbOfItems;

    ServletPrinter sp = null;
    sp = new ServletPrinter(response, "SearchItemsByRegion");
    sp.printHTMLheader("RUBiS: Search items by region");

    String value = request.getParameter("category");
    if ((value == null) || (value.equals("")))
    {
      printError("You must provide a category!", sp);
      sp.printHTMLfooter();
      return;
    }
    else
      categoryId = new Integer(value);

    value = request.getParameter("region");
    if ((value == null) || (value.equals("")))
    {
      printError("You must provide a region!", sp);
      sp.printHTMLfooter();
      return;
    }
    else
      regionId = new Integer(value);

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

    itemList(categoryId, regionId, page.intValue(), nbOfItems.intValue(), sp);
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
