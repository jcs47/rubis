package edu.rice.rubis.servlets;

import bftsmart.tom.util.TOMUtil;
import static edu.rice.rubis.servlets.RubisHttpServlet.getCache;
import static edu.rice.rubis.servlets.RubisHttpServlet.getRepository;
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
import lasige.steeldb.jdbc.ResultSetData;
import merkletree.Leaf;
import merkletree.MerkleTree;
import merkletree.TreeCertificate;
import org.json.JSONArray;

/** Builds the html page with the list of all region in the database */
public class BrowseRegions extends RubisHttpServlet
{
 


  public int getPoolSize()
  {
    return Config.BrowseRegionsPoolSize;
  }

  private void dropTables(String id) throws ClassNotFoundException, IOException {
      try {
          Statement s = getRepository().createStatement();
          s.executeUpdate("DROP TABLE regions" + id);
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
            
     //copy regions, signatures and leafs to local repository
     // this will make verification easier
     
     // tables...
     
     //drop tables used in repository
     dropTables(id);
     
     //sp.printHTML("<p>Tables...</p>");
     s = getRepository().createStatement();
     s.executeUpdate("CREATE TABLE regions"+ id + " (id INT, name VARCHAR(50), timestamp TIMESTAMP, position INT, index INT)");
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
      String regionName = cachedRS.getString("name");
      int regionId = cachedRS.getInt("id");
      int position = cachedRS.getInt("position");
      int index = cachedRS.getInt("index");
      Timestamp t = cachedRS.getTimestamp("timestamp");

      stmt = getRepository().prepareStatement("INSERT INTO regions"+ id + " VALUES (" + regionId + ",'" + regionName + "',?," + position + "," + index + ")");
      stmt.setTimestamp(1, t);
      stmt.executeUpdate();
      stmt.close();

     }
     
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
     
     // Fetch regions
     //sp.printHTML("<p>Fetch regions...</p>");
     s = getRepository().createStatement();
     ResultSet regions = s.executeQuery("SELECT * FROM regions"+ id);
     
     List<byte[]> temp = new LinkedList();
     
     // Check respective leafs
     while (regions.next())
     {
         
      //sp.printHTML("<p>Parsing category " + regions.getString("name") + "</p>");
          
      JSONArray row = new JSONArray();
      List<byte[]> l = new LinkedList<>();

      row.put(1, regions.getObject("name"));
      row.put(2, regions.getObject("id"));            

      l.add(row.get(1).toString().getBytes(StandardCharsets.UTF_8));
      l.add(row.get(2).toString().getBytes(StandardCharsets.UTF_8));

      byte[] b = TreeCertificate.getLeafHashes(new Leaf(l));
      temp.add(b);

      stmt = getRepository().prepareStatement("SELECT count(*) AS total FROM leafHashes"+ id + " WHERE timestamp = ? AND index = " + regions.getInt("index") + " AND position = " +regions.getInt("position") + " AND value = ?", ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
      stmt.setTimestamp(1, regions.getTimestamp("timestamp"));
      stmt.setBytes(2, b);
      rs = stmt.executeQuery();
      rs.next();

      int total = rs.getInt("total");
      if (total == 0 /*&& !Arrays.equals(b, rs.getBytes("value"))*/) {
          s.close();
          regions.close();
          stmt.close();
          rs.close();
          sp.printHTML("<p>Leaf hash not found!</p>");
          
          //drop tables used in repository
          dropTables(id);
          return false;
      }
      stmt.close();
      rs.close();
      //sp.printHTML("<p>total number of leaf hashes: " + total + "</p>");
     }
     s.close();
     regions.close();
           
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
     
     //////////////////////////////////////////////////////////
    /*stmt = getRepository().prepareStatement("SELECT name, id from regions", ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
    rs = stmt.executeQuery();

    JSONArray json = TreeCertificate.getJSON((new ResultSetData(rs)).getRows());

    Leaf[] leafs = TreeCertificate.jsonToLeafs(json);
    
    sp.printHTML("<p>From regions' result set</p>");
    for (Leaf b : leafs) {
        sp.printHTML("<p>" + Arrays.toString(TreeCertificate.getLeafHashes(b)) + "</p>");
    }
    
    branches = TreeCertificate.getFirstLevel(leafs);*/

    
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
  
/**
 * Get the list of regions from the database
 */
  private void regionList(ServletPrinter sp)
  {
    PreparedStatement stmt = null;
    Connection conn = null;
    String regionName;
    ResultSet rs = null;
    
    String sql = "SELECT * FROM regions";
    //boolean fromCache = false;
    
    // get the list of regions
    try
    {
      conn = getConnection();
      
      stmt = getCache().prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
      rs = stmt.executeQuery();
      
      if (verifyCache(rs, sp)) {
          sp.printHTML("Successfully fetched from cache!");
          //fromCache = true;
      }
      else {
          sp.printHTML("Fetching from database");
          stmt = conn.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
          rs = stmt.executeQuery();
      }
    }
    catch (Exception e)
    {
      printError("Failed to executeQuery for the list of regions.", sp);
      printException(e, sp);
      closeConnection(stmt, conn);
      return;
    }
    try
    {
      if (!rs.first())
      {
        printError("Sorry, but there is no region available at this time. Database table is empty.", sp);
        closeConnection(stmt, conn);
        return;
      }
      else
        sp.printHTML("<h2>Currently available regions</h2><br>");

      do
      {
        regionName = rs.getString("name");
        sp.printRegion(regionName);
      }
      while (rs.next());
      
      closeConnection(stmt, conn);
      
    }
    catch (Exception e)
    {
      printError("Exception getting region list.", sp);
      printException(e, sp);
      closeConnection(stmt, conn);
    }
  }

  public void doGet(HttpServletRequest request, HttpServletResponse response)
    throws IOException, ServletException
  {
    ServletPrinter sp = null;
    sp = new ServletPrinter(response, "BrowseRegions");
    sp.printHTMLheader("RUBiS: Available regions");

    regionList(sp);
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
