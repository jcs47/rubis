package edu.rice.rubis.servlets;

import bftsmart.tom.util.TOMUtil;
import static edu.rice.rubis.servlets.RubisHttpServlet.getCache;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.LinkedList;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import lasige.steeldb.jdbc.BFTPreparedStatement;
import lasige.steeldb.jdbc.ResultSetData;
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
    boolean fromCache = false;
    
    // get the list of regions
    try
    {
      conn = getConnection();
      
      stmt = getCache().prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
      rs = stmt.executeQuery();
      
      if (rs.first()) {
          sp.printHTML("Successfully fetched from cache!");
          fromCache = true;
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
      
      if (fromCache) { // temporary code
          
          stmt = getCache().prepareStatement("SELECT name, id from regions", ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
          rs = stmt.executeQuery();
          
          sp.printHTML("<h3>Re-creating first branches from result set fetched from cache (table 'regions')</h3>");
          JSONArray json = TreeCertificate.getJSON((new ResultSetData(rs)).getRows());
          //sp.printHTML("<p>JSON format: " + json + "</p>");
          //sp.printHTML("<p>Branches:</p>");

          MerkleTree[] branches = TreeCertificate.getFirstLevel(TreeCertificate.jsonToLeafs(json));
          
          for (MerkleTree b : branches) {
              sp.printHTML("<p>" + org.apache.catalina.tribes.util.Arrays.toString(b.digest()) + "</p>");
          }
          
          sp.printHTML("<h3>Getting first branches from cache (table 'branches')</h3>");
          
          stmt = getCache().prepareStatement("SELECT * from regions", ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
          rs = stmt.executeQuery();
          
          rs.next();
          long ts = rs.getLong("timestamp");
          
          stmt = getCache().prepareStatement("SELECT * from branches WHERE timestamp = " +  ts + " AND index = 0 ORDER BY position", ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
          rs = stmt.executeQuery();
          
          rs.last();
          branches = new MerkleTree[rs.getRow()];
          rs.beforeFirst();
          
          int position = 0;
          while (rs.next()) {
              byte[] data = rs.getBytes("value");
              sp.printHTML("<p>" + org.apache.catalina.tribes.util.Arrays.toString(data) + "</p>");
              branches[position] = new MerkleTree(null, data);
              position++;
          }
          
          while (branches.length > 1)
              branches = TreeCertificate.getNextLevel(branches);
          
          sp.printHTML("<h3>MerkleTree root:</h3>");
          sp.printHTML("<p>" + org.apache.catalina.tribes.util.Arrays.toString(branches[0].digest())  + "</p>");
          
          sp.printHTML("<h3>Verifiyng signatures:</h3>");
          
          LinkedList<byte[]> l = new LinkedList<>();
          for (MerkleTree b : branches) {
              l.add(b.digest());
          }
          

          stmt = getCache().prepareStatement("SELECT * FROM signatures WHERE timestamp = " + ts, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
          rs = stmt.executeQuery();
          while (rs.next()) {
          
            byte[] buffer = TreeCertificate.concatenate(rs.getInt("replica"), ts, l);
            boolean verify = TOMUtil.verifySignature(getReplicaKey(rs.getInt("replica")), buffer, rs.getBytes("value"));
            //sp.printHTML("<p>Concatenation from replica " + rs.getInt("replica") + ": " + Arrays.toString(buffer) + "</p>");
            
            //sp.printHTML("<p>Signature for replica " + rs.getInt("replica") + ": " + Arrays.toString(rs.getBytes("value")) + "</p>");
            
            sp.printHTML("<p>Verified: " + rs.getInt("replica") + ": " + verify + "</p>");
            
          }
      }
      
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
