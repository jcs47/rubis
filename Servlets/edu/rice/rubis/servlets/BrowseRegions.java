package edu.rice.rubis.servlets;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Arrays;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import lasige.steeldb.jdbc.BFTPreparedStatement;
import merkletree.TreeCertificate;

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

    // get the list of regions
    try
    {
      conn = getConnection();

      String sql = "SELECT name, id FROM regions";
      
      stmt = getCache().prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
      rs = stmt.executeQuery();
      
      if (rs.first()) {
          sp.printHTML("Successfully fetched from cache!");
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
