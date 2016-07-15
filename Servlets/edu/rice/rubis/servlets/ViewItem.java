package edu.rice.rubis.servlets;

import static edu.rice.rubis.servlets.RubisHttpServlet.storeSignatures;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import lasige.steeldb.jdbc.BFTPreparedStatement;
import merkletree.TreeCertificate;

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
      
      if (rs.first()) {
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
        conn);
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
