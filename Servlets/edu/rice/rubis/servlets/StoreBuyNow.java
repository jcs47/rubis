package edu.rice.rubis.servlets;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/** This servlet records a BuyNow in the database and display
 * the result of the transaction.
 * It must be called this way :
 * <pre>
 * http://..../StoreBuyNow?itemId=aa&userId=bb&minBuyNow=cc&maxQty=dd&BuyNow=ee&maxBuyNow=ff&qty=gg 
 *   where: aa is the item id 
 *          bb is the user id
 *          cc is the minimum acceptable BuyNow for this item
 *          dd is the maximum quantity available for this item
 *          ee is the user BuyNow
 *          ff is the maximum BuyNow the user wants
 *          gg is the quantity asked by the user
 * </pre>
 * @author <a href="mailto:cecchet@rice.edu">Emmanuel Cecchet</a> and <a href="mailto:julie.marguerite@inrialpes.fr">Julie Marguerite</a>
 * @version 1.0
 */

public class StoreBuyNow extends RubisHttpServlet
{

  public int getPoolSize()
  {
    return Config.StoreBuyNowPoolSize;
  }

  /**
   * Call the <code>doPost</code> method.
   *
   * @param request a <code>HttpServletRequest</code> value
   * @param response a <code>HttpServletResponse</code> value
   * @exception IOException if an error occurs
   * @exception ServletException if an error occurs
   */
  public void doGet(HttpServletRequest request, HttpServletResponse response)
    throws IOException, ServletException
  {
    doPost(request, response);
  }

  /**
   * Store the BuyNow to the database and display resulting message.
   *
   * @param request a <code>HttpServletRequest</code> value
   * @param response a <code>HttpServletResponse</code> value
   * @exception IOException if an error occurs
   * @exception ServletException if an error occurs
   */
  public void doPost(HttpServletRequest request, HttpServletResponse response)
    throws IOException, ServletException
  {
    Integer userId; // item id
    Integer itemId; // user id
    //     float   minBuyNow; // minimum acceptable BuyNow for this item
    //     float   BuyNow;    // user BuyNow
    //     float   maxBuyNow; // maximum BuyNow the user wants
    int maxQty; // maximum quantity available for this item
    int qty; // quantity asked by the user
    ServletPrinter sp = null;
    PreparedStatement stmt = null;
    Connection conn = null;

    sp = new ServletPrinter(response, "StoreBuyNow");
    sp.printHTMLheader("RUBiS: BuyNow result");
      
    /* Get and check all parameters */

    String value = request.getParameter("userId");
    if ((value == null) || (value.equals("")))
    {
      printError("You must provide a user identifier!", sp);
      sp.printHTMLfooter();
      return;
    }
    else
      userId = new Integer(value);

    value = request.getParameter("itemId");
    if ((value == null) || (value.equals("")))
    {
      printError("You must provide an item identifier!", sp);
      sp.printHTMLfooter();
      return;
    }
    else
      itemId = new Integer(value);

    value = request.getParameter("maxQty");
    if ((value == null) || (value.equals("")))
    {
      printError("You must provide a maximum quantity!", sp);
      sp.printHTMLfooter();
      return;
    }
    else
    {
      Integer foo = new Integer(value);
      maxQty = foo.intValue();
    }

    value = request.getParameter("qty");
    if ((value == null) || (value.equals("")))
    {
      printError("You must provide a quantity!", sp);
      sp.printHTMLfooter();
      return;
    }
    else
    {
      Integer foo = new Integer(value);
      qty = foo.intValue();
    }

    /* Check for invalid values */
    if (qty > maxQty)
    {
      printError(
        "You cannot request "
          + qty
          + " items because only "
          + maxQty
          + " are proposed!", sp);
      sp.printHTMLfooter();
      return;
    }
    String now = TimeManagement.currentDateToString();
    // Try to find the Item corresponding to the Item ID
    try
    {
      int quantity;
      conn = getConnection();
      conn.setAutoCommit(false);
      stmt =
        conn.prepareStatement(
          "SELECT quantity, end_date FROM items WHERE id=?", ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
      stmt.setInt(1, itemId.intValue());
      ResultSet irs = stmt.executeQuery();
      if (!irs.first())
      {
        conn.rollback();
        printError("This item does not exist in the database.", sp);
        sp.printHTMLfooter();
        closeConnection(stmt, conn);
        return;
      }
      quantity = irs.getInt("quantity");
      quantity = quantity - qty;
      stmt.close();
      if (quantity == 0)
      {
        stmt =
          conn.prepareStatement(
            "UPDATE items SET end_date=to_timestamp(?,'YYYY-MM-DD HH24:MI:SS'), quantity=? WHERE id=?", ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        stmt.setString(1, now);
        stmt.setInt(2, quantity);
        stmt.setInt(3, itemId.intValue());
        stmt.executeUpdate();
        stmt.close();
      }
      else
      {
        stmt = conn.prepareStatement("UPDATE items SET quantity=? WHERE id=?", ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        stmt.setInt(1, quantity);
        stmt.setInt(2, itemId.intValue());
        stmt.executeUpdate();
        stmt.close();
      }
    }
    catch (Exception e)
    {
      printError("Failed to execute Query for the item.", sp);
      printException(e, sp);
      sp.printHTMLfooter();
      try
      {
        conn.rollback();
        closeConnection(stmt, conn);
      }
      catch (Exception se)
      {
        printError("Transaction rollback failed.", sp);
        printException(e, sp);
        sp.printHTMLfooter();
        closeConnection(stmt, conn);
      }
      sp.printHTMLfooter();
      return;
    }
    try
    {
      stmt =
        conn.prepareStatement(
          "INSERT INTO buy_now (buyer_id, item_id, qty, date) VALUES ('"
            + userId
            + "', '"
            + itemId
            + "', '"
            + qty
            + "', '"
            + now
            + "')",
		ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
      stmt.executeUpdate();
      
      conn.commit();
      if (qty == 1)
        sp.printHTML(
          "<center><h2>Your have successfully bought this item.</h2></center>\n");
      else
        sp.printHTML(
          "<center><h2>Your have successfully bought these items.</h2></center>\n");
    }
    catch (Exception e)
    {
      printError("Error while storing the BuyNow.", sp);
      printException(e, sp);
      try
      {
        conn.rollback();
        closeConnection(stmt, conn);
      }
      catch (Exception se)
      {
        printError("Transaction rollback failed.", sp);
        printException(e, sp);
      }
      sp.printHTMLfooter();
      return;
    }
    sp.printHTMLfooter();
    closeConnection(stmt, conn);
  }

  /**
  * Clean up the connection pool.
  */
  public void destroy()
  {
    super.destroy();
  }

}
