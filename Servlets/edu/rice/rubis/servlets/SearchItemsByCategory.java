package edu.rice.rubis.servlets;

import java.io.IOException;
import java.net.URLEncoder;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import lasige.steeldb.jdbc.BFTPreparedStatement;
import merkletree.TreeCertificate;

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
      
        
      stmt = getCache().prepareStatement("SELECT COUNT(*) AS total FROM items_aux WHERE category = ? AND first_item >= ? AND last_item <= ?");
      stmt.setInt(1, categoryId.intValue());
      stmt.setInt(2, first);
      stmt.setInt(3, last);
      
      rs = stmt.executeQuery();
      rs.next();
      
      int total = rs.getInt("total");
      
      rs.close();
      stmt.close();
            
      if (total > 0) {
          sp.printHTML("Fetching data from cache...");
                    
          stmt = getCache().prepareStatement("SELECT * FROM items WHERE category= ? AND end_date>= ? ORDER BY end_date ASC { LIMIT ? OFFSET ? }",
                  ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);

      } else {
      
        sp.printHTML("Fetching data from database...");
      
        conn = getConnection();
        //conn.setAutoCommit(false);

        stmt =
          conn.prepareStatement(
            "SELECT items.name, items.id, items.description, items.initial_price, items.quantity, items.reserve_price, items.buy_now, items.nb_of_bids, items.max_bid, items.start_date, items.end_date, items.seller, items.category, users.nickname, users.region FROM items, users WHERE items.category=? AND items.seller=users.id AND end_date>= ? ORDER BY items.end_date ASC LIMIT ? OFFSET ?",
                  ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
      }
      
      Timestamp ts = new Timestamp(System.currentTimeMillis());
      
      stmt.setInt(1, categoryId.intValue());
      stmt.setTimestamp(2, ts);
      stmt.setInt(3, nbOfItems);
      stmt.setInt(4, page * nbOfItems);
      rs = stmt.executeQuery();
                  
      if (total == 0) {
          
           conn.commit();

           // get certificates
           TreeCertificate[] cert  = ((BFTPreparedStatement) stmt).getCertificates(); 
      
           storeSignatures(cert);
           storeBranches(new Timestamp(cert[0].getTimestamp()), rs, 0);

           PreparedStatement cache = getCache().prepareStatement("INSERT INTO items_aux VALUES(?,?,?,?,?)");
      
           cache.setInt(1, first);
           cache.setInt(2, last);
           cache.setInt(3, categoryId.intValue());
           cache.setInt(4, -1);
           cache.setTimestamp(5, ts);
           
           cache.executeUpdate();
           cache.close();
           
          while (rs.next()) {
              
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
                      + rs.getRow() + ","
                      + "0)";
          
              //sp.printHTML("<br>" + sql + "<br>");
              cache = getCache().prepareStatement(sql);
              cache.setTimestamp(1, rs.getTimestamp("start_date"));
              cache.setTimestamp(2, rs.getTimestamp("end_date"));
              cache.setTimestamp(3, new Timestamp(cert[0].getTimestamp()));
              
              cache.executeUpdate();
              cache.close();

          }
          
          rs.beforeFirst();
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
