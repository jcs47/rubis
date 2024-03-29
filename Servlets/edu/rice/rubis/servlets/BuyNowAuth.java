package edu.rice.rubis.servlets;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/** This servlets display the page authentifying the user 
 * to allow him to put a buy an item at the "buy now" price.
 * It must be called this way :
 * <pre>
 * http://..../BuyNowAuth?itemId=xx where xx is the id of the item
 * /<pre>
 * @author <a href="mailto:cecchet@rice.edu">Emmanuel Cecchet</a> and <a href="mailto:julie.marguerite@inrialpes.fr">Julie Marguerite</a>
 * @version 1.0
 */

public class BuyNowAuth extends RubisHttpServlet
{


  /** Display the web page with the form to authenticate the user */
  public void doGet(HttpServletRequest request, HttpServletResponse response)
    throws IOException, ServletException
  {
    ServletPrinter sp = null;
    sp = new ServletPrinter(response, "BuyNowAuth");
    sp.printHTMLheader("RUBiS: User authentification for buying an item");

    String value = request.getParameter("itemId");
    if ((value == null) || (value.equals("")))
    {
      printError("No item identifier received - Cannot process the request.", sp);
      sp.printHTMLfooter();
      return;
    }

    sp.printFile(Config.HTMLFilesPath + "/buy_now_auth_header.html");
    sp.printHTML("<input type=hidden name=\"itemId\" value=\"" + value + "\">");
    sp.printFile(Config.HTMLFilesPath + "/auth_footer.html");
    sp.printHTMLfooter();
  }

  /** Call the doGet method*/
  public void doPost(HttpServletRequest request, HttpServletResponse response)
    throws IOException, ServletException
  {
    doGet(request, response);
  }

    @Override
    public int getPoolSize() {
        return 0;
    }
}
