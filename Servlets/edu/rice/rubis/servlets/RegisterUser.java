package edu.rice.rubis.servlets;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
//import javax.transaction.UserTransaction;

/** 
 * Add a new user in the database 
 * @author <a href="mailto:cecchet@rice.edu">Emmanuel Cecchet</a> and <a href="mailto:julie.marguerite@inrialpes.fr">Julie Marguerite</a>
 * @version 1.0
 */
public class RegisterUser extends RubisHttpServlet
{
  //private UserTransaction utx = null;
  

  public int getPoolSize()
  {
    return Config.RegisterUserPoolSize;
  }

  public void doGet(HttpServletRequest request, HttpServletResponse response)
    throws IOException, ServletException
  {
    PreparedStatement stmt = null;
    Connection conn = null;
    
    String firstname = null,
      lastname = null,
      nickname = null,
      email = null,
      password = null;
    int regionId;
    int userId;
    String creationDate, region;

    ServletPrinter sp = null;
    sp = new ServletPrinter(response, "RegisterUser");
    sp.printHTMLheader("RUBiS: Register user");
    
    String value = request.getParameter("firstname");
    if ((value == null) || (value.equals("")))
    {
      printError("You must provide a first name!", sp);
      sp.printHTMLfooter();
      return;
    }
    else
      firstname = value;

    value = request.getParameter("lastname");
    if ((value == null) || (value.equals("")))
    {
      printError("You must provide a last name!", sp);
      sp.printHTMLfooter();
      return;
    }
    else
      lastname = value;

    value = request.getParameter("nickname");
    if ((value == null) || (value.equals("")))
    {
      printError("You must provide a nick name!", sp);
      sp.printHTMLfooter();
      return;
    }
    else
      nickname = value;

    value = request.getParameter("email");
    if ((value == null) || (value.equals("")))
    {
      printError("You must provide an email address!", sp);
      sp.printHTMLfooter();
      return;
    }
    else
      email = value;

    value = request.getParameter("password");
    if ((value == null) || (value.equals("")))
    {
      printError("You must provide a password!", sp);
      sp.printHTMLfooter();
      return;
    }
    else
      password = value;

    value = request.getParameter("region");
    if ((value == null) || (value.equals("")))
    {
      printError("You must provide a valid region!", sp);
      sp.printHTMLfooter();
      return;
    }
    else
    {
      region = value;
      
      try
      {
        conn = getConnection();
        stmt = conn.prepareStatement("SELECT id FROM regions WHERE name=?", ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        stmt.setString(1, region);
        ResultSet rs = stmt.executeQuery();
        if (!rs.first())
        {
          printError(" Region " + value + " does not exist in the database!", sp);
          sp.printHTMLfooter();
          closeConnection(stmt, conn);
          return;
        }
        regionId = rs.getInt("id");
        stmt.close();
      }
      catch (Exception e)
      {
        printError("Failed to execute Query for region.", sp);
        printException(e, sp);
        sp.printHTMLfooter();
        closeConnection(stmt, conn);
        return;
      }
    }
    // Try to create a new user
    try
    {
      stmt =
        conn.prepareStatement("SELECT nickname FROM users WHERE nickname=?", ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
      stmt.setString(1, nickname);
      ResultSet rs = stmt.executeQuery();
      if (rs.first())
      {
        printError("The nickname you have choosen is already taken by someone else. Please choose a new nickname.", sp);
        sp.printHTMLfooter();
        closeConnection(stmt, conn);
        return;
      }
      stmt.close();
    }
    catch (Exception e)
    {
      printError("Failed to execute Query to check the nickname.", sp);
      printException(e, sp);
      sp.printHTMLfooter();
      closeConnection(stmt, conn);
      return;
    }
    try
    {
      String now = TimeManagement.currentDateToString();
      stmt =
        conn.prepareStatement(
          "INSERT INTO users (firstname, lastname, nickname, password, email, rating, balance, creation_date, region) VALUES ('"
            + firstname
            + "', '"
            + lastname
            + "', '"
            + nickname
            + "', '"
            + password
            + "', '"
            + email
            + "', 0, 0,'"
            + now
            + "', "
            + regionId
            + ")",
		ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
      stmt.executeUpdate();
      stmt.close();
    }
    catch (Exception e)
    {
      printError("User registration failed.", sp);
      printException(e, sp);
      sp.printHTMLfooter();
      closeConnection(stmt, conn);
      return;
    }
    try
    {
      stmt =
        conn.prepareStatement(
          "SELECT id, creation_date FROM users WHERE nickname=?",
		ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
      stmt.setString(1, nickname);
      ResultSet urs = stmt.executeQuery();
      if (!urs.first())
      {
        printError("This user does not exist in the database.", sp);
        sp.printHTMLfooter();
        closeConnection(stmt, conn);
        return;
      }
      userId = urs.getInt("id");
      creationDate = urs.getString("creation_date");
    }
    catch (Exception e)
    {
      printError("Failed to execute Query for user.", sp);
      printException(e, sp);
      sp.printHTMLfooter();
      closeConnection(stmt, conn);
      return;
    }

    sp.printHTML(
      "<h2>Your registration has been processed successfully</h2><br>");
    sp.printHTML("<h3>Welcome " + nickname + "</h3>");
    sp.printHTML("RUBiS has stored the following information about you:<br>");
    sp.printHTML("First Name : " + firstname + "<br>");
    sp.printHTML("Last Name  : " + lastname + "<br>");
    sp.printHTML("Nick Name  : " + nickname + "<br>");
    sp.printHTML("Email      : " + email + "<br>");
    sp.printHTML("Password   : " + password + "<br>");
    sp.printHTML("Region     : " + region + "<br>");
    sp.printHTML(
      "<br>The following information has been automatically generated by RUBiS:<br>");
    sp.printHTML("User id       :" + userId + "<br>");
    sp.printHTML("Creation date :" + creationDate + "<br>");

    sp.printHTMLfooter();
    closeConnection(stmt, conn);
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
