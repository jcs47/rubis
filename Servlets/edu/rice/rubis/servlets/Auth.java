package edu.rice.rubis.servlets;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class Auth
{

  //private Context servletContext;
  private Connection conn = null;
  private ServletPrinter sp;

  public Auth(Connection connect, ServletPrinter printer)
  {
    conn = connect;
    sp = printer;
  }

  public int authenticate(String name, String password)
  {
    int userId = -1;
    ResultSet rs = null;
    PreparedStatement stmt = null;

    // Lookup the user
    try
    {
      stmt =
        conn.prepareStatement(
          "SELECT users.id FROM users WHERE nickname=? AND password=?", ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
      stmt.setString(1, name);
      stmt.setString(2, password);
      rs = stmt.executeQuery();
      if (!rs.first())
      {
        sp.printHTML(
          " User " + name + " does not exist in the database!<br><br>");
        return userId;
      }
      userId = rs.getInt("id");
    }
    catch (Exception e)
    {
      sp.printHTML("<h3>Your request has not been processed due to the following error:</h3><br>");
      sp.printHTML("<p>Cause: " + e.toString() + "</p>");
      sp.printHTML("<p>Message: " + e.getMessage() + "</p>");
      sp.printHTML("<p>Stacktrace: </p><blockquote>");
      e.printStackTrace(sp.getOut());
      sp.printHTML("</blockquote>");
      return userId;
    }
    finally
    {
      try
      {
        if (stmt != null)
          stmt.close(); // close statement
      }
      catch (Exception ignore)
      {
      }
      return userId;
    }
  }

}
