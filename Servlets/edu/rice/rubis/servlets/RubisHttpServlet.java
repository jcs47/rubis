/*
 * RUBiS
 * Copyright (C) 2002, 2003, 2004 French National Institute For Research In Computer
 * Science And Control (INRIA).
 * Contact: jmob@objectweb.org
 * 
 * This library is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as published by the
 * Free Software Foundation; either version 2.1 of the License, or any later
 * version.
 * 
 * This library is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License
 * for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public License
 * along with this library; if not, write to the Free Software Foundation,
 * Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA.
 *
 * Initial developer(s): Emmanuel Cecchet, Julie Marguerite
 * Contributor(s): Jeremy Philippe
 */
package edu.rice.rubis.servlets;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.EmptyStackException;
import java.util.Properties;
import java.util.Stack;

import javax.servlet.ServletException;
import javax.servlet.UnavailableException;
import javax.servlet.http.HttpServlet;
import lasige.steeldb.jdbc.BFTPreparedStatement;
import merkletree.TreeCertificate;

/**
 * Provides the method to initialize connection to the database. All the
 * servlets inherit from this class
 */
public abstract class RubisHttpServlet extends HttpServlet
{
  /** Controls connection pooling */
  private static final boolean enablePooling = true;
  /** Stack of available connections (pool) */
  private Stack      freeConnections = null;
  private int        poolSize;
  private static Properties dbProperties    = null;

  public abstract int getPoolSize(); // Get the pool size for this class

  /** Load the driver and get a connection to the database */
  @Override
  public void init() throws ServletException
  {

    poolSize = getPoolSize();
    try
    {
      // Get the properties for the database connection
      initProperties();
      
      freeConnections = new Stack();
      initializeConnections();
    }
    catch (FileNotFoundException f)
    {
      throw new UnavailableException("Couldn't find file mysql.properties: "
          + f + "<br>");
    }
    catch (IOException io)
    {
      throw new UnavailableException("Cannot open read mysql.properties: " + io
          + "<br>");
    }
    catch (ClassNotFoundException c)
    {
      throw new UnavailableException("Couldn't load database driver: " + c
          + "<br>");
    }
    catch (SQLException s)
    {
      throw new UnavailableException("Couldn't get database connection: " + s
          + "<br>");
    }
  }

  /**
   * Initialize the pool of connections to the database. The caller must ensure
   * that the driver has already been loaded else an exception will be thrown.
   * 
   * @exception SQLException if an error occurs
   */
  public synchronized void initializeConnections() throws SQLException
  {
    if (enablePooling)
    for (int i = 0; i < poolSize; i++)
    {
      // Get connections to the database
      freeConnections.push(
      DriverManager.getConnection(
      dbProperties.getProperty("datasource.url"),
      dbProperties.getProperty("datasource.username"),
      dbProperties.getProperty("datasource.password")));
    }
    
  }

  /**
   * Closes a <code>Connection</code>.
   * 
   * @param connection to close
   */
  private void closeConnection(Connection connection)
  {
    try
    {
      connection.close();
    }
    catch (Exception e)
    {

    }
  }

  /**
   * Gets a connection from the pool (round-robin)
   * 
   * @return a <code>Connection</code> or null if no connection is available
   */
  public synchronized Connection getConnection()
  {
    if (enablePooling)
    {
      try
      {
        // Wait for a connection to be available
        while (freeConnections.isEmpty())
        {
          try
          {
            wait();
          }
          catch (InterruptedException e)
          {
           System.out.println("Connection pool wait interrupted.");
          }
         }
      

        Connection c = (Connection) freeConnections.pop();
        c.setAutoCommit(false); // reset connection status
        return c;
      }
      catch (Exception e)
      {
        e.printStackTrace(System.out);
        return null;
      }
    }
     else
     {
       try
       {
        return DriverManager.getConnection(
        dbProperties.getProperty("datasource.url"),
        dbProperties.getProperty("datasource.username"),
        dbProperties.getProperty("datasource.password"));
       } 
       catch (Exception ex) 
       {
           
        System.out.println("SQLException: " + ex.getMessage());
        return null; 
       }
     }
  }

  /**
   * Releases a connection to the pool.
   * 
   * @param c the connection to release
   */
  private synchronized void releaseConnection(Connection c)
  {  
    if (enablePooling)
    {
      boolean mustNotify = freeConnections.isEmpty();
      freeConnections.push(c);
      // Wake up one servlet waiting for a connection (if any)
      if (mustNotify)
      notifyAll();
    }
    else
    {
      closeConnection(c);
    }
    
  }

  /**
   * Release all the connections to the database.
   * 
   * @exception SQLException if an error occurs
   */
  public synchronized void finalizeConnections() throws SQLException
  {
    if (enablePooling)
    {
      Connection c = null;
      while (!freeConnections.isEmpty())
      {
      c = (Connection) freeConnections.pop();
      c.close();
      }
    }   
  }

  /**
   * Clean up database connections.
   */
  public void destroy()
  {
    try
    {
      finalizeConnections();
    }
    catch (Exception e)
    {
    }
  }

  /**
    * Display an error message.
    * @param errorMsg the error message value
    */
    public void printError(String errorMsg, ServletPrinter sp)
    {
        sp.printHTML("<h2>Your request has not been processed due to the following error:</h2>");
        sp.printHTML("<h3>" + errorMsg + "</h3>");
    
  }
    
    public void printException(Exception e, ServletPrinter sp) {
      sp.printHTML("<p>Cause: " + e.toString() + "</p>");
      sp.printHTML("<p>Message: " + e.getMessage() + "</p>");
      sp.printHTML("<p>Stacktrace: </p><blockquote>");
      e.printStackTrace(sp.getOut());
      sp.printHTML("</blockquote>");
    }

/**
 * Close both statement and connection.
 */
  public void closeConnection(PreparedStatement stmt, Connection conn)
    {
    try
    {
      if (stmt != null)
        stmt.close(); // close statement
      if (conn != null) {
          
        conn.commit();
        releaseConnection(conn);
        
      }
    }
     catch (Exception e)
    {
        System.out.println("[RubisHttpServlet.closeConnection] Exception:"); 
        e.printStackTrace(System.out);
    }
  }
  
  private static Connection cache = null; 
  
  public static Properties getDBProperties() {
      return dbProperties;
  }
  
  public static void initProperties() throws FileNotFoundException, IOException, ClassNotFoundException {

      if (dbProperties == null) {
      
        InputStream in = null;

        dbProperties = new Properties();
        in = new FileInputStream(Config.DatabaseProperties);
        dbProperties.load(in);
        
        in.close();
      }
  }
  
  public static Connection getCache() throws SQLException, ClassNotFoundException, IOException {
      if (cache == null) {

          loadCache();
      }
      return cache;
  }
  
  public static void loadCache() throws SQLException, ClassNotFoundException, IOException {
      
      initProperties(); //initialize main db properties
                
      if (cache == null) {
          
          //create cache database
          Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
          cache = DriverManager.getConnection("jdbc:derby:memory:myCache;create=true");

          // fetch connection with main database
          Connection db = DriverManager.getConnection(
            dbProperties.getProperty("datasource.url"),
            dbProperties.getProperty("datasource.username"),
            dbProperties.getProperty("datasource.password"));

          db.setAutoCommit(false);
          
          // create tables for certificates
          Statement s = cache.createStatement();
          s.executeUpdate("CREATE TABLE roots (timestamp BIGINT, replica INT, index INT, value VARCHAR (20) FOR BIT DATA NOT NULL)");
          s.close();
          
          s = cache.createStatement();
          s.executeUpdate("CREATE TABLE signatures (timestamp BIGINT, replica INT, value VARCHAR (128) FOR BIT DATA NOT NULL)");
          s.close();
          
          // create pre-fetched tables for categories
          s = cache.createStatement();
          s.executeUpdate("CREATE TABLE categories (id INT, name VARCHAR(50), certificate BIGINT, index INT)");
          s.close();

          PreparedStatement stmt = db.prepareStatement("SELECT name, id FROM categories", ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
          ResultSet rs = stmt.executeQuery();
        
          db.commit(); // commit and received certificates
          
          // store certificates for categories
          TreeCertificate[] cert  = ((BFTPreparedStatement) stmt).getCertificates();          

           if (cert != null) {

              for (TreeCertificate c : cert) {

                  if (c != null) {
                      
                      stmt = cache.prepareStatement("INSERT INTO signatures VALUES (" + c.getTimestamp() + "," + c.getId() + ",?)");
                      stmt.setBytes(1, c.getSignature());
                      stmt.executeUpdate();
                      stmt.close();

                      int index = 0;
                      for (byte[] r : c.getRoots()) {
                      
                        stmt = cache.prepareStatement("INSERT INTO roots VALUES (" + c.getTimestamp() + "," + c.getId() + "," + index + ",?)");
                        stmt.setBytes(1, r);
                        stmt.executeUpdate();
                        stmt.close();
                        
                        index++;
                      }
                  }
              }
           }
           
          // store rows for categories in cache
          String categoryName;
          int categoryId, index = 0;
          
          while (rs.next() && cert != null && cert.length > 0)
          {
            categoryName = rs.getString("name");
            categoryId = rs.getInt("id");

            s = cache.createStatement();
            s.executeUpdate("INSERT INTO categories VALUES (" + categoryId + ",'" + categoryName + "'," + cert[0].getTimestamp() + "," + index + ")");
            s.close();
            
            index++;
          }
      
          // create pre-fetched tables for regions
          s = cache.createStatement();
          s.executeUpdate("CREATE TABLE regions (id INT, name VARCHAR(25), certificate BIGINT, index INT)");
          s.close();

          stmt = db.prepareStatement("SELECT name, id FROM regions", ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
          rs = stmt.executeQuery();
        
          db.commit(); // commit and received certificates
          
          // store certificates for regions
          cert  = ((BFTPreparedStatement) stmt).getCertificates();
          

           if (cert != null) {

              for (TreeCertificate c : cert) {

                  if (c != null) {
                      
                      stmt = cache.prepareStatement("INSERT INTO signatures VALUES (" + c.getTimestamp() + "," + c.getId() + ",?)");
                      stmt.setBytes(1, c.getSignature());
                      stmt.executeUpdate();
                      stmt.close();

                      index = 0;
                      for (byte[] r : c.getRoots()) {
                      
                        stmt = cache.prepareStatement("INSERT INTO roots VALUES (" + c.getTimestamp() + "," + c.getId() + "," + index + ",?)");
                        stmt.setBytes(1, r);
                        stmt.executeUpdate();
                        stmt.close();
                        
                        index++;
                      }
                  }
              }
           }
           
          // store rows for regions in cache
          index = 0;
          
          while (rs.next() && cert != null && cert.length > 0)
          {
            categoryName = rs.getString("name");
            categoryId = rs.getInt("id");

            s = cache.createStatement();
            s.executeUpdate("INSERT INTO regions VALUES (" + categoryId + ",'" + categoryName + "'," + cert[0].getTimestamp() + "," + index + ")");
            s.close();
            
            index++;
          }

          db.close();
      }
  }
}