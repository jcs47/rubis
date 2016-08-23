/*
 * This class is used to initialize the cache and SteelDB upon start-up
 */
package edu.rice.rubis.servlets;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

/**
 *
 * @author joao
 */
public class Listener implements ServletContextListener {

    @Override
    public void contextInitialized(ServletContextEvent sce) {

        // This is necessary for SteelDB
        System.setProperty("divdb.folder", "/home/joao/Desktop/git/steeldb/config");

        try {
            // initialize properties
            RubisHttpServlet.initProperties();
            
            // load the driver
            Class.forName(RubisHttpServlet.getDBProperties().getProperty("datasource.classname"));
            
            // load cache and pre-fetch some tables
            RubisHttpServlet.loadCache();

        } catch (Exception ex) {
            ex.printStackTrace();
        }
        
    }

    @Override
    public void contextDestroyed(ServletContextEvent sce) {
        //nothing to do
    }
    
}
