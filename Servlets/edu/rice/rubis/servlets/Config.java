package edu.rice.rubis.servlets;

/** This class contains the configuration for the servlets
 * like the path of HTML files, etc ...
 * @author <a href="mailto:cecchet@rice.edu">Emmanuel Cecchet</a> and <a href="mailto:julie.marguerite@inrialpes.fr">Julie Marguerite</a>
 * @version 1.0
 */

public class Config
{

  /**
   * Creates a new <code>Config</code> instance.
   *
   */
  Config()
  {
  }

  public static final String HTMLFilesPath =
    "/home/joao/Desktop/git/RUBiS/Servlet_HTML";
  public static final String DatabaseProperties =
    "/home/joao/Desktop/git/RUBiS/Servlets/mysql.properties";

 // original values for pooling
 /* public static final int AboutMePoolSize = 10;
  public static final int BrowseCategoriesPoolSize = 6;
  public static final int BrowseRegionsPoolSize = 6;
  public static final int BuyNowPoolSize = 4;
  public static final int PutBidPoolSize = 8;
  public static final int PutCommentPoolSize = 2;
  public static final int RegisterItemPoolSize = 2;
  public static final int RegisterUserPoolSize = 2;
  public static final int SearchItemsByCategoryPoolSize = 15;
  public static final int SearchItemsByRegionPoolSize = 20;
  public static final int StoreBidPoolSize = 8;
  public static final int StoreBuyNowPoolSize = 4;
  public static final int StoreCommentPoolSize = 2;
  public static final int ViewBidHistoryPoolSize = 4;
  public static final int ViewItemPoolSize = 20;
  public static final int ViewUserInfoPoolSize = 4;*/
  
  public static final int AboutMePoolSize = 3;
  public static final int BrowseCategoriesPoolSize = 3;
  public static final int BrowseRegionsPoolSize = 3;
  public static final int BuyNowPoolSize = 3;
  public static final int PutBidPoolSize = 3;
  public static final int PutCommentPoolSize = 3;
  public static final int RegisterItemPoolSize = 3;
  public static final int RegisterUserPoolSize = 3;
  public static final int SearchItemsByCategoryPoolSize = 3;
  public static final int SearchItemsByRegionPoolSize = 3;
  public static final int StoreBidPoolSize = 3;
  public static final int StoreBuyNowPoolSize = 3;
  public static final int StoreCommentPoolSize = 3;
  public static final int ViewBidHistoryPoolSize = 3;
  public static final int ViewItemPoolSize = 3;
  public static final int ViewUserInfoPoolSize = 3;
}
