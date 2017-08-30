package com.facebook.audience.main;

import java.util.List;

import com.facebook.audience.creator.PageAudience;
import com.facebook.audience.creator.WebsiteAudience;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.TableDataList;
import com.google.api.services.bigquery.model.TableRow;
import com.google.bigquery.main.Authenticate;
import com.google.bigquery.main.TableResults;

public class App{
	
    public static void main( String[] args )
    {
        
    	Bigquery bigquery;
    	
    	if((bigquery = Authenticate.getAuthenticated()) != null){
    		
    		System.out.println(bigquery);
    		
    		if(TableResults.ListDataSet(bigquery)){
    		
    			if(TableResults.ListTables(bigquery)){
    				
    				TableDataList listData = TableResults.getResults(bigquery);
    				
    				if(null != listData && listData.getTotalRows() > 0){
    					
    					List<TableRow> rows = listData.getRows();
    					
    					for(int arr_i = 0; arr_i < listData.getTotalRows(); arr_i++){
    						
    						TableRow row = rows.get(arr_i);
    						
    						if(((String)row.getF().get(12).getV()).equals("ENGAGEMENT")){
    							
    							if(PageAudience.createPageRelatedAudience(row)){
    								System.out.println("Audience is created Successfully : " + row.getF().get(0).getV());
    							}
    							else{
    								System.out.println("Response Message : Page Engagement Audience Creation Failed.");
    							}
    							
    						}
    						else if(((String)row.getF().get(12).getV()).equals("WEBSITE")){
    							
    							if(WebsiteAudience.createWebsiteRelatedAudience(row)){
    								System.out.println("Audience is created Successfully : " + row.getF().get(0).getV());
    							}
    							else{
    								System.out.println("Response Message : Website Audience Creation Failed.");
    							}
    							
    						}
    						else{
    							System.out.println("TYPE NOT MATCHED.");
    						}
    						
    					}
    					
    				}
    				else{
    					System.out.println("Response Message : Some Error while retrieving data from Table.");
    				}
    				
    			}
    			else{
    				System.out.println("Response Message : Error while Listing Tables.");
    			}
    			
    		}
    		else{
    			System.out.println("Response Message : Error while Listing Datasets.");
    		}
    		
    		System.exit(0);
    		
    	}
    	else{
    		
    		System.out.println("Response Message : Didn't got the object of Big Query from get Authenticated Method.");
    		System.exit(0);
    		
    	}
    	
    }

}