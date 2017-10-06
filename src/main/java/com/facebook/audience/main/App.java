package com.facebook.audience.main;

import java.util.ArrayList;
import java.util.List;

import com.facebook.audience.creator.PageAudience;
import com.facebook.audience.creator.WebsiteAudience;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.TableDataInsertAllRequest.Rows;
import com.google.api.services.bigquery.model.TableRow;
import com.google.bigquery.main.Authenticate;
import com.google.bigquery.main.TableResults;

public class App{
	
	public static ArrayList<Rows> logChunk = new ArrayList<Rows>();
	
    public static void main( String[] args )
    {
     
    	Bigquery bigquery;
    	
    	if((bigquery = Authenticate.getAuthenticated()) != null){
    		
    		System.out.println(bigquery);
    		
    		if(TableResults.ListDataSet(bigquery)){
    		
    			if(TableResults.ListTables(bigquery)){
    				
    				//Version 1 Commented Area
    				//TableDataList listData = TableResults.getResults(bigquery);
    				
    				List<TableRow> listData = TableResults.getResults(bigquery);
    				
    				//if(null != listData && listData.getTotalRows() > 0){
    				//	Version 1 Commented Area	
    				//	List<TableRow> rows = listData.getRows();
    				if(null != listData && listData.size() > 0){	
    				
    					//for(int arr_i = 0; arr_i < listData.getTotalRows(); arr_i++){
    					
    					for(int arr_i = 0; arr_i < listData.size(); arr_i++){
    					
    						//TableRow row = rows.get(arr_i);
    						
    						TableRow row = listData.get(arr_i);
    						
    						if(((String)row.getF().get(1).getV()).equals("ENGAGEMENT")){
    							
    							if(PageAudience.createPageRelatedAudience(row)){
    								System.out.println("Audience is created Successfully : " + row.getF().get(0).getV());
    							}
    							else{
    								System.out.println("Response Message : Page Engagement Audience Creation Failed.");
    							}
    							
    						}
    						else if(((String)row.getF().get(1).getV()).equals("WEBSITE")){
    							
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
    					
    					if(null !=  logChunk && logChunk.size() > 0){
    			    		
    			    		if(TableResults.insertDataRows(bigquery, logChunk)){
    			    			System.out.println("Response Message : Logs Added Successfully.");
    			    		}else{
    			    			System.out.println("Response Message : Error while saving Logs.");
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