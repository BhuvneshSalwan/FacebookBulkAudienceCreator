package com.facebook.audience.creator;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.json.JSONObject;

import com.facebook.audience.main.App;
import com.google.api.services.bigquery.model.TableDataInsertAllRequest.Rows;
import com.google.api.services.bigquery.model.TableRow;

public class PageAudience {

	public static final String URL = "https://graph.facebook.com";
	public static final String VERSION = "v2.9";
	private static final String SESSION_TOKEN = "CAAWXmQeQZAmcBANADF6ew1ZBXAAifj7REIcHmbTVjkAR5q6GAnRjrpcuVhhV435LHMXpb8HzUKzQaUU4uwkxIl5xpYSgzUNog43JX4qxe0pqVBvjHZCsPfgIpRRGY7xfFC2hb1Hi1s9EH0IhQu4KlnTGcsdgIq5FN2ufeNHOeEB9YGck36aah1rPHrdi10ZD";

	public static Boolean createPageRelatedAudience(TableRow row){
		
		try{
			
			String account_id;
			String audience_name;
			String page_id;
			String event;
			String retention_days;
			
			try{ account_id = (String) row.getF().get(3).getV(); } catch(Exception e){ account_id = "NULL";}
			try{ audience_name = (String) row.getF().get(0).getV(); } catch(Exception e){ audience_name = "NULL"; }
			try{ page_id = (String) row.getF().get(5).getV(); } catch(Exception e){ page_id = "NULL"; }
			try{ event = (String) row.getF().get(8).getV(); } catch(Exception e){ event = "NULL"; }
			try{ retention_days = (String) row.getF().get(2).getV(); } catch(Exception e){ retention_days = "NULL"; }
			
			if(account_id.equals("NULL")){
				System.out.println("Response Message : Couldn't find the Account ID for the Audience.");
				return false;
			}
		
			String custom_url = URL + "/" + VERSION + "/act_" + account_id + "/customaudiences";
			
			HttpClient reqClient = new DefaultHttpClient();
			HttpPost reqpost = new HttpPost(custom_url);
			
			ArrayList<NameValuePair> urlparameters = new ArrayList<NameValuePair>();
			
			urlparameters.add(new BasicNameValuePair("access_token", SESSION_TOKEN));

			if(audience_name.equals("NULL")){
				System.out.println("Response Message : Couldn't find the name for the Audience.");
				return false;
			}
			
			if(page_id.equals("NULL")){
				System.out.println("Response Message : Couldn't find the Page ID for the Audience.");
				return false;
			}
			
			if(event.equals("NULL")){
				System.out.println("Response Message : Couldn't find the Event for the Audience.");
				return false;
			}
			
			urlparameters.add(new BasicNameValuePair("name", audience_name));
			urlparameters.add(new BasicNameValuePair("subtype","ENGAGEMENT"));
			urlparameters.add(new BasicNameValuePair("retention_days", String.valueOf(retention_days)));
			urlparameters.add(new BasicNameValuePair("prefill","true"));
			if(event.equalsIgnoreCase("Page")){
				event = "page_engaged";
			}
			else if(event.equalsIgnoreCase("AdEngage")){
				event = "page_post_interaction";
			}
			else if(event.equalsIgnoreCase("PageSave")){
				event = "page_or_post_save";
			}
			else if(event.equalsIgnoreCase("PageMessage")){
				event = "page_messaged";
			}
			else if(event.equalsIgnoreCase("ClickCTA")){
				event = "page_cta_clicked";
			}
			else if(event.equalsIgnoreCase("PageVisit")){
				event = "page_visited";
			}
			else{
				event = "page_engaged";
			}
			urlparameters.add(new BasicNameValuePair("rule","[{\"object_id\":\""+ page_id +"\",\"event_name\":\""+ event +"\"}]"));
			reqpost.setEntity(new UrlEncodedFormEntity(urlparameters));
		
			System.out.println("Sending POST Request : " + custom_url);
			System.out.println("POST Parameters : " + urlparameters.toString());
			
			HttpResponse response = reqClient.execute(reqpost);
			
			System.out.println("Response Code : " + response.getStatusLine().getStatusCode());
			
			StringBuffer buffer = new StringBuffer();
			BufferedReader reader = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
			String line = null;
			
			while((line = reader.readLine()) != null){
				buffer.append(line);
			}
			
			System.out.println("Response Content : " + buffer.toString());
			
			Rows logsRow = new Rows();
			
			HashMap<String, Object> logsMap = new HashMap<String, Object>();
			
			logsMap.put("account_id", account_id);
			logsMap.put("operation", "CREATE");
			logsMap.put("table_name", "AUDIENCE_CREATE");
			logsMap.put("audience_name", audience_name);
			logsMap.put("status_code", response.getStatusLine().getStatusCode());
			logsMap.put("response_message", buffer.toString());
			logsMap.put("created_at", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(System.currentTimeMillis()));
			
			logsRow.setJson(logsMap);
			App.logChunk.add(logsRow);
			
			if(response.getStatusLine().getStatusCode() >= 200 && response.getStatusLine().getStatusCode() < 300){
				
				JSONObject responseObj = new JSONObject(buffer.toString());
				
				if(responseObj.has("id")){
					System.out.println("Response Message : Audience Created Successfully with Audience ID : " + responseObj.getString("id"));
					return true;
				}
				else{
					System.out.println("Response Message : Please check the response. Wasn't able to find ID for the audience.");
					return false;
				}
				
			}
			else{
				System.out.println("Response Message : Request for Facebook Page Audience Creation Failed.");
				return false;
			}
			
		}
		catch(Exception e){
			System.out.println("Exception : PageAudience - createPageRelatedAudience Method");
			System.out.println(e);
			return false;
		}
	
	}
	
}