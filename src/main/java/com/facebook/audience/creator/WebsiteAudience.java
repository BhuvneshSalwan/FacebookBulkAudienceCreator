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

public class WebsiteAudience {

	public static final String URL = "https://graph.facebook.com";
	public static final String VERSION = "v2.9";
	private static final String SESSION_TOKEN = "CAAWXmQeQZAmcBANADF6ew1ZBXAAifj7REIcHmbTVjkAR5q6GAnRjrpcuVhhV435LHMXpb8HzUKzQaUU4uwkxIl5xpYSgzUNog43JX4qxe0pqVBvjHZCsPfgIpRRGY7xfFC2hb1Hi1s9EH0IhQu4KlnTGcsdgIq5FN2ufeNHOeEB9YGck36aah1rPHrdi10ZD";

	public static Boolean createWebsiteRelatedAudience(TableRow row){
		
		try{
			
			String account_id;
			String audience_name;
			String pixel_id;
			String event;
			String retention_days;
			String keywords;
			String lower_bound;
			String hostname;
			String parse_client_id;
			
			try{ account_id = (String) row.getF().get(3).getV(); } catch(Exception e){ account_id = "NULL";}
			try{ audience_name = (String) row.getF().get(0).getV(); } catch(Exception e){ audience_name = "NULL"; }
			try{ pixel_id = (String) row.getF().get(4).getV(); } catch(Exception e){ pixel_id = "NULL"; }
			try{ event = (String) row.getF().get(8).getV(); } catch(Exception e){ event = "NULL"; }
			try{ retention_days = (String) row.getF().get(2).getV(); } catch(Exception e){ System.out.println(e); retention_days = "NULL"; }
			try{ keywords = (String) row.getF().get(7).getV(); } catch(Exception e){ keywords = "NULL"; }
			try{ lower_bound = (String) row.getF().get(9).getV(); } catch(Exception e){ System.out.println(e); lower_bound = "NULL"; }
			try{ hostname = (String) row.getF().get(6).getV(); } catch(Exception e){ System.out.println(e); hostname = "NULL"; }
			try{ parse_client_id = (String) row.getF().get(10).getV(); } catch(Exception e){ System.out.println(e); parse_client_id = "NULL"; }
			
			System.out.println(account_id);
			System.out.println(audience_name);
			System.out.println(pixel_id);
			System.out.println(event);
			System.out.println(retention_days);
			System.out.println(keywords);
			System.out.println(lower_bound);
			System.out.println(hostname);
			System.out.println(parse_client_id);
			
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
			
			if(pixel_id.equals("NULL")){
				System.out.println("Response Message : Couldn't find the Pixel ID for the Audience.");
				return false;
			}
			
			urlparameters.add(new BasicNameValuePair("name", audience_name));
			urlparameters.add(new BasicNameValuePair("subtype","WEBSITE"));
			urlparameters.add(new BasicNameValuePair("retention_days", retention_days));
			urlparameters.add(new BasicNameValuePair("pixel_id", pixel_id));
			urlparameters.add(new BasicNameValuePair("prefill","true"));
			
			if(!keywords.equals("NULL") && !keywords.equals("") && keywords.equals("checkout")){
				
				urlparameters.add(new BasicNameValuePair("rule", "{\"url\":{\"i_contains\":\"checkout\"}}"));
			
			}
			
			else if(!keywords.equals("NULL") && !keywords.equals("")){
				
				urlparameters.add(new BasicNameValuePair("rule", "{\"or\":[{\"url\":{\"i_contains\":\"thank\"}},{\"url\":{\"i_contains\":\"success\"}},{\"url\":{\"i_contains\":\"purchase\"}}]}"));
			
			}
			
			else if(!event.equals("NULL") && !event.equals("")){
	
				urlparameters.add(new BasicNameValuePair("rule", "{\"and\":[{\"event\":{\"eq\":\"" + event + "\"}}]}"));
				
			}
			
			else if(!lower_bound.equals("NULL") && !lower_bound.equals("")){
				
				urlparameters.add(new BasicNameValuePair("rule", "{\"and\":[{\"url\":{\"i_contains\":\"\"}},{\"or\":[{\"url\":{\"i_contains\":\""+ (String) row.getF().get(6).getV() +"\"}}]}]}"));
				
				urlparameters.add(new BasicNameValuePair("rule_aggregation", "{\"type\":\"time_spent\",\"method\":\"percentile\",\"lower_bound\":"+ lower_bound +",\"upper_bound\":100}"));		
						
			}
			
			else{
				
				urlparameters.add(new BasicNameValuePair("rule", "{\"and\":[{\"url\":{\"i_contains\":\"" + (String) row.getF().get(6).getV() + "\"}}]}"));
			
			}
			
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
			
			logsMap.put("hostname", hostname);
			logsMap.put("parse_client_id", parse_client_id);
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
				System.out.println("Response Message : Request for Facebbok Website Audience Creation Failed.");
				return false;
			}
			
		}
		catch(Exception e){
			System.out.println("Exception : WebsiteAudience - createWebsiteRelatedAudience Method");
			System.out.println(e);
			return false;
		}
	
	}
	
}