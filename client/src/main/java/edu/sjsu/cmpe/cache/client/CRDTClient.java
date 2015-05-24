package edu.sjsu.cmpe.cache.client;

import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import java.util.Iterator;

/**
 * 
 * @author neh
 * CRDT client implementation
 */
public class CRDTClient {

	private ArrayList<DistributedCacheService> serverArrayList = new ArrayList<DistributedCacheService>();
	public ConcurrentHashMap<String, String> putStatus = new ConcurrentHashMap<String, String>();
	public ConcurrentHashMap<String, String> getStatus = new ConcurrentHashMap<String, String>();
	
	//add server information to an array list
	public void addServer(String serverURL) {
		serverArrayList.add(new DistributedCacheService(serverURL, this));
	}
	
	public void put(long key, String value) throws Exception{
		//put the value for each server
		for(DistributedCacheService service: serverArrayList) {
			service.put(key, value);
		}
		
		while(true) {
			//starting to update all the three servers until the updation is complete
        	if(putStatus.size() < 3) {
        		System.out.println("The update is being currently processed!");
				Thread.sleep(1000);
        	} 
        	else{
        		//once all the three servers got updated, we need to confirm their status
        		int fail = 0, pass = 0;
        		for(DistributedCacheService service: serverArrayList) {
        			System.out.println("Status for 'PUT' on server: "+service.getCacheServerURL()+ " = "+ putStatus.get(service.getCacheServerURL()));
        			if(putStatus.get(service.getCacheServerURL()).equalsIgnoreCase("fail")){ 
            			++fail;
        			}
            		else{
            			++pass;
            		}
        		}
        		//Do a Rollback: If the failure was greater than 1, means it could not get updated on 2.
        		if(fail > 1) {
        			System.out.println("PUT a rollback...");
        			for(DistributedCacheService service: serverArrayList) {
        				service.delete(key);
        			}
        		}
        		else {
        			System.out.println("PUT SUCCESSFUL...");
        		}
        		putStatus.clear();
        		break;
        	}
        }
	}
	
	public String get(long key) throws Exception{
		for(DistributedCacheService service: serverArrayList) {
			service.get(key);
		}
		
		while(true) {
        	if(getStatus.size() < 3) {
        		System.out.println("Getting your response...");
				Thread.sleep(1000);
        	} 
        	else{
        		HashMap<String, List<String>> valuesMap = new HashMap<String, List<String>>();
        		for(DistributedCacheService service: serverArrayList) {
        			
        			//getting a value from a server fails
        			if(getStatus.get(service.getCacheServerURL()).equalsIgnoreCase("fail")){ 
            			System.out.println("Getting value from server: "+ service.getCacheServerURL() + " failed.");
        			}
            		else {
            			if(valuesMap.containsKey(getStatus.get(service.getCacheServerURL()))) {
            				valuesMap.get(getStatus.get(service.getCacheServerURL())).add(service.getCacheServerURL());
            			} 
            			else {
            				List<String> tempList = new ArrayList<String>();
            				tempList.add(service.getCacheServerURL());
            				valuesMap.put(getStatus.get(service.getCacheServerURL()),tempList);
            			}
            		}
        		}
        		
        		if(valuesMap.size() != 1) {
        			System.out.println("There is an Inconsistent state on the Servers!");
        			Iterator<Entry<String, List<String>>> iterator = valuesMap.entrySet().iterator();
        			int majority = 0;
        			String finVal = null;
        			ArrayList <String> updateServer = new ArrayList<String>();
        			
        		    while (iterator.hasNext()) {
        		        Map.Entry<String, List<String>> map = (Map.Entry<String, List<String>>)iterator.next();
        		        if(map.getValue().size() > majority) {
        		        	majority = map.getValue().size();
        		        	finVal = map.getKey();
        		        } 
        		        else {
        		        	for (String str: map.getValue()){
        		        		updateServer.add(str);
        		        	}
        		        }
        		    }
        		    
        			System.out.println("Now making server consistent!");
        			for(String str: updateServer){
        				for(DistributedCacheService service: serverArrayList) {
            				if(service.getCacheServerURL().equalsIgnoreCase(str)){
            					service.put(key, finVal);
            				}
            			}
        			}
        			getStatus.clear();
        			return finVal;
        		} 
        		else {
        			System.out.println("GET SUCCESSFUL.");
        			getStatus.clear();
        			return valuesMap.keySet().toArray()[0].toString();
        		}
        	}
        }
	}
}
