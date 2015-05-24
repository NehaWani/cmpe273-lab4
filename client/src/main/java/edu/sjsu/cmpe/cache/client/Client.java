package edu.sjsu.cmpe.cache.client;

/**
 * 
 * @author neh
 * Client file with main method.
 * It calls put function to put two values and then a get function call to check the status. 
 */
public class Client {
	 public static void main(String[] args) throws Exception {
	        System.out.println("Starting Cache Client!!!");
	        
	        CRDTClient crdt = new CRDTClient();
	        crdt.addServer("http://localhost:3000");
	        crdt.addServer("http://localhost:3001");
	        crdt.addServer("http://localhost:3002");
	        
	        crdt.put(1, "a");
	        System.out.println("Step 1: put(1 => a) ");
	        Thread.sleep(30*1000);
	        System.out.println("Step 1: Sleeping for 30 secs...");

	        crdt.put(1, "b");
	        System.out.println("Step 2: put(1 => b) ");
	        Thread.sleep(30*1000);
	        System.out.println("Step 2: Sleeping for 30 secs...");

	        String data = crdt.get(1);
	        System.out.println("Step 3: get(1) => " + data);

	        System.out.println("Exiting Cache Client!!!");
	    }
}