/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.datamelt.coordination;

import java.util.Arrays;

/**
 * utility class which can be used to send messages to a running server.
 * 
 * messages allow to get information about the server such as uptime and other
 * useful information.
 *  * 
 * @author uwe geercken 2017
 *
 */
public class CoordinationClientMessage 
{
    private static String hostname							= "localhost";
    private static int port									= 9000;
    private static String message;
    
	public static void main(String[] args) throws Exception
	{
		if(args.length!=3 && args.length!=1)
		{
			help();
		}
		else
		{
			processArguments(args);
			if(message!=null)
			{
				boolean validMessage = checkMessageValidity(message);
				
				if(validMessage)
				{
					// create a client to communicate with the server
					CoordinationClient client = new CoordinationClient(hostname,port);
			    	
					Object response = client.getServerMessage(message);
			    	System.out.println(response.toString());
			    	
			    	// send an exit signal
			    	client.getServerMessage(ClientHandler.RESPONSE_EXIT);
			    	
					// cleanup
			    	client.closeOutputStream();
				    client.closeSocket();
				}
				else
				{
					System.out.println("error: the message provided is invalid. possible messages are: " + Arrays.deepToString(ClientHandler.MESSAGES));
				}
			}
			else
			{
				System.out.println("error: message is undefined - exiting");
			}
		}
    }
	
	private static boolean checkMessageValidity(String message)
	{
		boolean validMessage=false;
		if(message!=null && !message.trim().equals(""))
		{
			for(String clientHandlerMessage : ClientHandler.MESSAGES)
			{
				String[] messageParts = message.split(":");
				if(messageParts[0].trim().equals(clientHandlerMessage.trim()))
				{
					validMessage = true;
					break;
				}
			}
		}
		return validMessage;
	}
	
	private static void processArguments(String[] args)
	{
		for(int i=0;i<args.length;i++)
		{
			if(args[i].startsWith("-h="))
			{
				hostname=args[i].substring(3);
			}
			else if(args[i].startsWith("-p="))
			{
				port=Integer.parseInt(args[i].substring(3));
			}
			else if(args[i].startsWith("-m="))
			{
				message=args[i].substring(3);
			}
		}
	}
	
	private static void help()
	{
		System.out.println("CoordinationClientMessage. Tool to send messages to a running coordination server");
		System.out.println("Valid messages are:");
		System.out.println("- uptime       : request response on the uptime of the coordination server");
		System.out.println("- processid    : request response on the Java process id of the coordination server");
		System.out.println("- hello        : request friendly response");
    	System.out.println();
    	System.out.println("CoordinationClientMessage -h=[hostname] -p=[port] -m=[message]");
    	System.out.println("where [hostname] : optional. the hostname or IP address of the server running the Jare rule engine. default: localhost");
    	System.out.println("      [port]     : optional. the port that the Jare rule engine server listens on. default: 9000");
    	System.out.println("      [message]  : required. the message to be sent to the Jare rule engine server");
    	System.out.println();
    	System.out.println("example: CoordinationClientMessage -h=localhost -p=9000 -m=uptime");
    	System.out.println();
    	System.out.println("published as open source under the Apache License. read the licence notice");
    	System.out.println("all code by uwe geercken, 2006-2017. uwe.geercken@web.de");
    	System.out.println();
	}

}
