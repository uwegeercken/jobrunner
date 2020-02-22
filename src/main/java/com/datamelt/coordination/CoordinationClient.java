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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.UnknownHostException;

public class CoordinationClient
{
	// the server address - default is 127.0.0.1
	private String server="127.0.0.1";
	// the port the server runs on - default 9000
	private int port=9000;
	// the socket to the server
	private Socket socket;
	
	private ObjectOutputStream outputStream;
	private ObjectInputStream inputStream;
	
	private long counter=0;
	
	public CoordinationClient(String server, int port) throws UnknownHostException, IOException
	{
		this.server = server;
		this.port = port;
		
		init();
	}
	
	public CoordinationClient(String server) throws UnknownHostException, IOException
	{
		this.server = server;
		
		init();
	}
	
	private void init() throws UnknownHostException, IOException
	{
		getServerSocket(server, port);
		outputStream = new ObjectOutputStream(new BufferedOutputStream(socket.getOutputStream()));
		// flush MUST be called - otherwise the stream is blocking!
		outputStream.flush();
		inputStream = new ObjectInputStream(new BufferedInputStream(socket.getInputStream()));
	}
	
	public Object getServerMessage(String message) throws IOException, ClassNotFoundException
	{
		sendMessage(message);
		return inputStream.readObject();
	}
	
	private void sendMessage(String message) throws IOException
	{
		// send the message to the server
		outputStream.writeObject(message);
       	outputStream.flush();
	}
	
	private void getServerSocket(String server, int port) throws UnknownHostException, IOException
	{
		// create a socket for the given server
		this.socket = new Socket(server, port);
	}
	
	public void closeSocket() throws IOException
	{
		if(!socket.isClosed())
		{
			socket.close();
		}
	}
	
	public void closeOutputStream() throws IOException
	{
		//outputStream.close();
	}
	
	public String getServer() 
	{
		return server;
	}

	public void setServer(String server) 
	{
		this.server = server;
	}

	public int getPort() 
	{
		return port;
	}

	public void setPort(int port) 
	{
		this.port = port;
	}

	public long getCounter() 
	{
		return counter;
	}
}