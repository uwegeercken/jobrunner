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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import javax.net.ServerSocketFactory;

import com.datamelt.coordination.JobManager;
import com.datamelt.util.FileUtility;

public class CoordinationServer extends Thread
{
    private ServerSocket serverSocket;
    private Properties properties = new Properties();
    private int port;
    private String propertiesFileFullname;
    private long serverStart;
    private JobManager jobManager;
    
    private static final String PROPERTIES_FILE 			= "server.properties";
    
    private static final String PROPERTY_FOLDER_LOGS		= "folder.logs";
    private static final String PROPERTY_JOBS_FILENAME		= "jobs.filename";
    private static final String PROPERTY_PORT 				= "server.port";
    private static final String PROPERTY_SCRIPT_FOLDER		= "script.folder";
    private static final String PROPERTY_SCRIPT_NAME		= "script.name";
    
    
    private static final int 	DEFAULT_PORT 				= 9000;
    private static final String DEFAULT_DATETIME_FORMAT		= "yyyy-MM-dd HH:mm:ss";
    
    private static SimpleDateFormat sdf						= new SimpleDateFormat(DEFAULT_DATETIME_FORMAT);
    private static Map<String,String> environmentVariables	= new HashMap<String,String>();
    
    public CoordinationServer() throws Exception
    {
    	loadProperties();
    	loadEnvironmentVariables();
    	setVariables();
    	createFolders();
    	createSocket();
        
    }
    
    public CoordinationServer(String propertiesFile) throws Exception
    {
    	propertiesFileFullname = propertiesFile;
    	loadProperties(propertiesFile);
    	loadEnvironmentVariables();
    	setVariables();
    	createFolders();
    	createSocket();
    }
    
    private void loadProperties() throws IOException
    {
    	propertiesFileFullname = FileUtility.adjustSlash(CoordinationServer.class.getClassLoader().getResource("").getPath()) + PROPERTIES_FILE;
    	properties.load(CoordinationServer.class.getClassLoader().getResourceAsStream(PROPERTIES_FILE));
    }
    
    private void loadProperties(String propertiesFilename) throws IOException
    {
    	FileInputStream inputStream = new FileInputStream(new File(propertiesFilename));
    	properties.load(inputStream);
    	inputStream.close();
    }
    
    public String getProperty(String key)
    {
    	return properties.getProperty(key);
    }
    
    private void loadEnvironmentVariables()
    {
    	Enumeration<?> enumeration = properties.propertyNames();
		while (enumeration.hasMoreElements())
		{
			String key = (String) enumeration.nextElement();
			if(key.startsWith("env."))
			{
				String realKey = key.substring(4);
				String value = properties.getProperty(key);
				environmentVariables.put(realKey, value);
			}
		}
    }
    
    private void setVariables()
    {
    	if(properties.getProperty(PROPERTY_PORT)!=null)
    	{
    		port = Integer.parseInt(getProperty(PROPERTY_PORT));
    	}
    	else
    	{
    		port = DEFAULT_PORT;
    	}
    }
    
    private void createFolders()
    {
    	FileUtility.mkDirs(getProperty(PROPERTY_FOLDER_LOGS));
    }
    
    private void createSocket() throws IOException
    {
    	serverSocket = ServerSocketFactory.getDefault().createServerSocket(port);
    }
    
    public static void main(String args[]) throws Exception
    {
    	CoordinationServer server = null;
    	if(args.length==0)
    	{
    		server = new CoordinationServer();
    	}
    	else
    	{
    		if(args[0].equals("-h")||args[0].equals("-help"))
    		{
    			//help();
    		}
    		else
    		{
    			server = new CoordinationServer(args[0]);
    		}
    	}
    	
    	server.serverStart = System.currentTimeMillis();
    	System.out.println(sdf.format(new Date()) + " - server start...");
		System.out.println(sdf.format(new Date()) + " - using properties from: [" + server.propertiesFileFullname + "]");
		File jsonFile = new File(server.getProperty(PROPERTY_JOBS_FILENAME));
		if(jsonFile.exists())
		{
			server.jobManager = new JobManager(server.getProperty(PROPERTY_JOBS_FILENAME));
			server.jobManager.setFolderLogfiles(server.getProperty(PROPERTY_FOLDER_LOGS));
			server.start();
	        System.out.println(sdf.format(new Date()) +  " - waiting on: [" + server.serverSocket.getInetAddress() + "], port: [" + server.port + "] for connections");
		}
		else
		{
			throw new Exception("error: can not load json file with job definitions: [" + server.getProperty(PROPERTY_JOBS_FILENAME) + "]");
		}
    }
    
    @Override
    public void run()
    {
    	boolean ok=true;
        while (ok)
        {
            try  
            {
                final Socket socketToClient = serverSocket.accept();
                //System.out.println(sdf.format(new Date()) + " - client connected from: [" + socketToClient.getInetAddress() +"]");
                ClientHandler clientHandler = new ClientHandler(socketToClient, jobManager, port, serverStart);
                ClientHandler.setEnvironmentVariables(environmentVariables);
                if(getProperty(PROPERTY_SCRIPT_FOLDER)!=null && getProperty(PROPERTY_SCRIPT_NAME)!=null)
                {
	                ClientHandler.setScriptName(getProperty(PROPERTY_SCRIPT_NAME));
	                ClientHandler.setScriptFolder(getProperty(PROPERTY_SCRIPT_FOLDER));
	                clientHandler.start();
                }
                else
                {
                	throw new Exception("the variables [script.folder] and [script.name] must be defined in the properties file");
                }
            }
            catch (Exception ex)
            {
            	ok = false;
            	if(!serverSocket.isClosed())
            	{
            		try 
            		{
						serverSocket.close();
					} 
            		catch (IOException ioexception)
            		{
            			ioexception.printStackTrace();
					}
            	}
                ex.printStackTrace();
            }
        }
    }

	private String getProcessId(String clientInetAddress)
	{
		return "client-" + clientInetAddress + "_" + sdf.format(new Date());
	}

	public Properties getProperties()
	{
		return properties;
	}

	public int getPort()
	{
		return port;
	}

	public JobManager getJobManager()
	{
		return jobManager;
	}

	public static Map<String, String> getEnvironmentVariables()
	{
		return environmentVariables;
	}
	
	
}
