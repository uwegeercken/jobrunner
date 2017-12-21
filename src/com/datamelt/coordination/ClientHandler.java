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
import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.SocketException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import com.datamelt.coordination.JobManager;
import com.datamelt.etl.Job;

public class ClientHandler extends Thread
{
	private String processId;
	private Socket socket;
    private long clientStart;
    private long serverStart;
    private JobManager jobManager;
    private ObjectOutputStream outputStream;
    private ObjectInputStream inputStream;
    
    // list of possible messages
    // the "exit" message is explicitly excluded here
    public static final String[] MESSAGES					= {"uptime","processid","hello","jobfinished", "jobcanstart", "jobstarttime", "jobrun", "jobexitcode", "jobruntime", "resetjobs", "reloadjobs"};
    
    public static final String RESPONSE_UPTIME 				= "uptime";
    public static final String RESPONSE_EXIT 				= "exit";
    public static final String RESPONSE_PROCESSID 			= "processid";
    public static final String RESPONSE_HELLO				= "hello";
    public static final String RESPONSE_JOB_FINISHED		= "jobfinished";
    public static final String RESPONSE_JOB_CAN_START		= "jobcanstart";
    public static final String RESPONSE_JOB_STARTTIME		= "jobstarttime";
    public static final String RESPONSE_JOB_RUNTIME			= "jobruntime";
    public static final String RESPONSE_RESET_JOBS			= "resetjobs";
    public static final String RESPONSE_RELOAD_JOBS			= "reloadjobs";
    public static final String RESPONSE_JOB_RUN				= "jobrun";
    public static final String RESPONSE_JOB_EXIT_CODE		= "jobexitcode";
    
    public static final String DELIMITER					= ":";
    
    private static final String DEFAULT_DATETIME_FORMAT		= "yyyy-MM-dd HH:mm:ss";
    private static SimpleDateFormat sdf						= new SimpleDateFormat(DEFAULT_DATETIME_FORMAT);
    
    private static Map<String,String> environmentVariables;
    
    ClientHandler(String processId, Socket socket, JobManager jobManager, long serverStart) throws Exception
    {
    	this.clientStart = System.currentTimeMillis();
    	this.serverStart = serverStart;
    	this.jobManager = jobManager;
    	this.processId= processId;
        this.socket = socket;
        
        this.outputStream = new ObjectOutputStream(new BufferedOutputStream(socket.getOutputStream()));
        // flush MUST be called after creating the output stream, otherwise the stream blocks
        outputStream.flush();
        this.inputStream = new ObjectInputStream(new BufferedInputStream(socket.getInputStream()));
    }

    @Override
    public void run()
    {
    	try
        {
    		boolean ok=true;
    		while (ok)
    		{
            	// waiting for a server object on the input stream
            	Object object = inputStream.readObject();

            	if(object instanceof String)
            	{
            		String serverObject = (String)object;
            		if(serverObject.equals(RESPONSE_EXIT))
            		{
    	                String responseMessage = "exit";
    	                sendClientMessage(responseMessage);
    	                
    	                //systemMessage("client requested exit - closing client socket");

    	               	ok=false;
            		}
            		else if(serverObject.equals(RESPONSE_UPTIME))
            		{
    	                String responseMessage = getRunTime(System.currentTimeMillis(),serverStart);
    	                sendClientMessage(responseMessage);
            		}
            		else if(serverObject.equals(RESPONSE_PROCESSID))
            		{
    	                String responseMessage = processId;
    	                sendClientMessage("client processid: " + responseMessage);
            		}
            		else if(serverObject.equals(RESPONSE_HELLO))
            		{
    	                sendClientMessage(RESPONSE_HELLO + " client from: " + socket.getInetAddress().toString());
            		}
            		else if(serverObject.startsWith(RESPONSE_JOB_CAN_START))
            		{
            			String jobId = parseJobId(serverObject);
            			int jobStatus = jobManager.getJobStatus(jobId);
            			if(jobStatus != JobManager.STATUS_UNDEFINED)
            			{
            				sendClientMessage(jobStatus);
            			}
            			else
            			{
            				sendClientMessage(jobId,"is not existing");
            			}
            		}
            		else if(serverObject.startsWith(RESPONSE_JOB_STARTTIME))
            		{
            			String jobId = parseJobId(serverObject);
            			String jobStarttime = jobManager.getJobScheduledStarttime(jobId);
            			if(jobStarttime!=null)
            			{
            				sendClientMessage(jobStarttime);
            			}
            			else
            			{
            				sendClientMessage(jobId, "is not existing");
            			}
            		}
            		else if(serverObject.startsWith(RESPONSE_JOB_RUNTIME))
            		{
            			String jobId = parseJobId(serverObject);
            			if(jobId!=null)
            			{
            				Job job = jobManager.getJob(jobId);
            				if(job.getActualStartTime()!=null && job.getFinishedTime()!=null)
            				{
            					long jobStarttime = job.getActualStartTime().getTimeInMillis();
            					long jobEndtime = job.getFinishedTime().getTimeInMillis();
            					sendClientMessage(getRunTime(jobEndtime, jobStarttime));
            				}
            				else if(job.getActualStartTime()==null)
            				{
            					sendClientMessage("job has not started");
            				}
            				else
            				{
            					sendClientMessage("job has not finished");
            				}
            			}
            			else
            			{
            				sendClientMessage(jobId, "is not existing");
            			}
            		}
            		else if(serverObject.startsWith(RESPONSE_JOB_EXIT_CODE))
            		{
            			String jobId = parseJobId(serverObject);
            			int exitCode = jobManager.getJob(jobId).getExitCode();
           				sendClientMessage(jobId, exitCode);
            		}
            		else if(serverObject.startsWith(RESPONSE_RESET_JOBS))
            		{
            			jobManager.resetJobs();
            			
            			systemMessage("reset all jobs and schedule for current date");
    	                sendClientMessage("ok");
            		}
            		else if(serverObject.startsWith(RESPONSE_RELOAD_JOBS))
            		{
            			jobManager.reloadJobs();
            			
            			systemMessage("reloaded all jobs");
    	                sendClientMessage("ok");
            		}
            		else if(serverObject.startsWith(RESPONSE_JOB_FINISHED))
            		{
            			String jobId = parseJobId(serverObject);
            			Job job = jobManager.getJob(jobId);
            			if(job!=null)
            			{
            				if(job.isFinished())
            				{
            					sendClientMessage(jobId,job.isFinished() + " - " + job.getFinishedTime().getTime());
            				}
            				else
            				{
            					sendClientMessage(jobId,"finished: " + job.isFinished());
            				}
            			}
            			else
            			{
            				sendClientMessage(jobId, "is not existing");
            			}
            		}
            		else if(serverObject.startsWith(RESPONSE_JOB_RUN))
            		{
            			String jobId = parseJobId(serverObject);
            			Job job = jobManager.getJob(jobId);
            			if(job!=null)
            			{
	            			if(!job.isFinished() && !job.isRunning())
	            			{
	            				EtlJob etlJob = new EtlJob(job,jobManager.getFolderLogfiles());
	            				EtlJob.setEnvironmentVariables(environmentVariables);
	            				systemMessage(job.getJobId(), "activated to run: " + job.getScheduledStartTime().getTime());
	            				sendClientMessage(jobId, "activated to run: " + job.getScheduledStartTime().getTime());
	            				etlJob.start();
	            				
	            			}
	            			else
	            			{
	            				if(job.isFinished())
	            				{
	            					sendClientMessage(jobId, "finished: " + job.getFinishedTime().getTime());
	            				}
	            				else
	            				{
	            					sendClientMessage(jobId, "running: " + job.getActualStartTime().getTime());
	            				}
	            			}
            			}
            			else
            			{
            				sendClientMessage(jobId, "is not existing");
            			}
            		}
            		else
            		{
    	                String responseMessage = "unknown message: " + serverObject;
    	                sendClientMessage(responseMessage);
            		}
            	}
            	else
            	{
            		String responseMessage = "unknown object received - only Strings are processed";
	                sendClientMessage(responseMessage);
            	}
            }
    		outputStream.flush();
            socket.close();
        }
    	catch (EOFException e)
        {
        	// something went wrong here
        	try
        	{
        		if(!socket.isClosed())
     			{
     				socket.close();
     			}
        	}
        	catch(Exception ex)
        	{
        		
        	}
        }
        catch (SocketException e)
        {
        	// something went wrong here
        	try
        	{
        		if(!socket.isClosed())
     			{
     				socket.close();
     			}
        	}
        	catch(Exception ex)
        	{
        		
        	}
        }
        catch (Exception e)
        {
        	// something went wrong here
        	try
        	{
        		if(!socket.isClosed())
     			{
     				socket.close();
     			}
        	}
        	catch(Exception ex)
        	{
        		
        	}
            e.printStackTrace();
        }
    }

    private void systemMessage(String message) throws IOException
    {
    	System.out.println(sdf.format(new Date()) + " - " + message);
    }
    
    private void systemMessage(String jobId, String message) throws IOException
    {
    	System.out.println(sdf.format(new Date()) + " - job [" + jobId + "]: " + message);
    }
    
    private void sendClientMessage(Object message) throws IOException
    {
    	sendMessage(message);
    }
    
    private void sendClientMessage(String jobId, Object message) throws IOException
    {
    	sendMessage("[" + jobId + "]: " + message);
    }

    private void sendMessage(Object responseMessage) throws IOException
    {
        outputStream.writeObject(responseMessage);
       	outputStream.flush();
    }
    
    private String getRunTime(long endTime, long startTime)
    {
    	long runTime = endTime - startTime;
    	long seconds = runTime/1000;
    	
    	if(seconds < 60)
    	{
    		return "" + seconds + " seconds";
    	}
    	else if(seconds >= 60 && seconds < 3600)
    	{
    		return "" + seconds/60 + " minute(s)";
    	}
    	else if(seconds >= 3600 && seconds < 86400)
    	{
    		return "" + seconds/3600 + " hour(s)";
    	}
    	else 
    	{
    		return "" + seconds/86400 + " day(s)";
    	}
    }

    private String parseJobId(String message)
	{
    	if(message!=null && !message.trim().equals(""))
    	{
	    	String [] parts = message.split(DELIMITER);
			if(parts.length>=2)
			{
				return parts[1];
			}
			else
			{
				return null;
			}
    	}
    	else
    	{
    		return null;
    	}
	}
    
	public String getProcessId()
	{
		return processId;
	}

	public long getClientStart() 
	{
		return clientStart;
	}

	public static Map<String, String> getEnvironmentVariables()
	{
		return environmentVariables;
	}

	public static void setEnvironmentVariables(Map<String, String> environmentVariables)
	{
		ClientHandler.environmentVariables = environmentVariables;
	}
	
}
