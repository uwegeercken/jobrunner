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
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;

import com.datamelt.coordination.ClientHandler;
import com.datamelt.coordination.CoordinationClient;
import com.datamelt.etl.Job;
import com.datamelt.util.Time;

public class EtlJob extends Thread 
{
	private static final String DEFAULT_DATETIME_FORMAT			= "yyyy-MM-dd HH:mm:ss";
	private static final String DEFAULT_LOG_DATETIME_FORMAT		= "yyyyMMddHHmmss";
	
	private static SimpleDateFormat sdf							= new SimpleDateFormat(DEFAULT_DATETIME_FORMAT);
	private static SimpleDateFormat sdfLogs						= new SimpleDateFormat(DEFAULT_LOG_DATETIME_FORMAT);
	private static Map<String,String> environmentVariables;
	private static String scriptName;
    private static String scriptFolder;
    
	private Job job;
	private String hostname 									= "127.0.0.1";
	private int serverPort;
	private String logfileFolder								= null;
	
	
	public EtlJob(Job job, int serverPort, String logfileFolder) throws Exception
	{
		this.job = job;
		this.serverPort = serverPort;
		if(logfileFolder!=null && logfileFolder.trim().equals(""))
		{
			this.logfileFolder = logfileFolder;
			File folder = new File(this.logfileFolder);
			if(!folder.exists())
			{
				folder.mkdirs();
			}
		}
		else
		{
			this.logfileFolder = Paths.get("").toAbsolutePath().toString();
		}
	}
	
	private ProcessBuilder getProcessBuilder()
	{
		ArrayList <String>parameters = new ArrayList<String>();
		parameters.add(scriptFolder +"/" + scriptName);
		parameters.add("-file=" + job.getPath() +"/" + job.getJobName());

    	for(Object key: job.getParameters().keySet())
    	{
    		String value= (String) job.getParameters().get(key);
    		parameters.add("-param:" + key + "=" + value);
    	}
        	
		parameters.add("-level=" + job.getLogLevel());
		
		ProcessBuilder pb = new ProcessBuilder(parameters);
		
		Map<String, String> env = pb.environment();
		env.putAll(environmentVariables);
		
		pb.directory(new File(job.getPath()));
		
		
		return pb;
	}
	
	public void run()
	{
		job.setStartRequested(true);
		int jobStatus = -1;
		CoordinationClient client = null;
		
		try
		{
			client = new CoordinationClient(hostname,serverPort);
			
			while(jobStatus!=JobManager.STATUS_JOB_CAN_START)
			{
				jobStatus = (int)client.getServerMessage(ClientHandler.RESPONSE_JOB_START_STATUS + ClientHandler.DELIMITER + job.getJobId());
				if(jobStatus==JobManager.STATUS_SCHEDULED_TIME_NOT_REACHED)
				{
					//System.out.println(sdf.format(new Date()) + " - [" + job.getJobId()+ "]: job scheduled start time not reached: " + job.getScheduledStartTime().getTime());
					sleep(job.getCheckInterval());
				}
				else if(jobStatus==JobManager.STATUS_DEPENDENT_JOB_NOT_FINISHED && job.getCheckIntervalCounter() < job.getMaxCheckIntervals())
				{
					job.setCheckIntervalCounter(job.getCheckIntervalCounter()+1);
					System.out.println(sdf.format(new Date()) + " - job [" + job.getJobId()+ "] - waiting for dependent job(s) to finish. retries left: " + (job.getMaxCheckIntervals() - job.getCheckIntervalCounter()));
					sleep(job.getCheckInterval());
				}
				else if(jobStatus!=JobManager.STATUS_JOB_CAN_START)
				{
					System.out.println(sdf.format(new Date()) + " - job [" + job.getJobId()+ "] - dependent job(s) not finished and max check intervals is reached");
					break;
				}
			}
		}
		catch(Exception ex)
		{
			ex.printStackTrace();
		}
		
		if(jobStatus==JobManager.STATUS_JOB_CAN_START)
		{
			ProcessBuilder processBuilder = getProcessBuilder();
			
			File output = new File(logfileFolder + "/" + job.getJobId() + "_" + sdfLogs.format(new Date()) + ".log");
			processBuilder.redirectOutput(output);
			Process process = null;
			try
			{
				job.setActualStartTime(new Time(Calendar.getInstance()));
				job.setRunning(true);
				
				System.out.println(sdf.format(new Date()) + " - job [" + job.getJobId() + "] started [" + job.getActualStartTime().getTime() + "]");
				
				// send an exit signal
		    	client.getServerMessage(ClientHandler.RESPONSE_EXIT);
		    	
				process = processBuilder.start();
			    int exitCode = process.waitFor();
			    job.setExitCode(exitCode);
			    job.setRunning(false);
			    job.setFinished(true);
			    job.setFinishedTime(new Time(Calendar.getInstance()));
			    
				System.out.println(sdf.format(new Date()) + " - job [" + job.getJobId() + "] finished [" + job.getFinishedTime().getTime() + "]");
				System.out.println(sdf.format(new Date()) + " - job [" + job.getJobId()+ "] exit code: " + exitCode);
				
		    	
			}
			catch(Exception e)
			{
			      e.printStackTrace();
			}  
		}
		
		// cleanup
		try
		{
			client.closeOutputStream();
			client.closeSocket();
		}
		catch(Exception ex)
		{
			ex.printStackTrace();
		}
	}
	
	public static Map<String, String> getEnvironmentVariables()
	{
		return environmentVariables;
	}

	public static void setEnvironmentVariables(Map<String, String> environmentVariables)
	{
		EtlJob.environmentVariables = environmentVariables;
	}
	
	public static String getScriptName()
	{
		return scriptName;
	}

	public static void setScriptName(String scriptName)
	{
		EtlJob.scriptName = scriptName;
	}

	public static String getScriptFolder()
	{
		return scriptFolder;
	}

	public static void setScriptFolder(String scriptFolder)
	{
		EtlJob.scriptFolder = scriptFolder;
	}
}
