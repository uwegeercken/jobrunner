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
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;

import com.datamelt.coordination.ClientHandler;
import com.datamelt.coordination.CoordinationClient;
import com.datamelt.etl.Report;
import com.datamelt.util.Time;

public class ReportJob extends Thread 
{
	private static final String DEFAULT_DATETIME_FORMAT			= "yyyy-MM-dd HH:mm:ss";
	private static final String DEFAULT_LOG_DATETIME_FORMAT		= "yyyyMMddHHmmss";
	
	private static SimpleDateFormat sdf							= new SimpleDateFormat(DEFAULT_DATETIME_FORMAT);
	private static SimpleDateFormat sdfLogs						= new SimpleDateFormat(DEFAULT_LOG_DATETIME_FORMAT);
    
	private Report report;
	private String hostname 									= "127.0.0.1";
	private int serverPort;
	private String logfileFolder								= null;
	
	private static String commandName;
    private static String commandFolder;
    private static String baServerUserid;
    private static String baServerPassword;
	
	public ReportJob(Report report, int serverPort, String logfileFolder) throws Exception
	{
		this.report = report;
		this.serverPort = serverPort;
		if(logfileFolder!=null && !logfileFolder.trim().equals(""))
		{
			this.logfileFolder = logfileFolder;
		}
		else
		{
			this.logfileFolder = Paths.get("").toAbsolutePath().toString();
		}
	}
	
	private ProcessBuilder getProcessBuilder()
	{
		ArrayList <String>parameters = new ArrayList<String>();
		parameters.add(commandFolder +"/" + commandName);
		parameters.add("--output-file=" + logfileFolder + "/" + report.getReportId() + "_" + sdfLogs.format(new Date()) + ".log");
		parameters.add("--no-check-certificate");
		parameters.add("--output-document=" + report.getOutputFilename());
		parameters.add(report.getServerUrl() + report.getParametersString() + "&userid=" + getBaServerUserid() + "&password=" + getBaServerPassword());
		
		//System.out.println("report command: " + Arrays.deepToString(parameters.toArray()));
    	
		ProcessBuilder pb = new ProcessBuilder(parameters);
		
		pb.directory(new File(report.getPath()));
		
		
		return pb;
	}
	
	
	public void run()
	{
		report.setStartRequested(true);
		int reportStatus = -1;
		CoordinationClient client = null;
		
		try
		{
			client = new CoordinationClient(hostname,serverPort);
			
			while(reportStatus!=JobManager.STATUS_CAN_START)
			{
				reportStatus = (int)client.getServerMessage(ClientHandler.MESSAGE_REPORT_START_STATUS + ClientHandler.DELIMITER + report.getReportId());
				if(reportStatus==JobManager.STATUS_SCHEDULED_TIME_NOT_REACHED)
				{
					System.out.println(sdf.format(new Date()) + " - [" + report.getReportId()+ "] job scheduled start time not reached: " + report.getScheduledStartTime().getTime());
					sleep(report.getCheckInterval());
				}
				else if(reportStatus==JobManager.STATUS_DEPENDENT_JOB_NOT_FINISHED && report.getCheckIntervalCounter() < report.getMaxCheckIntervals())
				{
					report.setCheckIntervalCounter(report.getCheckIntervalCounter()+1);
					System.out.println(sdf.format(new Date()) + " - report [" + report.getReportId()+ "] waiting for dependent job(s) to finish. interval: [" + report.getCheckIntervalSeconds() + "] seconds, retries left: [" + (report.getMaxCheckIntervals() - report.getCheckIntervalCounter())+"]");
					sleep(report.getCheckInterval());
				}
				else if(reportStatus!=JobManager.STATUS_CAN_START)
				{
					System.out.println(sdf.format(new Date()) + " - report [" + report.getReportId() + "] dependent job(s) not finished and max check intervals is reached");
					break;
				}
			}
		}
		catch(Exception ex)
		{
			ex.printStackTrace();
		}
		
		if(reportStatus==JobManager.STATUS_CAN_START)
		{
			ProcessBuilder processBuilder = getProcessBuilder();
			
			File output = new File(logfileFolder + "/" + report.getReportId() + "_" + sdfLogs.format(new Date()) + ".log");
			processBuilder.redirectOutput(output);
			Process process = null;
			try
			{
				report.setActualStartTime(new Time(Calendar.getInstance()));
				report.setRunning(true);
				
				System.out.println(sdf.format(new Date()) + " - report [" + report.getReportId() + "] started [" + report.getActualStartTime().getTime() + "]");
				
				// send an exit signal
		    	client.getServerMessage(ClientHandler.MESSAGE_EXIT);
		    	
				process = processBuilder.start();
			    int exitCode = process.waitFor();
			    report.setExitCode(exitCode);
			    report.setRunning(false);
			    report.setFinished(true);
			    report.setFinishedTime(new Time(Calendar.getInstance()));
			    
				System.out.println(sdf.format(new Date()) + " - report [" + report.getReportId() + "] finished [" + report.getFinishedTime().getTime() + "]");
				if(exitCode!=0)
				{
					System.out.println(sdf.format(new Date()) + " - report [" + report.getReportId() + "] exit code: " + exitCode);
				}
				
		    	
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
	
	public static String getCommandName()
	{
		return commandName;
	}

	public static void setCommandName(String commandName)
	{
		ReportJob.commandName = commandName;
	}

	public static String getCommandFolder()
	{
		return commandFolder;
	}

	public static void setCommandFolder(String scriptFolder)
	{
		ReportJob.commandFolder = scriptFolder;
	}

	public static String getBaServerUserid()
	{
		return baServerUserid;
	}

	public static void setBaServerUserid(String baServerUserid)
	{
		ReportJob.baServerUserid = baServerUserid;
	}

	public static String getBaServerPassword()
	{
		return baServerPassword;
	}

	public static void setBaServerPassword(String baServerPassword)
	{
		ReportJob.baServerPassword = baServerPassword;
	}
	
}
