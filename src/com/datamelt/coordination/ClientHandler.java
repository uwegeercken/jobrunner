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
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.SocketException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Map;

import com.datamelt.coordination.JobManager;
import com.datamelt.etl.Job;
import com.datamelt.etl.Report;
import com.datamelt.util.SystemUtility;

public class ClientHandler extends Thread
{
	private Socket socket;
    private long clientStart;
    private long serverStart;
    private int serverPort;
    private JobManager jobManager;
    private ObjectOutputStream outputStream;
    private ObjectInputStream inputStream;
    
    // list of possible messages
    // the "exit" message is explicitly excluded here
    public static final String[] MESSAGES					= {
    		"uptime",
    		"processid",
    		"hello",
    		"jobfinished", 
    		"jobcanstart",
    		"jobstartstatus",
    		"jobstarttime",
    		"jobrun",
    		"jobexitcode",
    		"jobruntime", 
    		"jobdependencies", 
    		"jobreset", 
    		"jobremove",
    		"jobadd", 
    		"jobjson", 
    		"listjobs", 
    		"resetjobs", 
    		"reloadjobs", 
    		"numberofjobs", 
    		"nextjob",
    		"reportrun",
    		"reportgrouprun",
    		"reportstarttime",
    		"reportruntime",
    		"listreports", 
    		"listgroupreports", 
    		"reportstartstatus", 
    		"reportjson", 
    		"reloadreports", 
    		"reportdependencies",
    		"reportexitcode",
    		"reportcanstart",
    		"reportremove",
    		"reportadd", 
    		};
    
    public static final String MESSAGE_UPTIME 				= "uptime";
    public static final String MESSAGE_EXIT 				= "exit";
    public static final String MESSAGE_PROCESSID 			= "processid";
    public static final String MESSAGE_HELLO				= "hello";
    public static final String MESSAGE_JOB_FINISHED			= "jobfinished";
    public static final String MESSAGE_JOB_CAN_START		= "jobcanstart";
    public static final String MESSAGE_JOB_START_STATUS		= "jobstartstatus";
    public static final String MESSAGE_JOB_STARTTIME		= "jobstarttime";
    public static final String MESSAGE_JOB_RUNTIME			= "jobruntime";
    public static final String MESSAGE_JOB_RESET			= "jobreset";
    public static final String MESSAGE_JOB_REMOVE			= "jobremove";
    public static final String MESSAGE_JOB_ADD				= "jobadd";
    public static final String MESSAGE_JOB_JSON				= "jobjson";
    public static final String MESSAGE_RESET_JOBS			= "resetjobs";
    public static final String MESSAGE_RELOAD_JOBS			= "reloadjobs";
    public static final String MESSAGE_NUMBER_OF_JOBS		= "numberofjobs";
    public static final String MESSAGE_LIST_JOBS			= "listjobs";
    public static final String MESSAGE_NEXT_JOB				= "nextjob";
    public static final String MESSAGE_JOB_RUN				= "jobrun";
    public static final String MESSAGE_JOB_EXIT_CODE		= "jobexitcode";
    public static final String MESSAGE_JOB_DEPENDENCIES		= "jobdependencies";
    
    public static final String MESSAGE_REPORT_RUN			= "reportrun";
    public static final String MESSAGE_REPORT_GROUP_RUN		= "reportgrouprun";
    public static final String MESSAGE_REPORT_STARTTIME		= "reportstarttime";
    public static final String MESSAGE_REPORT_CAN_START		= "reportcanstart";
    public static final String MESSAGE_REPORT_START_STATUS	= "reportstartstatus";
    public static final String MESSAGE_REPORT_RUNTIME		= "reportruntime";
    public static final String MESSAGE_REPORT_RESET			= "reportreset";
    public static final String MESSAGE_REPORT_REMOVE		= "reportremove";
    public static final String MESSAGE_REPORT_ADD			= "reportadd";
    public static final String MESSAGE_REPORT_JSON			= "reportjson";
    public static final String MESSAGE_LIST_REPORTS			= "listreports";
    public static final String MESSAGE_LIST_GROUP_REPORTS	= "listgroupreports";
    public static final String MESSAGE_RELOAD_REPORTS		= "reloadreports";
    public static final String MESSAGE_REPORT_DEPENDENCIES	= "reportdependencies";
    public static final String MESSAGE_REPORT_EXIT_CODE		= "reportexitcode";
    
    public static final String DELIMITER					= ":";
    
    private static final String DEFAULT_DATETIME_FORMAT		= "yyyy-MM-dd HH:mm:ss";
    private static SimpleDateFormat sdf						= new SimpleDateFormat(DEFAULT_DATETIME_FORMAT);
    private static final String CLIENT_MESSAGE_TYPE_JOB		= "job";
    private static final String CLIENT_MESSAGE_TYPE_REPORT  = "report";
    
    private static Map<String,String> environmentVariables;
    private static String scriptName;
    private static String scriptFolder;
    private static String commandName;
    private static String commandFolder;
    private static String baServerUserid;
    private static String baServerPassword;
    
    ClientHandler(Socket socket, JobManager jobManager, int serverPort, long serverStart) throws Exception
    {
    	this.clientStart = System.currentTimeMillis();
    	this.serverStart = serverStart;
    	this.serverPort = serverPort;
    	this.jobManager = jobManager;
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
            		if(serverObject.equals(MESSAGE_EXIT))
            		{
    	                String responseMessage = "exit";
    	                sendClientMessage(responseMessage);
    	                
    	                //systemMessage("client requested exit - closing client socket");

    	               	ok=false;
            		}
            		else if(serverObject.equals(MESSAGE_UPTIME))
            		{
    	                String responseMessage = "uptime " + getRunTime(System.currentTimeMillis(),serverStart);
    	                sendClientMessage(responseMessage);
            		}
            		else if(serverObject.equals(MESSAGE_PROCESSID))
            		{
    	                long pid = getProcessId();
    	                sendClientMessage("server processid: [" + pid + "]");
            		}
            		else if(serverObject.equals(MESSAGE_HELLO))
            		{
    	                sendClientMessage(MESSAGE_HELLO + " client from: [" + socket.getInetAddress().toString() + "]");
            		}
            		else if(serverObject.startsWith(MESSAGE_JOB_CAN_START))
            		{
            			String jobId = parseId(serverObject);
            			int jobStatus = jobManager.getJobStatus(jobId);
            			if(jobStatus != JobManager.STATUS_UNDEFINED)
            			{
            				sendClientMessage("status: [" + JobManager.STATUS_TYPES[jobStatus] + "]");
            			}
            			else
            			{
            				sendClientMessage(CLIENT_MESSAGE_TYPE_JOB,jobId, "not existing");
            			}
            		}
            		else if(serverObject.startsWith(MESSAGE_REPORT_CAN_START))
            		{
            			String reportId = parseId(serverObject);
            			int reportStatus = jobManager.getReportStatus(reportId);
            			if(reportStatus != JobManager.STATUS_UNDEFINED)
            			{
            				sendClientMessage("status: [" + JobManager.STATUS_TYPES[reportStatus] + "]");
            			}
            			else
            			{
            				sendClientMessage(CLIENT_MESSAGE_TYPE_REPORT,reportId, "not existing");
            			}
            		}
            		else if(serverObject.startsWith(MESSAGE_JOB_START_STATUS))
            		{
            			String jobId = parseId(serverObject);
            			int jobStatus = jobManager.getJobStatus(jobId);
            			if(jobStatus != JobManager.STATUS_UNDEFINED)
            			{
            				sendClientMessage(jobStatus);
            			}
            			else
            			{
            				sendClientMessage(CLIENT_MESSAGE_TYPE_JOB,jobId, "not existing");
            			}
            		}
            		else if(serverObject.startsWith(MESSAGE_REPORT_START_STATUS))
            		{
            			String reportId = parseId(serverObject);
            			int reportStatus = jobManager.getReportStatus(reportId);
            			if(reportStatus != JobManager.STATUS_UNDEFINED)
            			{
            				sendClientMessage(reportStatus);
            			}
            			else
            			{
            				sendClientMessage(CLIENT_MESSAGE_TYPE_REPORT,reportId, "not existing");
            			}
            		}
            		else if(serverObject.startsWith(MESSAGE_JOB_STARTTIME))
            		{
            			String jobId = parseId(serverObject);
            			String jobStarttime = jobManager.getJobScheduledStarttime(jobId);
            			if(jobStarttime!=null)
            			{
            				sendClientMessage(jobStarttime);
            			}
            			else
            			{
            				sendClientMessage(CLIENT_MESSAGE_TYPE_JOB,jobId, "not existing");
            			}
            		}
            		else if(serverObject.startsWith(MESSAGE_REPORT_STARTTIME))
            		{
            			String reportId = parseId(serverObject);
            			String reportStarttime = jobManager.getReportScheduledStarttime(reportId);
            			if(reportStarttime!=null)
            			{
            				sendClientMessage(reportStarttime);
            			}
            			else
            			{
            				sendClientMessage(CLIENT_MESSAGE_TYPE_REPORT,reportId, "not existing");
            			}
            		}
            		else if(serverObject.startsWith(MESSAGE_JOB_JSON))
            		{
            			String jobId = parseId(serverObject);
        				Job job = jobManager.getJob(jobId);
        				if(job!=null)
        				{
        					sendMessage(jobManager.getJobAsJson(jobId));
            			}
            			else
            			{
            				sendClientMessage(CLIENT_MESSAGE_TYPE_JOB,jobId, "not existing");
            			}
            		}
            		else if(serverObject.startsWith(MESSAGE_REPORT_JSON))
            		{
            			String reportId = parseId(serverObject);
        				Report report = jobManager.getReport(reportId);
        				if(report!=null)
        				{
        					sendMessage(jobManager.getReportAsJson(reportId));
            			}
            			else
            			{
            				sendClientMessage(CLIENT_MESSAGE_TYPE_REPORT,reportId, "not existing");
            			}
            		}
            		else if(serverObject.startsWith(MESSAGE_JOB_RUNTIME))
            		{
            			String jobId = parseId(serverObject);
        				Job job = jobManager.getJob(jobId);
        				if(job!=null)
        				{
            				if(job.getActualStartTime()!=null && job.getFinishedTime()!=null)
            				{
            					long jobStarttime = job.getActualStartTime().getTimeInMillis();
            					long jobEndtime = job.getFinishedTime().getTimeInMillis();
            					sendClientMessage(getRunTime(jobEndtime, jobStarttime));
            				}
            				else if(job.getActualStartTime()==null)
            				{
            					sendClientMessage(CLIENT_MESSAGE_TYPE_JOB,jobId, "not started");
            				}
            				else
            				{
            					sendClientMessage(CLIENT_MESSAGE_TYPE_JOB,jobId, "not finished");
            				}
            			}
            			else
            			{
            				sendClientMessage(CLIENT_MESSAGE_TYPE_JOB,jobId, "not existing");
            			}
            		}
            		else if(serverObject.startsWith(MESSAGE_REPORT_RUNTIME))
            		{
            			String reportId = parseId(serverObject);
        				Report report = jobManager.getReport(reportId);
        				if(report!=null)
        				{
            				if(report.getActualStartTime()!=null && report.getFinishedTime()!=null)
            				{
            					long reportStarttime = report.getActualStartTime().getTimeInMillis();
            					long reportEndtime = report.getFinishedTime().getTimeInMillis();
            					sendClientMessage(getRunTime(reportEndtime, reportStarttime));
            				}
            				else if(report.getActualStartTime()==null)
            				{
            					sendClientMessage(CLIENT_MESSAGE_TYPE_REPORT,reportId, "not started");
            				}
            				else
            				{
            					sendClientMessage(CLIENT_MESSAGE_TYPE_REPORT,reportId, "not finished");
            				}
            			}
            			else
            			{
            				sendClientMessage(CLIENT_MESSAGE_TYPE_REPORT,reportId, "not existing");
            			}
            		}
            		else if(serverObject.startsWith(MESSAGE_JOB_DEPENDENCIES))
            		{
            			String jobId = parseId(serverObject);
        				Job job = jobManager.getJob(jobId);
        				if(job!=null)
        				{
        					sendClientMessage(CLIENT_MESSAGE_TYPE_JOB,jobId, "depends on: " + job.getDependentJobs().toString());
        				}
            			else
            			{
            				sendClientMessage(CLIENT_MESSAGE_TYPE_JOB,jobId, "not existing");
            			}
            		}
            		else if(serverObject.startsWith(MESSAGE_REPORT_DEPENDENCIES))
            		{
            			String reportId = parseId(serverObject);
        				Report report = jobManager.getReport(reportId);
        				if(report!=null)
        				{
        					sendClientMessage(CLIENT_MESSAGE_TYPE_REPORT,reportId, "depends on: " + report.getDependentJobs().toString());
        				}
            			else
            			{
            				sendClientMessage(CLIENT_MESSAGE_TYPE_REPORT,reportId, "not existing");
            			}
            		}
            		else if(serverObject.startsWith(MESSAGE_NEXT_JOB))
            		{
            				ArrayList<String> nextJobIds = jobManager.getNextJobs();
            				if(nextJobIds.size()>0)
            				{
            					String jobId = nextJobIds.get(0);
            					Job job = jobManager.getJob(jobId);
            					sendClientMessage("next job(s): " + nextJobIds + " at [" +job.getScheduledStartTime().getTime() + "]");
            				}
            				else
            				{
            					sendClientMessage("no next job");
            				}
            		}
            		else if(serverObject.startsWith(MESSAGE_LIST_JOBS))
            		{
        				sendClientMessage("list of jobs: " + Arrays.deepToString(jobManager.getJobList()));
            		}
            		else if(serverObject.startsWith(MESSAGE_LIST_REPORTS))
            		{
        				sendClientMessage("list of reports: " + Arrays.deepToString(jobManager.getReportList()));
            		}
            		else if(serverObject.startsWith(MESSAGE_LIST_GROUP_REPORTS))
            		{
            			String groupId = parseId(serverObject);
            			ArrayList<String> reports = jobManager.getGroupReportsIds(Long.parseLong(groupId));
            			if(reports.size()>0)
            			{
            				sendClientMessage("list of reports, group [" + groupId + "]: " + Arrays.deepToString(reports.toArray()));
            			}
            			else
            			{
            				sendClientMessage(CLIENT_MESSAGE_TYPE_REPORT, groupId, "no reports found");
            			}
            		}
            		else if(serverObject.startsWith(MESSAGE_NUMBER_OF_JOBS))
            		{
        				sendClientMessage("number of jobs: [" + jobManager.getNumberOfJobs() + "]");
            		}
            		else if(serverObject.startsWith(MESSAGE_JOB_EXIT_CODE))
            		{
            			String jobId = parseId(serverObject);
            			int exitCode = jobManager.getJob(jobId).getExitCode();
           				sendClientMessage(CLIENT_MESSAGE_TYPE_JOB,jobId, exitCode);
            		}
            		else if(serverObject.startsWith(MESSAGE_REPORT_EXIT_CODE))
            		{
            			String reportId = parseId(serverObject);
            			int exitCode = jobManager.getReport(reportId).getExitCode();
           				sendClientMessage(CLIENT_MESSAGE_TYPE_REPORT,reportId, exitCode);
            		}
            		else if(serverObject.startsWith(MESSAGE_RESET_JOBS))
            		{
            			jobManager.resetJobs();
            			
            			systemMessage("reset jobs. schedules set to current date");
    	                sendClientMessage("ok");
            		}
            		else if(serverObject.startsWith(MESSAGE_JOB_RESET))
            		{
            			String jobId = parseId(serverObject);
            			Job job = jobManager.getJob(jobId);
        				if(job!=null)
        				{
        					jobManager.resetJob(job);
        					sendClientMessage(CLIENT_MESSAGE_TYPE_JOB,jobId, "reset");
        				}
            			else
            			{
            				sendClientMessage(CLIENT_MESSAGE_TYPE_JOB,jobId, "not existing");
            			}
            		}
            		else if(serverObject.startsWith(MESSAGE_REPORT_RESET))
            		{
            			String reportId = parseId(serverObject);
            			Report report = jobManager.getReport(reportId);
        				if(report!=null)
        				{
        					jobManager.resetReport(report);
        					sendClientMessage(CLIENT_MESSAGE_TYPE_REPORT,reportId, "reset");
        				}
            			else
            			{
            				sendClientMessage(CLIENT_MESSAGE_TYPE_REPORT,reportId, "not existing");
            			}
            		}
            		else if(serverObject.startsWith(MESSAGE_JOB_REMOVE))
            		{
            			String jobId = parseId(serverObject);
            			Job job = jobManager.getJob(jobId);
        				if(job!=null)
        				{
        					jobManager.removeJob(jobId);
        					sendClientMessage(CLIENT_MESSAGE_TYPE_JOB,jobId, "removed");
        				}
            			else
            			{
            				sendClientMessage(CLIENT_MESSAGE_TYPE_JOB,jobId, "not existing");
            			}
            		}
            		else if(serverObject.startsWith(MESSAGE_REPORT_REMOVE))
            		{
            			String reportId = parseId(serverObject);
            			Report report = jobManager.getReport(reportId);
        				if(report!=null)
        				{
        					jobManager.removeReport(reportId);
        					sendClientMessage(CLIENT_MESSAGE_TYPE_REPORT,reportId, "removed");
        				}
            			else
            			{
            				sendClientMessage(CLIENT_MESSAGE_TYPE_REPORT,reportId, "not existing");
            			}
            		}
            		else if(serverObject.startsWith(MESSAGE_JOB_ADD))
            		{
            			String id = parseId(serverObject);
            			try
       					{
       						File file = new File(id);
       						if(file.exists() && file.isFile())
       						{
       							Job job  = jobManager.addJob(id);
       							if(job!=null)
       							{
       								sendClientMessage(CLIENT_MESSAGE_TYPE_JOB,id, "added");
       							}
       							else
       							{
       								sendClientMessage(CLIENT_MESSAGE_TYPE_JOB,id, "already existing");
       							}
       						}
       						else
       						{
       							sendClientMessage(CLIENT_MESSAGE_TYPE_REPORT,id, "does not exist or not a file");
       						}
       					}
   						catch(Exception ex)
       					{
       						sendClientMessage(CLIENT_MESSAGE_TYPE_REPORT,id, "error reading file");
       					}
            		}
            		else if(serverObject.startsWith(MESSAGE_REPORT_ADD))
            		{
            			String id = parseId(serverObject);
       					try
       					{
       						File file = new File(id);
       						if(file.exists() && file.isFile())
       						{
       							Report report = jobManager.addReport(id);
       							if(report!=null)
       							{
       								sendClientMessage(CLIENT_MESSAGE_TYPE_REPORT,id, "added");
       							}
       							else
       							{
       								sendClientMessage(CLIENT_MESSAGE_TYPE_REPORT,id, "already existing");
       							}
       						}
       						else
       						{
       							sendClientMessage(CLIENT_MESSAGE_TYPE_REPORT,id, "does not exist or not a file");
       						}
       					}
       					catch(Exception ex)
       					{
       						sendClientMessage(CLIENT_MESSAGE_TYPE_REPORT,id, "error reading file");
       					}
            			
            		}
            		else if(serverObject.startsWith(MESSAGE_RELOAD_JOBS))
            		{
            			jobManager.reloadJobs();
            			
            			systemMessage("reloaded jobs");
    	                sendClientMessage("ok");
            		}
            		else if(serverObject.startsWith(MESSAGE_RELOAD_REPORTS))
            		{
            			jobManager.reloadReports();
            			
            			systemMessage("reloaded reports");
    	                sendClientMessage("ok");
            		}
            		else if(serverObject.startsWith(MESSAGE_JOB_FINISHED))
            		{
            			String jobId = parseId(serverObject);
            			Job job = jobManager.getJob(jobId);
            			if(job!=null)
            			{
            				if(job.isFinished())
            				{
            					sendClientMessage(CLIENT_MESSAGE_TYPE_JOB,jobId,"finished [" + job.getFinishedTime().getTime() + "]");
            				}
            				else
            				{
            					sendClientMessage(CLIENT_MESSAGE_TYPE_JOB,jobId, "not finished");
            				}
            			}
            			else
            			{
            				sendClientMessage(CLIENT_MESSAGE_TYPE_JOB,jobId, "not existing");
            			}
            		}
            		else if(serverObject.startsWith(MESSAGE_JOB_RUN))
            		{
            			String jobId = parseId(serverObject);
            			Job job = jobManager.getJob(jobId);
            			if(job!=null)
            			{
	            			if(!job.isFinished() && !job.isRunning())
	            			{
	            				EtlJob etlJob = new EtlJob(job, serverPort, jobManager.getFolderLogfiles());
	            				EtlJob.setEnvironmentVariables(environmentVariables);
	            				EtlJob.setScriptFolder(scriptFolder);
	            				EtlJob.setScriptName(scriptName);
	            				systemMessage(CLIENT_MESSAGE_TYPE_JOB,job.getJobId(), "activated run. scheduled: [" + job.getScheduledStartTime().getTime() + "]");
	            				sendClientMessage(CLIENT_MESSAGE_TYPE_JOB,jobId, "activated run. scheduled: [" + job.getScheduledStartTime().getTime() + "]");
	            				etlJob.start();
	            				
	            			}
	            			else
	            			{
	            				if(job.isFinished())
	            				{
	            					sendClientMessage(CLIENT_MESSAGE_TYPE_JOB,jobId, "finished: [" + job.getFinishedTime().getTime() + "]");
	            				}
	            				else
	            				{
	            					sendClientMessage(CLIENT_MESSAGE_TYPE_JOB,jobId, "running: [" + job.getActualStartTime().getTime() + "]");
	            				}
	            			}
            			}
            			else
            			{
            				sendClientMessage(CLIENT_MESSAGE_TYPE_JOB,jobId, "not existing");
            			}
            		}
            		else if(serverObject.startsWith(MESSAGE_REPORT_RUN))
            		{
            			String reportId = parseId(serverObject);
            			Report report = jobManager.getReport(reportId);
            			if(report!=null)
            			{
	            			if(!report.isFinished() && !report.isRunning())
	            			{
	            				ReportJob reportJob = new ReportJob(report, serverPort, jobManager.getFolderLogfiles());
	            				ReportJob.setCommandFolder(commandFolder);
	            				ReportJob.setCommandName(commandName);
	            				ReportJob.setBaServerUserid(baServerUserid);
	            				ReportJob.setBaServerPassword(baServerPassword);
	            				systemMessage(CLIENT_MESSAGE_TYPE_REPORT,report.getReportId(), "activated run. scheduled: [" + report.getScheduledStartTime().getTime() + "]");
	            				sendClientMessage(CLIENT_MESSAGE_TYPE_REPORT,reportId, "activated run. scheduled: [" + report.getScheduledStartTime().getTime() + "]");
	            				reportJob.start();
	            				
	            			}
	            			else
	            			{
	            				if(report.isFinished())
	            				{
	            					sendClientMessage(CLIENT_MESSAGE_TYPE_REPORT,reportId, "finished: [" + report.getFinishedTime().getTime() + "]");
	            				}
	            				else
	            				{
	            					sendClientMessage(CLIENT_MESSAGE_TYPE_REPORT,reportId, "running: [" + report.getActualStartTime().getTime() + "]");
	            				}
	            			}
            			}
            			else
            			{
            				sendClientMessage(CLIENT_MESSAGE_TYPE_REPORT,reportId, "not existing");
            			}
            		}
            		else if(serverObject.startsWith(MESSAGE_REPORT_GROUP_RUN))
            		{
            			String groupId = parseId(serverObject);
            			
            			ArrayList<Report> reports = jobManager.getGroupReports(Long.parseLong(groupId));
            			if(reports.size()>0)
            			{
            				for(int i=0;i<reports.size();i++)
            				{
	            				Report report = reports.get(i);
	            				if(!report.isFinished() && !report.isRunning())
		            			{
		            				ReportJob reportJob = new ReportJob(report, serverPort, jobManager.getFolderLogfiles());
		            				ReportJob.setCommandFolder(commandFolder);
		            				ReportJob.setCommandName(commandName);
		            				ReportJob.setBaServerUserid(baServerUserid);
		            				ReportJob.setBaServerPassword(baServerPassword);
		            				systemMessage(CLIENT_MESSAGE_TYPE_REPORT,report.getReportId(), "activated run. scheduled: [" + report.getScheduledStartTime().getTime() + "]");
		            				sendClientMessage(CLIENT_MESSAGE_TYPE_REPORT,report.getReportId(), "activated run. scheduled: [" + report.getScheduledStartTime().getTime() + "]");
		            				reportJob.start();
		            			}
		            			else
		            			{
		            				if(report.isFinished())
		            				{
		            					sendClientMessage(CLIENT_MESSAGE_TYPE_REPORT,report.getReportId(), "finished: [" + report.getFinishedTime().getTime() + "]");
		            				}
		            				else
		            				{
		            					sendClientMessage(CLIENT_MESSAGE_TYPE_REPORT,report.getReportId(), "running: [" + report.getActualStartTime().getTime() + "]");
		            				}
		            			}
            				}
            			}
            			else
            			{
            				sendClientMessage(CLIENT_MESSAGE_TYPE_REPORT,groupId, "no reports found");
            			}
            		}
            		else
            		{
    	                String responseMessage = "unknown message: [" + serverObject + "]";
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
    
    private void systemMessage(String type, String id, String message) throws IOException
    {
    	System.out.println(sdf.format(new Date()) + " - " + type + " [" + id + "] " + message);
    }
    
    private void sendClientMessage(Object message) throws IOException
    {
    	sendMessage(message);
    }
    
    private void sendClientMessage(String type, String jobId, Object message) throws IOException
    {
    	sendMessage(type + " [" + jobId + "] " + message);
    }

    private void sendMessage(Object responseMessage) throws IOException
    {
        outputStream.writeObject(responseMessage);
       	outputStream.flush();
    }
    
    public static String getBaServerUserid()
	{
		return baServerUserid;
	}

	public static void setBaServerUserid(String baServerUserid)
	{
		ClientHandler.baServerUserid = baServerUserid;
	}

	public static String getBaServerPassword()
	{
		return baServerPassword;
	}

	public static void setBaServerPassword(String baServerPassword)
	{
		ClientHandler.baServerPassword = baServerPassword;
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

    private String parseId(String message)
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
    
	public long getProcessId()
	{
		return SystemUtility.getPID();
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

	public static String getScriptName()
	{
		return scriptName;
	}

	public static void setScriptName(String scriptName)
	{
		ClientHandler.scriptName = scriptName;
	}

	public static String getScriptFolder()
	{
		return scriptFolder;
	}

	public static void setScriptFolder(String scriptFolder)
	{
		ClientHandler.scriptFolder = scriptFolder;
	}
	
	public static String getCommandName()
	{
		return commandName;
	}

	public static void setCommandName(String commandName)
	{
		ClientHandler.commandName = commandName;
	}

	public static String getCommandFolder()
	{
		return commandFolder;
	}

	public static void setCommandFolder(String scriptFolder)
	{
		ClientHandler.commandFolder = scriptFolder;
	}
}
