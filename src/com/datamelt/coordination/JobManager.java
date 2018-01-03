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
import java.io.FileReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import com.datamelt.etl.Job;
import com.datamelt.etl.JobCollection;
import com.datamelt.etl.Report;
import com.datamelt.etl.ReportCollection;
import com.datamelt.util.DateTimeUtility;
import com.datamelt.util.FileUtility;
import com.datamelt.util.SpecialDatesUtility;
import com.datamelt.util.Time;
import com.datamelt.util.VariableReplacer;

public class JobManager
{
	public static final String JSON_KEY_JOB_ID							= "id";
	public static final String JSON_KEY_JOB_FILENAME					= "filename";
	public static final String JSON_KEY_JOB_NAME						= "name";
	public static final String JSON_KEY_JOB_PATH						= "path";
	public static final String JSON_KEY_JOBS							= "jobs";
	public static final String JSON_KEY_JOB_SCHEDULED_START_TIME		= "scheduled_start_time";
	public static final String JSON_KEY_JOB_CHECK_INTERVAL				= "check_interval";
	public static final String JSON_KEY_JOB_LOG_LEVEL					= "log_level";
	public static final String JSON_KEY_JOB_MAX_CHECK_INTERVALS			= "max_check_intervals";
	public static final String JSON_KEY_JOB_DEPENDS_ON_JOB				= "depends_on_job";
	public static final String JSON_KEY_JOB_DEPENDENT_JOB_ID			= "jobid";
	public static final String JSON_KEY_JOB_PARAMETERS					= "parameters";
	public static final String JSON_KEY_JOB_PARAMETER					= "parameter";
	
	public static final String JSON_KEY_REPORT_ID						= "id";
	public static final String JSON_KEY_REPORT_GROUP					= "group";
	public static final String JSON_KEY_REPORT_FILENAME					= "filename";
	public static final String JSON_KEY_REPORT_NAME						= "name";
	public static final String JSON_KEY_REPORT_PATH						= "path";
	public static final String JSON_KEY_REPORT_TARGET_PATH				= "target_path";
	public static final String JSON_KEY_REPORT_SCHEDULED_START_TIME		= "scheduled_start_time";
	public static final String JSON_KEY_REPORT_CHECK_INTERVAL			= "check_interval";
	public static final String JSON_KEY_REPORT_MAX_CHECK_INTERVALS		= "max_check_intervals";
	public static final String JSON_KEY_REPORT_PARAMETERS				= "parameters";
	public static final String JSON_KEY_REPORT_PARAMETER				= "parameter";
	public static final String JSON_KEY_REPORT_DEPENDS_ON_JOB			= "depends_on_job";
	
	public static final String JSON_KEY_REPORT_PENTAHO_SERVER			= "pentaho_server";
	public static final String JSON_KEY_REPORT_PENTAHO_SERVER_PORT		= "pentaho_server_port";
	public static final String JSON_KEY_REPORT_PENTAHO_BASE_URL			= "pentaho_base_url";
	public static final String JSON_KEY_REPORT_PENTAHO_SOLUTION			= "pentaho_solution";
	public static final String JSON_KEY_REPORT_PENTAHO_PATH				= "pentaho_path";
	public static final String JSON_KEY_REPORT_PENTAHO_RENDER_MODE		= "render_mode";
	public static final String JSON_KEY_REPORT_PENTAHO_OUTPUT_TARGET	= "output-target";
	public static final String JSON_KEY_REPORT_PENTAHO_LOCALE		= "locale";
	public static final String JSON_KEY_REPORT_PENTAHO_ATTACHMENT_NAME	= "attachment_name";
	
	private static final String DEFAULT_DATETIME_FORMAT					= "yyyy-MM-dd HH:mm:ss";
    private static SimpleDateFormat sdf									= new SimpleDateFormat(DEFAULT_DATETIME_FORMAT);
	
	public static final int STATUS_UNDEFINED	 						= 0;
	public static final int STATUS_CAN_START	 						= 1;
	public static final int STATUS_SCHEDULED_TIME_NOT_REACHED 			= 2;
	public static final int STATUS_DEPENDENT_JOB_NOT_FINISHED 			= 3;
	public static final int STATUS_DEPENDENT_JOB_BAD_EXIT_CODE 			= 4;
	
	public static final String[] JOB_STATUS 							= {"undefined","can start","scheduled time not reached","dependent job(s) not finished", "dependent job(s) with bad exit code"};
	
	public static final String TIME_DELIMITER							= ":";
	
	private JobCollection jobs 											= new JobCollection();
	private ReportCollection reports 									= new ReportCollection();
	private String folderJobs											= null;
	private String folderReports											= null;
	private String folderLogfiles										= null;
	
	private HashMap<String,String> jsonJobs								= new HashMap<String,String>();
	
	public JobManager(String folderJobs, String folderReports) throws Exception
	{
		this.folderJobs = folderJobs;
		this.folderReports = folderReports;
		loadJobs();
    	loadReports();
	}

	public JobManager(ArrayList<Job> jobs, ArrayList<Report> reports)
	{
		this.jobs = new JobCollection(jobs);
		this.reports = new ReportCollection(reports);
	}
	
	public void addJob(Job job)
	{
		jobs.addJob(job);
	}
	
	public void addReport(Report report)
	{
		reports.addReport(report);
	}

	public Job getJob(String jobId)
	{
		for(Job job : jobs.getJobs())
		{
			if(job.getJobId().equals(jobId))
			{
				return job;
			}
		}
		return null;
	}
	
	public String getJobAsJson(String jobId)
	{
		if(jobId!=null && jsonJobs.containsKey(jobId))
		{
			return jsonJobs.get(jobId);
		}
		else
		{
			return null;
		}
	}
	
	public String[] getJobList()
	{
		String[] list = new String[jobs.size()];
		for(int i=0;i<jobs.size();i++)
		{
			Job job = jobs.getJob(i);
			list[i] = job.getJobId();
		}
		return list;
	}
	
	public String[] getReportList()
	{
		String[] list = new String[reports.size()];
		for(int i=0;i<reports.size();i++)
		{
			Report report = reports.getReport(i);
			list[i] = report.getReportId();
		}
		return list;
	}

	private boolean checkFileOk(String path, String filename)
	{
		if(path!=null && filename!=null)
		{
			String fullFilename = FileUtility.addTrailingSlash(path) + filename;
			File file = new File(fullFilename);
			if(file.exists() && file.isFile() && file.canRead())
			{
				return true;
			}
			else
			{
				return false;
			}
		}
		else
		{
			return false;
		}
		
	}
	
	public void removeJob(String jobId)
	{
		int counter=0;
		if(jobId!=null)
		{
			for(Job job : jobs.getJobs())
			{
				if(job.getJobId().equals(jobId))
				{
					jobs.removeJob(counter);;
				}
				counter++;
			}
		}
	}

	public Report getReport(String reportId)
	{
		if(reportId!=null)
		{
			for(Report report : reports.getReports())
			{
				if(report.getReportId().equals(reportId))
				{
					return report;
				}
			}
		}
		return null;
	}
	
	public void removeReport(String reportId)
	{
		int counter=0;
		if(reportId!=null)
		{
			for(Report report : reports.getReports())
			{
				if(report.getReportId().equals(reportId))
				{
					reports.removeReport(counter);;
				}
				counter++;
			}
		}
	}
	
	public void reloadJobs() throws Exception
	{
		jobs.clear();
		loadJobs();
	}
	
	private void loadJobs() throws Exception
	{
		File directory = new File(folderJobs);
		File[] listOfFiles = directory.listFiles();
		for (File file : listOfFiles)
		{
		    if (file.isFile()) 
		    {
		    	System.out.println(file.getName());
		    	
				JSONParser parser = new JSONParser();
				// capture all job ids
				ArrayList<String> jobIds = new ArrayList<String>();
				
				try
				{
					JSONObject jsonObject  = (JSONObject) parser.parse(new FileReader(folderJobs +"/" + file.getName()));
					
					JSONArray jobs = (JSONArray) jsonObject.get(JSON_KEY_JOBS);
		            Iterator<JSONObject> iterator = jobs.iterator();
		            while (iterator.hasNext()) 
		            {
		            	JSONObject jsonJob = iterator.next();
		            	
		            	String jobId = (String) jsonJob.get(JSON_KEY_JOB_ID);
		            	String jobFilename = (String) jsonJob.get(JSON_KEY_JOB_FILENAME);
		            	String jobPath = (String) jsonJob.get(JSON_KEY_JOB_PATH);
		
		            	boolean idExists = false;
		            	
		            	if(jobId!=null && !jobId.trim().equals(""))
		            	{
		            		idExists = jobIds.contains(jobId);
		            	}
		            	
		            	boolean fileOk = checkFileOk(jobPath, jobFilename);
		            	
		            	if(jobId!= null && fileOk & !idExists)
		            	{
			            	jsonJobs.put(jobId,jsonJob.toString());
		            		
		            		jobIds.add(jobId);
		            		
		            		Job job = new Job(jobId,jobFilename,jobPath);
			            	
		            		if(jsonJob.get(JSON_KEY_JOB_NAME)!=null)
			            	{
			            		job.setJobName((String) jsonJob.get(JSON_KEY_JOB_NAME));	
			            	}
			            	if(jsonJob.get(JSON_KEY_JOB_SCHEDULED_START_TIME)!=null)
			            	{
			            		String parts[] = ((String) jsonJob.get(JSON_KEY_JOB_SCHEDULED_START_TIME)).split(TIME_DELIMITER);
			            		if(parts.length==3)
			            		{
			            			int hours = Integer.parseInt(parts[0]);
			            			int minutes = Integer.parseInt(parts[1]);
			            			int seconds = Integer.parseInt(parts[2]);
			            			job.setScheduledStartTime(new Time(hours,minutes,seconds));
			            		}
			            		else
			            		{
			            			throw new Exception("invalid scheduled start time definition. correct format is: [HH:mm:ss]");
			            		}
			            	}
			            	if(jsonJob.get(JSON_KEY_JOB_CHECK_INTERVAL)!=null)
			            	{
			            		job.setCheckInterval((long) jsonJob.get(JSON_KEY_JOB_CHECK_INTERVAL));	
			            	}
			            	if(jsonJob.get(JSON_KEY_JOB_LOG_LEVEL)!=null)
			            	{
			            		job.setLogLevel((String) jsonJob.get(JSON_KEY_JOB_LOG_LEVEL));	
			            	}
			            	if(jsonJob.get(JSON_KEY_JOB_MAX_CHECK_INTERVALS)!=null)
			            	{
			            		job.setMaxCheckIntervals((long) jsonJob.get(JSON_KEY_JOB_MAX_CHECK_INTERVALS));	
			            	}
			            	if(jsonJob.get(JSON_KEY_JOB_DEPENDS_ON_JOB)!=null)
			            	{
			            		JSONArray dependentJobs = (JSONArray) jsonJob.get(JSON_KEY_JOB_DEPENDS_ON_JOB);
			            		for(int i=0;i< dependentJobs.size();i++)
			                	{
			            			JSONObject dependentJob = (JSONObject) dependentJobs.get(i);
			            			job.addDependentJob((String) dependentJob.get(JSON_KEY_JOB_DEPENDENT_JOB_ID));
			            		}
			            	}
			            	if(jsonJob.get(JSON_KEY_JOB_PARAMETERS)!=null)
			            	{
			            		JSONObject jsonParameters = (JSONObject) jsonJob.get(JSON_KEY_JOB_PARAMETERS);
			            		
			            		ArrayList<String>parameters = new ArrayList<String>();
		
			                	for(Object key: jsonParameters.keySet())
			                	{
			                		String value = (String) jsonParameters.get(key);
			                		
			                		// translate variables to their real value
			                		if(VariableReplacer.isVariable(value))
			                		{
			                			String variableName = VariableReplacer.getVariableName(value);
			                			int offset = VariableReplacer.getOffset(value);
			                			int realValue = DateTimeUtility.getFieldValue(variableName,offset);
			                		
			                			parameters.add("-param:" + key + "=" + realValue);
			                		}
			                		else
			                		{
			                			parameters.add("-param:" + key + "=" + value);
			                		}
			                	}
			                	
				            	job.setParameters(parameters);
			            	}
			        		addJob(job);
		            	}
		            	else
		            	{
		            		if(!fileOk)
		            		{
		            			System.out.println(sdf.format(new Date()) + " - error: the file [" + jobFilename + "] in folder [" + jobPath + "] is not existing or can not be read. skipping data.");
		            		}
		            		else if(jobId == null)
			                {
		            			System.out.println(sdf.format(new Date()) + " -  error: job id is undefined. skipping data.");
			                }
		            		else if(idExists)
			                {
		            			System.out.println(sdf.format(new Date()) + " -  error: job id [" + jobId + "] is defined multiple times. skipping data.");
			                }
		            	}
		            }
				}
				catch(Exception ex)
				{
					ex.printStackTrace();
				}
		    }		
		}
	}
	
	private void loadReports() throws Exception
	{
		ArrayList<String> reportIds = new ArrayList<String>(); 
		
		File directory = new File(folderReports);
		File[] listOfFiles = directory.listFiles();
		for (File file : listOfFiles)
		{
		    if (file.isFile()) 
		    {
		    	System.out.println(file.getName());
		        
	        	JSONParser parser = new JSONParser();
	        	JSONObject jsonReport = (JSONObject) parser.parse(new FileReader(folderReports +"/" + file.getName()));
	        	
	        	String reportId = (String) jsonReport.get(JSON_KEY_REPORT_ID);
	        	String reportFilename = (String) jsonReport.get(JSON_KEY_REPORT_FILENAME);
	        	String reportPath = (String) jsonReport.get(JSON_KEY_REPORT_PATH);
	
	        	boolean reportIdExists = false;
	        	
	        	if(reportId!=null && !reportId.trim().equals(""))
	        	{
	        		reportIdExists = reportIds.contains(reportId);
	        	}
	        	
	        	boolean reportFileOk = checkFileOk(reportPath, reportFilename);
	        	
	        	if(reportId!= null && reportFileOk & !reportIdExists)
	        	{
	        		reportIds.add(reportId);
	        		
	        		Report report = new Report(reportId,reportFilename, reportPath);
	
	        		if(jsonReport.get(JSON_KEY_REPORT_NAME)!=null)
	            	{
	            		report.setReportName((String) jsonReport.get(JSON_KEY_REPORT_NAME));	
	            	}
	            	if(jsonReport.get(JSON_KEY_REPORT_SCHEDULED_START_TIME)!=null)
	            	{
	        			String parts[] = ((String) jsonReport.get(JSON_KEY_REPORT_SCHEDULED_START_TIME)).split(TIME_DELIMITER);
	            		if(parts.length==3)
	            		{
	                		try
	                		{
	                			int hours = Integer.parseInt(parts[0]);
	                			int minutes = Integer.parseInt(parts[1]);
	                			int seconds = Integer.parseInt(parts[2]);
	                			report.setScheduledStartTime(new Time(hours,minutes,seconds));
	                		}
	                		catch(Exception ex)
	                		{
	                			ex.printStackTrace();
	                		}
	            		}
	            		else
	            		{
	            			throw new Exception("invalid scheduled start time. correct format is: [HH:mm:ss]");
	            		}
	            	}
	            	if(jsonReport.get(JSON_KEY_REPORT_DEPENDS_ON_JOB)!=null)
	            	{
	            		JSONArray dependentJobs = (JSONArray) jsonReport.get(JSON_KEY_REPORT_DEPENDS_ON_JOB);
	            		for(int i=0;i< dependentJobs.size();i++)
	                	{
	            			JSONObject dependentJob = (JSONObject) dependentJobs.get(i);
	            			report.addDependentJob((String) dependentJob.get(JSON_KEY_JOB_DEPENDENT_JOB_ID));
	            		}
	            	}
	            	if(jsonReport.get(JSON_KEY_REPORT_PENTAHO_SERVER)!=null)
	            	{
	            		report.setPentahoServer((String) jsonReport.get(JSON_KEY_REPORT_PENTAHO_SERVER));	
	            	}
	            	if(jsonReport.get(JSON_KEY_REPORT_PENTAHO_SERVER_PORT)!=null)
	            	{
	            		report.setPentahoServerPort((long) jsonReport.get(JSON_KEY_REPORT_PENTAHO_SERVER_PORT));	
	            	}
	            	if(jsonReport.get(JSON_KEY_REPORT_PENTAHO_BASE_URL)!=null)
	            	{
	            		report.setPentahoBaseUrl((String) jsonReport.get(JSON_KEY_REPORT_PENTAHO_BASE_URL));	
	            	}
	            	if(jsonReport.get(JSON_KEY_REPORT_PENTAHO_SOLUTION)!=null)
	            	{
	            		report.setPentahoSolution((String) jsonReport.get(JSON_KEY_REPORT_PENTAHO_SOLUTION));	
	            	}
	            	if(jsonReport.get(JSON_KEY_REPORT_PENTAHO_PATH)!=null)
	            	{
	            		report.setPentahoPath((String) jsonReport.get(JSON_KEY_REPORT_PENTAHO_PATH));	
	            	}
	            	if(jsonReport.get(JSON_KEY_REPORT_PENTAHO_RENDER_MODE)!=null)
	            	{
	            		report.setPentahoRenderMode((String) jsonReport.get(JSON_KEY_REPORT_PENTAHO_RENDER_MODE));	
	            	}
	            	if(jsonReport.get(JSON_KEY_REPORT_PENTAHO_OUTPUT_TARGET)!=null)
	            	{
	            		report.setPentahoOutputTarget((String) jsonReport.get(JSON_KEY_REPORT_PENTAHO_OUTPUT_TARGET));	
	            	}
	            	if(jsonReport.get(JSON_KEY_REPORT_PENTAHO_LOCALE)!=null)
	            	{
	            		report.setPentahoLocale((String) jsonReport.get(JSON_KEY_REPORT_PENTAHO_LOCALE));	
	            	}
	            	if(jsonReport.get(JSON_KEY_REPORT_PENTAHO_ATTACHMENT_NAME)!=null)
	            	{
	            		report.setPentahoAttachmentName((String) jsonReport.get(JSON_KEY_REPORT_PENTAHO_ATTACHMENT_NAME));	
	            	}
	            	if(jsonReport.get(JSON_KEY_REPORT_CHECK_INTERVAL)!=null)
	            	{
	            		report.setCheckInterval((long) jsonReport.get(JSON_KEY_REPORT_CHECK_INTERVAL));	
	            	}
	            	if(jsonReport.get(JSON_KEY_REPORT_GROUP)!=null)
	            	{
	            		report.setGroup((long) jsonReport.get(JSON_KEY_REPORT_GROUP));	
	            	}
	            	if(jsonReport.get(JSON_KEY_REPORT_TARGET_PATH)!=null)
	            	{
	            		report.setTargetPath((String) jsonReport.get(JSON_KEY_REPORT_TARGET_PATH));	
	            	}
	            	if(jsonReport.get(JSON_KEY_REPORT_MAX_CHECK_INTERVALS)!=null)
	            	{
	            		report.setMaxCheckIntervals((long) jsonReport.get(JSON_KEY_REPORT_MAX_CHECK_INTERVALS));	
	            	}
	            	if(jsonReport.get(JSON_KEY_REPORT_PARAMETERS)!=null)
	            	{
	            		JSONObject jsonParameters = (JSONObject) jsonReport.get(JSON_KEY_REPORT_PARAMETERS);
	
	            		ArrayList<String>parameters = new ArrayList<String>();
	
	                	for(Object key: jsonParameters.keySet())
	                	{
	                		String value = (String) jsonParameters.get(key);
	                		String keyValue = getParameterKeyValue((String)key, value);
	                		
	                		parameters.add("&" + keyValue);
	                	}
		            	report.setParameters(parameters);
	            	}
	            	System.out.println(report.getServerUrl());
	        		addReport(report);
	        	}
	        	else
	        	{
	        		if(!reportFileOk)
	        		{
	        			System.out.println(sdf.format(new Date()) + " - error: in file [" + file.getName() + "] report [" + reportPath + "/" + reportFilename + "] is not existing or can not be read. skipping data.");
	        		}
	        		else if(reportId == null)
	                {
	        			System.out.println(sdf.format(new Date()) + " -  error: in file [" + file.getName() + "] report id is undefined. skipping data.");
	                }
	        		else if(reportIdExists)
	                {
	        			System.out.println(sdf.format(new Date()) + " -  error: in file [" + file.getName() + "] report id [" + reportId + "] is defined multiple times. skipping data.");
	                }
	        	}
	        }
		}
	}

	private String getParameterKeyValue(String key, String value) throws Exception
	{
		// translate variables to their real value
		if(VariableReplacer.isVariable(value))
		{
			int numberOfParts = 0;
			
			numberOfParts = value.split(":").length;
			String variableName = VariableReplacer.getVariableName(value);
			int offset = 0;
			if(numberOfParts==1)
			{
				int realValue = DateTimeUtility.getFieldValue(variableName,offset);
				return key + "=" + realValue;	
			}
			else if(numberOfParts==2)
			{
				offset = VariableReplacer.getOffset(value);
				int realValue = DateTimeUtility.getFieldValue(variableName,offset);
				return key + "=" + realValue;	
			}
			else if(numberOfParts==3)
			{
				if(SpecialDatesUtility.containsMethod(variableName))
				{
					offset = VariableReplacer.getOffset(value);
					String dateFormat = VariableReplacer.getDateFormat(value);
					String realValue = DateTimeUtility.getSpecialDateValue(variableName,offset,dateFormat);
					return key + "=" + realValue;	
				}
				else
				{
					offset = VariableReplacer.getOffset(value);
					String dateFormat = VariableReplacer.getDateFormat(value);
					String realValue = DateTimeUtility.getFieldValue(variableName,offset,dateFormat);
					return key + "=" + realValue;
				}
			}
			else
			{
				return key + "=" + value;
			}
			
		}
		else
		{
			return key + "=" + value;
		}
	}
	private boolean getJobScheduledTimeReached(Job job)
	{
		Time now = new Time(Calendar.getInstance());
		return now.sameOrAfter(job.getScheduledStartTime());
	}
	
	private boolean getReportScheduledTimeReached(Report report)
	{
		Time now = new Time(Calendar.getInstance());
		return now.sameOrAfter(report.getScheduledStartTime());
	}
	
	public String getJobScheduledStarttime(String jobId)
	{
		Job job = getJob(jobId);
		if(job!=null)
		{
			return job.getScheduledStartTime().getTime(Time.DEFAULT_DATETIME_FORMAT);
		}
		else
		{
			return null;
		}
	}

	public void resetJobs()
	{
		for(Job job : jobs.getJobs())
		{
			resetJob(job);
		}
	}

	public void resetJob(Job job)
	{
		job.setActualStartTime(null);
		job.setCheckIntervalCounter(0);
		job.setRunning(false);
		job.setFinishedTime(null);
		job.setFinished(false);
			
		Calendar calendar = job.getScheduledStartTime().getCalendar();
		int hours = calendar.get(Calendar.HOUR_OF_DAY);
		int minutes = calendar.get(Calendar.MINUTE);
		int seconds = calendar.get(Calendar.SECOND);
	
		job.setScheduledStartTime(new Time(hours,minutes,seconds));	
	}

	public void resetJob(String jobId)
	{
		Job job = getJob(jobId);
		if(job!=null)
		{
			resetJob(job);
		}
	}
	
	public int getJobStatus(String jobId)
	{
		Job job = getJob(jobId);
		if(job!=null)
		{
			return getJobStatus(job);
		}
		else
		{
			return STATUS_UNDEFINED;
		}
	}
	
	public int getJobStatus(Job job)
	{
		int status = STATUS_UNDEFINED;
		if(!getJobScheduledTimeReached(job))
		{
			status = STATUS_SCHEDULED_TIME_NOT_REACHED;
		}
		else
		{
			ArrayList<String> dependentJobs = job.getDependentJobs();
			if(dependentJobs!=null && dependentJobs.size()>0)
			{
				for(int i=0;i<dependentJobs.size();i++)
				{
					Job dependentJob = getJob(dependentJobs.get(i));
					if(dependentJob!=null && !dependentJob.isFinished())
					{
						status = STATUS_DEPENDENT_JOB_NOT_FINISHED;
						break;
					}
					else if(dependentJob!=null && dependentJob.isFinished() && dependentJob.getExitCode()>0)
					{
						status = STATUS_DEPENDENT_JOB_BAD_EXIT_CODE;
						break;
					}
					else
					{
						status = STATUS_CAN_START;
						
					}
				}
			}
			else
			{
				status = STATUS_CAN_START;
			}
		}
		return status;
	}
	
	public int getReportStatus(Report report)
	{
		int status = STATUS_UNDEFINED;
		
		status = STATUS_CAN_START;
		
		return status;
	}

	public int getReportStatus(String id)
	{
		Report report = getReport(id);
		if(report!=null)
		{
			return getReportStatus(report);
		}
		else
		{
			return STATUS_UNDEFINED;
		}
	}
	
	public String getFolderLogfiles()
	{
		return folderLogfiles;
	}

	public void setFolderLogfiles(String folderLogfiles)
	{
		this.folderLogfiles = folderLogfiles;
	}

	public int getNumberOfJobs()
	{
		return jobs.size();
	}
	
	public ArrayList<Report> getGroupReports(long group)
	{
		ArrayList<Report> groupReports = new ArrayList<Report>();
		for(Report report : reports.getReports())
		{
			if(report.getGroup() == group)
			{
				groupReports.add(report);
			}
		}
		return groupReports;
	}
	
	public ArrayList<String> getNextJobs()
	{
		long now = System.currentTimeMillis();
		long differenceToNow=Long.MAX_VALUE;
		// list will hold multiple job ids if they start at the same time
		ArrayList<String> nextJobIds = new ArrayList<String>();
		for(Job job : jobs.getJobs())
		{
			long jobTime = job.getScheduledStartTime().getTimeInMillis();
			long jobDifferenceToNow = jobTime - now;
			// check if the difference is greater zero (in the future) and
			// smaller than what we found before
			if(jobDifferenceToNow > 0 &&  jobDifferenceToNow < differenceToNow )
			{
				differenceToNow = jobDifferenceToNow;
				// clear current list if smaller value found
				nextJobIds.clear();
				nextJobIds.add(job.getJobId());
			}
			else if(jobDifferenceToNow == differenceToNow )
			{
				nextJobIds.add(job.getJobId());
			}
		}
		return nextJobIds;
	}
}
