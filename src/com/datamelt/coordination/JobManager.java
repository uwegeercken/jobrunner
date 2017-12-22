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

import java.io.FileReader;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Iterator;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import com.datamelt.etl.Job;
import com.datamelt.etl.JobCollection;
import com.datamelt.etl.Report;
import com.datamelt.etl.ReportCollection;
import com.datamelt.util.Time;

public class JobManager
{
	public static final String JSON_KEY_JOB_ID								= "id";
	public static final String JSON_KEY_JOB_NAME							= "name";
	public static final String JSON_KEY_JOB_PATH							= "path";
	public static final String JSON_KEY_JOBS								= "jobs";
	public static final String JSON_KEY_JOB_RUN_REPORTS						= "run_reports";
	public static final String JSON_KEY_JOB_SCHEDULED_START_TIME			= "scheduled_start_time";
	public static final String JSON_KEY_JOB_CHECK_INTERVAL					= "check_interval";
	public static final String JSON_KEY_JOB_LOG_LEVEL						= "log_level";
	public static final String JSON_KEY_JOB_MAX_CHECK_INTERVALS				= "max_check_intervals";
	public static final String JSON_KEY_JOB_DEPENDS_ON_JOB					= "depends_on_job";
	public static final String JSON_KEY_JOB__DEPENDENT_JOB_ID				= "jobid";
	public static final String JSON_KEY_JOB_PARAMETERS						= "parameters";
	public static final String JSON_KEY_JOB_PARAMETER						= "parameter";
		
	
	public static final int STATUS_UNDEFINED	 				= 0;
	public static final int STATUS_JOB_CAN_START 				= 1;
	public static final int STATUS_SCHEDULED_TIME_NOT_REACHED 	= 2;
	public static final int STATUS_DEPENDENT_JOB_NOT_FINISHED 	= 3;
	public static final int STATUS_DEPENDENT_JOB_BAD_EXIT_CODE 	= 4;
	
	public static final String[] JOB_STATUS 					= {"undefined","can start","scheduled time not reached","dependent job(s) not finished", "dependent job(s) with bad exit code"};
	
	public static final String TIME_DELIMITER					= ":";
	
	private JobCollection jobs 									= new JobCollection();
	private ReportCollection reports 							= new ReportCollection();
	private String jobFilename									= null;
	private String folderLogfiles								= null;
	
	public JobManager(String filename) throws Exception
	{
		this.jobFilename = filename;
		loadJobs();
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

	public void removeJob(String jobId)
	{
		int counter=0;
		for(Job job : jobs.getJobs())
		{
			if(job.getJobId().equals(jobId))
			{
				jobs.removeJob(counter);;
			}
			counter++;
		}
		
	}

	public Report getReport(String reportId)
	{
		for(Report report : reports.getReports())
		{
			if(report.getReportId().equals(reportId))
			{
				return report;
			}
		}
		return null;
	}
	
	public void removeReport(String reportId)
	{
		int counter=0;
		for(Report report : reports.getReports())
		{
			if(report.getReportId().equals(reportId))
			{
				reports.removeReport(counter);;
			}
			counter++;
		}
	}
	
	public void reloadJobs() throws Exception
	{
		jobs.clear();
		loadJobs();
	}
	
	private void loadJobs() throws Exception
	{
		JSONParser parser = new JSONParser();
		
		try
		{
			Object object = parser.parse(new FileReader(jobFilename));
			
			JSONObject jsonObject = (JSONObject) object;
			
			JSONArray jobs = (JSONArray) jsonObject.get(JSON_KEY_JOBS);
            Iterator<JSONObject> iterator = jobs.iterator();
            while (iterator.hasNext()) 
            {
            	JSONObject jsonJob = iterator.next();
            	
            	String jobId = (String) jsonJob.get(JSON_KEY_JOB_ID);
            	String jobName = (String) jsonJob.get(JSON_KEY_JOB_NAME);
            	String jobPath = (String) jsonJob.get(JSON_KEY_JOB_PATH);

            	Job job = new Job(jobId,jobName,jobPath);
            	
            	if(jsonJob.get(JSON_KEY_JOB_RUN_REPORTS)!=null)
            	{
            		job.setRunReports((boolean) jsonJob.get(JSON_KEY_JOB_RUN_REPORTS));	
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
            			job.addDependentJob((String) dependentJob.get(JSON_KEY_JOB__DEPENDENT_JOB_ID));
            		}
            	}
            	if(jsonJob.get(JSON_KEY_JOB_PARAMETERS)!=null)
            	{
            		JSONObject parameters = (JSONObject) jsonJob.get(JSON_KEY_JOB_PARAMETERS);
	            	job.setParameters(parameters);
            	}           	
        		addJob(job);
            }
		}
		catch(Exception ex)
		{
			ex.printStackTrace();
		}
		

	}
	
	private void loadReports(Job job)
	{
		// load the reports for the specified job
		
		Report report = new Report("id_0001", "testReport_1","/home/uwe/development/jobexecutor");
		report.setScheduledTime(2017,12,10,15,30,00);
		report.setDependentJob(job);
		report.setRequiresJobFinished(true);
		
		addReport(report);

	}
	
	private boolean getJobScheduledTimeReached(Job job)
	{
		Time now = new Time(Calendar.getInstance());
		return now.sameOrAfter(job.getScheduledStartTime());
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
						status = STATUS_JOB_CAN_START;
					}
				}
			}
			else
			{
				status = STATUS_JOB_CAN_START;
			}
		}
		return status;
	}

	public String getFolderLogfiles()
	{
		return folderLogfiles;
	}

	public void setFolderLogfiles(String folderLogfiles)
	{
		this.folderLogfiles = folderLogfiles;
	}

}
