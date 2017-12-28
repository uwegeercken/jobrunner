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

package com.datamelt.etl;

import java.util.ArrayList;
import java.util.Calendar;

import com.datamelt.util.Time;

public class Report implements Comparable<Report>
{
	private static final long DEFAULT_CHECK_INTERVAL 		= 60000;
	private static final int DEFAULT_MAX_CHECK_INTERVALS 	= 10;
	
	private String reportId;
	private String reportName;
	private String path;
	private String targetPath;
	private long group;
	private Time scheduledStartTime;
	private Time actualStartTime;
	private Time finishedTime;
	private Job dependentJob;
	private boolean requiresJobFinished;
	private long checkInterval 						= DEFAULT_CHECK_INTERVAL;
	private long checkIntervalCounter				= 0;
	private long maxCheckIntervals 					= DEFAULT_MAX_CHECK_INTERVALS;
	private String pentahoServer;
	private long pentahoServerPort;
	private String pentahoBaseUrl;
	private String pentahoSolution;
	private String pentahoPath;
	private String pentahoRenderMode;
	private String pentahoOutputTarget;
	private String pentahoLocale;
	private String pentahoAttachmentName;

	private ArrayList<String> parameters			= new ArrayList<String>();
	
	public Report(String id, String name, String path)
	{
		this.reportId = id;
		this.reportName = name;
		this.path = path;
	}
	
	public Report(String id, String name, String path, Job dependentJob)
	{
		this.reportId = id;
		this.reportName = name;
		this.path = path;
		this.dependentJob = dependentJob;
	}
	
	public String getReportId()
	{
		return reportId;
	}
	
	public void setReportId(String reportId)
	{
		this.reportId = reportId;
	}

	public String getReportName()
	{
		return reportName;
	}
	
	public void setReportName(String reportName)
	{
		this.reportName = reportName;
	}
	
	public String getPath()
	{
		return path;
	}
	
	public void setPath(String path)
	{
		this.path = path;
	}

	public String getServerPath()
	{
		return pentahoServer +":" + pentahoServerPort +"/" + pentahoBaseUrl + "/" + pentahoSolution + pentahoPath + "&render_mode=" + pentahoRenderMode + "&output-target=" + pentahoOutputTarget + "&locale=" + pentahoLocale;
	}
	
	public String getTargetPath()
	{
		return targetPath;
	}
	
	public void setTargetPath(String targetPath)
	{
		this.targetPath = targetPath;
	}

	public Job getDependentJob()
	{
		return dependentJob;
	}

	public void setDependentJob(Job dependentJob)
	{
		this.dependentJob = dependentJob;
	}

	public boolean getRequiresJobFinished()
	{
		return requiresJobFinished;
	}

	public void setRequiresJobFinished(boolean requiresJobFinished)
	{
		this.requiresJobFinished = requiresJobFinished;
	}
	
	public boolean getDependentJobFinished()
	{
		return dependentJob.isFinished();
	}

	public ArrayList<String> getParameters()
	{
		return parameters;
	}

	public void setParameters(ArrayList<String> parameters)
	{
		this.parameters = parameters;
	}
	
	public long getCheckInterval()
	{
		return checkInterval;
	}
	
	public long getCheckIntervalCounter()
	{
		return checkIntervalCounter;
	}

	public void setCheckIntervalCounter(long checkIntervalCounter)
	{
		this.checkIntervalCounter = checkIntervalCounter;
	}

	public long getCheckIntervalSeconds()
	{
		return checkInterval/1000;
	}

	public void setCheckInterval(long checkInterval)
	{
		this.checkInterval = checkInterval;
	}

	public long getMaxCheckIntervals()
	{
		return maxCheckIntervals;
	}

	public void setMaxCheckIntervals(long maxCheckIntervals)
	{
		this.maxCheckIntervals = maxCheckIntervals;
	}
	
	public Time getScheduledStartTime()
	{
		return scheduledStartTime;
	}

	public void setScheduledStartTime(Calendar scheduledStartTime)
	{
		this.scheduledStartTime = new Time(scheduledStartTime);
	}

	public Time getActualStartTime()
	{
		return actualStartTime;
	}

	public Time getFinishedTime()
	{
		return finishedTime;
	}

	public void setScheduledStartTime(int year, int month, int day, int hour, int minute, int second)
	{
		this.scheduledStartTime = new Time(year, month, day, hour, minute, second);
	}
	
	public void setScheduledStartTime(Time scheduledStartTime)
	{
		this.scheduledStartTime = scheduledStartTime;
	}
	
	public String getPentahoServer()
	{
		return pentahoServer;
	}

	public void setPentahoServer(String pentahoServer)
	{
		this.pentahoServer = pentahoServer;
	}

	public long getPentahoServerPort()
	{
		return pentahoServerPort;
	}

	public void setPentahoServerPort(long pentahoServerPort)
	{
		this.pentahoServerPort = pentahoServerPort;
	}

	public String getPentahoBaseUrl()
	{
		return pentahoBaseUrl;
	}

	public void setPentahoBaseUrl(String pentahoBaseUrl)
	{
		this.pentahoBaseUrl = pentahoBaseUrl;
	}

	public String getPentahoSolution()
	{
		return pentahoSolution;
	}

	public void setPentahoSolution(String pentahoSolution)
	{
		this.pentahoSolution = pentahoSolution;
	}

	public String getPentahoPath()
	{
		return pentahoPath;
	}

	public void setPentahoPath(String pentahoPath)
	{
		this.pentahoPath = pentahoPath;
	}

	public String getPentahoRenderMode()
	{
		return pentahoRenderMode;
	}

	public void setPentahoRenderMode(String pentahoRenderMode)
	{
		this.pentahoRenderMode = pentahoRenderMode;
	}

	public String getPentahoOutputTarget()
	{
		return pentahoOutputTarget;
	}

	public void setPentahoOutputTarget(String pentahoOutputTarget)
	{
		this.pentahoOutputTarget = pentahoOutputTarget;
	}

	public String getPentahoLocale()
	{
		return pentahoLocale;
	}

	public void setPentahoLocale(String pentahoLocale)
	{
		this.pentahoLocale = pentahoLocale;
	}

	public String getPentahoAttachmentName()
	{
		return pentahoAttachmentName;
	}

	public void setPentahoAttachmentName(String pentahoAttachmentName)
	{
		this.pentahoAttachmentName = pentahoAttachmentName;
	}

	
	public long getGroup()
	{
		return group;
	}

	public void setGroup(long group)
	{
		this.group = group;
	}

	@Override
	public int compareTo(Report arg0)
	{
		// TODO implement method to compare for sorting by time
		return 0;
	}
	
}
