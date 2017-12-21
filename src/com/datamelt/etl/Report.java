package com.datamelt.etl;

import java.util.Calendar;

import com.datamelt.util.Time;

public class Report implements Comparable<Report>
{
	private String reportId;
	private String reportName;
	private String path;
	private Time scheduledTime;
	private Job dependentJob;
	private boolean requiresJobFinished;
	
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

	public Time getScheduledTime()
	{
		return scheduledTime;
	}

	public void setScheduledTime(Calendar scheduledTime)
	{
		this.scheduledTime = new Time(scheduledTime);
	}

	public void setScheduledTime(int year, int month, int day, int hour, int minute, int second)
	{
		this.scheduledTime = new Time(year, month, day, hour, minute, second);
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

	@Override
	public int compareTo(Report arg0)
	{
		// TODO implement method to compare for sorting by time
		return 0;
	}
	
}
