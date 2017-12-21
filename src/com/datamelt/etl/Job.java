package com.datamelt.etl;

import java.util.ArrayList;
import java.util.Calendar;

import com.datamelt.util.Time;

public class Job implements Comparable<Job>
{
	private static final long DEFAULT_CHECK_INTERVAL 		= 60000;
	private static final int DEFAULT_MAX_CHECK_INTERVALS 	= 10;
	
	private String jobId;
	private String jobName;
	private String path;
	private Time scheduledStartTime;
	private Time actualStartTime;
	private Time finishedTime;
	private String dependentJobId;
	private boolean requiresDependentJobFinished 	= false;
	private boolean runReports 						= false;
	private boolean running							= false;
	private boolean finished						= false;
	private boolean startRequested					= false;
	private long checkInterval 						= DEFAULT_CHECK_INTERVAL;
	private long checkIntervalCounter				= 0;
	private long maxCheckIntervals 					= DEFAULT_MAX_CHECK_INTERVALS;
	private String logLevel							= "Basic";
	private int exitCode							= 0;
	private ArrayList<String> parameters 			= new ArrayList<String>();
	
	public Job(String id, String name, String path)
	{
		this.jobId = id;
		this.jobName = name;
		this.path = path;
	}
	
	public Job(String id, String name, String path, String dependentJobId)
	{
		this.jobId = id;
		this.jobName = name;
		this.path = path;
		this.dependentJobId = dependentJobId;
	}
	
	public String getJobId()
	{
		return jobId;
	}
	
	public void setJobId(String jobId)
	{
		this.jobId = jobId;
	}
	
	public String getJobName()
	{
		return jobName;
	}
	
	public void setJobName(String jobName)
	{
		this.jobName = jobName;
	}
	
	public String getPath()
	{
		return path;
	}
	
	public void setPath(String path)
	{
		this.path = path;
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
	
	public String getDependentJobId()
	{
		return dependentJobId;
	}

	public void setDependentJobId(String dependentJobId)
	{
		this.dependentJobId = dependentJobId;
	}

	public boolean getRequiresDependentJobFinished()
	{
		return requiresDependentJobFinished;
	}

	public void setRequiresDependentJobFinished(boolean requiresDependentJobFinished)
	{
		this.requiresDependentJobFinished = requiresDependentJobFinished;
	}

	public boolean isFinished()
	{
		return finished;
	}
	
	public boolean isRunning()
	{
		return running;
	}
	
	public boolean isStartRequested()
	{
		return startRequested;
	}
	
	public void setFinished(boolean finished)
	{
		this.finished = finished;
	}

	public void setRunning(boolean running)
	{
		this.running = running;
	}
	
	public void setStartRequested(boolean startRequested)
	{
		this.startRequested = startRequested;
	}
	
	@Override
	public int compareTo(Job arg0)
	{
		// TODO implement method to compare jobs for sorting by time
		return 0;
	}

	public long getCheckInterval()
	{
		return checkInterval;
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

	public boolean getRunReports()
	{
		return runReports;
	}

	public void setRunReports(boolean runReports)
	{
		this.runReports = runReports;
	}

	public long getCheckIntervalCounter()
	{
		return checkIntervalCounter;
	}

	public void setCheckIntervalCounter(long checkIntervalCounter)
	{
		this.checkIntervalCounter = checkIntervalCounter;
	}

	public void setScheduledStartTime(Time scheduledStartTime)
	{
		this.scheduledStartTime = scheduledStartTime;
	}

	public void setActualStartTime(Time actualStartTime)
	{
		this.actualStartTime = actualStartTime;
	}

	public void setFinishedTime(Time finishedTime)
	{
		this.finishedTime = finishedTime;
	}

	public ArrayList<String> getParameters()
	{
		return parameters;
	}

	public String getLogLevel()
	{
		return logLevel;
	}

	public void setLogLevel(String logLevel)
	{
		this.logLevel = logLevel;
	}

	public int getExitCode()
	{
		return exitCode;
	}

	public void setExitCode(int exitCode)
	{
		this.exitCode = exitCode;
	}
	
}
