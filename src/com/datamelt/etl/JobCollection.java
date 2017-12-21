package com.datamelt.etl;

import java.util.ArrayList;

public class JobCollection
{
	private ArrayList<Job> jobs = new ArrayList<Job>();
	
	public JobCollection()
	{
		
	}
	
	public JobCollection(ArrayList<Job> jobs)
	{
		this.jobs = jobs;
	}

	public void addJob(Job job)
	{
		jobs.add(job);
	}
	
	public Job getJob(int index)
	{
		return jobs.get(index);
	}
	
	public void removeJob(int index)
	{
		jobs.remove(index);
	}
	
	public ArrayList<Job> getJobs()
	{
		return jobs;
	}
	
	public int size()
	{
		return jobs.size();
	}
	
	public void clear()
	{
		jobs.clear();
	}
}
