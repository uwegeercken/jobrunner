package com.datamelt.etl;

import java.util.ArrayList;

public class ReportCollection
{
	private ArrayList<Report> reports = new ArrayList<Report>();
	
	public ReportCollection()
	{
		
	}
	
	public ReportCollection(ArrayList<Report> reports)
	{
		this.reports = reports;
	}
	
	public void addReport(Report report)
	{
		reports.add(report);
	}
	
	public Report getReport(int index)
	{
		return reports.get(index);
	}
	
	public void removeReport(int index)
	{
		reports.remove(index);
	}
	
	public ArrayList<Report> getReports()
	{
		return reports;
	}

	public int size()
	{
		return reports.size();
	}
}
