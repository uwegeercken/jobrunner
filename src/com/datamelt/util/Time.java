package com.datamelt.util;

import java.text.SimpleDateFormat;
import java.util.Calendar;

public class Time 
{
	public static final String DEFAULT_DATETIME_FORMAT		= "yyyy-MM-dd HH:mm:ss";
	public static final String DEFAULT_DATE_FORMAT			= "yyyy-MM-dd";
	public static final String DEFAULT_TIME_FORMAT			= "HH:mm:ss";
    
	private Calendar calendar;

	public Time(Calendar calendar)
	{
		this.calendar = calendar;
	}
	
	public Time(String datetime) throws Exception
	{
		SimpleDateFormat sdf = new SimpleDateFormat(DEFAULT_DATETIME_FORMAT);
		calendar = Calendar.getInstance();
		calendar.setTime(sdf.parse(datetime));
	}
	
	public Time(int year, int month, int day, int hour, int minute, int second)
	{
		Calendar time = Calendar.getInstance();
		
		time.set(Calendar.YEAR,year);
		time.set(Calendar.MONTH,month-1);
		time.set(Calendar.DAY_OF_MONTH,day);
		time.set(Calendar.HOUR_OF_DAY, hour);
		time.set(Calendar.MINUTE,minute);
		time.set(Calendar.SECOND,second);
		
		this.calendar = time;
	}

	public Time(int hour, int minute, int second)
	{
		Calendar time = Calendar.getInstance();
		
		time.set(Calendar.HOUR_OF_DAY, hour);
		time.set(Calendar.MINUTE,minute);
		time.set(Calendar.SECOND,second);
		
		this.calendar = time;
	}

	public long getTimeInMillis()
	{
		return calendar.getTimeInMillis();
	}
	
	public String getDate()
	{
		return getDateTime(DEFAULT_DATE_FORMAT);
	}
	
	public String getDate(String format)
	{
		return getDateTime(format);
	}
	
	public String getTime()
	{
		return getDateTime(DEFAULT_TIME_FORMAT);
	}
	
	public String getTime(String format)
	{
		return getDateTime(format);
	}

	private String getDateTime(String format)
	{
		SimpleDateFormat sdf = new SimpleDateFormat(format);
		return sdf.format(calendar.getTime());
	}

	public Calendar getCalendar()
	{
		return calendar;
	}

	public void setCalendar(Calendar calendar)
	{
		this.calendar = calendar;
	}

	public boolean sameOrAfter(Time otherTime)
	{
		if(this.getCalendar().getTimeInMillis()>= otherTime.getCalendar().getTimeInMillis())
		{
			return true;
		}
		else
		{
			return false;
		}
	}
}
