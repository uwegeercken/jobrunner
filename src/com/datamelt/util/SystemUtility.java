package com.datamelt.util;

public class SystemUtility
{
		  public static long getPID() 
		  {
			  String processName = java.lang.management.ManagementFactory.getRuntimeMXBean().getName();
			  
			  //System.out.println("processname: " + processName);
			  return Long.parseLong(processName.split("@")[0]);
		  }

}
