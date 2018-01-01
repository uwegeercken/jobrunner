package com.datamelt.coordination;

import java.io.FileReader;
import java.util.ArrayList;
import java.util.Iterator;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

public class Test1
{
	public static final String JSON_KEY_REPORTS							= "reports";
	public static final String JSON_KEY_REPORT_ID						= "id";
	public static final String JSON_KEY_REPORT_GROUP					= "group";
	public static final String JSON_KEY_REPORT_FILENAME					= "filename";
	public static final String JSON_KEY_REPORT_NAME						= "name";
	public static final String JSON_KEY_REPORT_PATH						= "path";
	
	public static void main(String[] args)
	{
		JSONParser parser = new JSONParser();
		
		try
		{
			JSONObject jsonReport = (JSONObject) parser.parse(new FileReader("/home/uwe/development/git/jobrunner/rp_0001.json"));
			
			String reportId = (String) jsonReport.get(JSON_KEY_REPORT_ID);
        	String reportFilename = (String) jsonReport.get(JSON_KEY_REPORT_FILENAME);
        	String reportPath = (String) jsonReport.get(JSON_KEY_REPORT_PATH);
        	
			System.out.println("");
			
		}
		catch(Exception ex)
		{
			
		}
			
	}
}
