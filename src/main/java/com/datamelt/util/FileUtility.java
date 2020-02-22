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

package com.datamelt.util;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;

public class FileUtility
{
	// the default timestamp format for the filename
	public static final String DEFAULT_TIMESTAMP_FORMAT     = "yyyyMMddHHmmss";
	public static final String XML_FILE_EXTENSION  			= ".xml";
	public static final String FORWARD_SLASH				= "/";
	
	/**
     * adds the current timestamp to the given filename
     * by trying to locate the dot and setting the timestamp
     * before it. if none is found, the timestamp is appended.
     *  
     * @param filename	the name of the file
     * @return			the name of the file and the prepended timestamp
     */
    public static String getTimeStampedFilename(String filename)
    {
    	SimpleDateFormat sdf = new SimpleDateFormat(DEFAULT_TIMESTAMP_FORMAT);
    	int dotPosition = filename.lastIndexOf(".");
    	if(dotPosition>0)
    	{
    		return filename.substring(0,dotPosition) + "_" + sdf.format(new Date()) + filename.substring(dotPosition);
    	}
    	else
    	{
    		return filename + "_" + sdf.format(new Date());
    	}
    	
    }
    
    /**
     * adds the current timestamp to the given filename
     * by trying to locate the dot and setting the timestamp
     * before it. if none is found, the timestamp is appended.
     *  
     * @param filename			the name of the file
     * @param timestampFormat	the format of the timestamp according to the SimpleDateFormat class
     * @return					the name of the file and the prepended timestamp
     */
    public static String getTimeStampedFilename(String filename, String timestampFormat)
    {
    	SimpleDateFormat sdf = new SimpleDateFormat(timestampFormat);
    	int dotPosition = filename.lastIndexOf(".");
    	if(dotPosition>0)
    	{
    		return filename.substring(0,dotPosition) + "_" + sdf.format(new Date()) + filename.substring(dotPosition);
    	}
    	else
    	{
    		return filename + "_" + sdf.format(new Date());
    	}
    }
    
    /**
     * returns a array of files that are in the specified folder.
     * 
     * @param folder	the name of the folder where the xml files are stored
     * @return			array of files
     */
    public static File[] getFiles(String folder)
    {
    	 File dir = new File(folder);
         return dir.listFiles();
    }
    
    /**
     * Add a trailing slash if not already present
     * 
     * @param folder	the name of the folder
     * @return			the name of the folder with a slash character at the end
     */
    public static String addTrailingSlash(String folder)
    {
    	if(folder.endsWith("/"))
    	{
    		return folder;
    	}
    	else
    	{
    		return folder + FORWARD_SLASH;
    	}
    }
    
    /**
     * Checks if the given filename exists as a file and is a file (not directory)
     * 
     * @param folder		the name of the folder
     * @param filename		the name of the file to check
     * @return				indicator if the file exists and is a file
     */
    public static boolean fileExists(String folder, String filename)
    {
    	String fullFilename = addTrailingSlash(folder) + filename;
    	File file = new File(fullFilename);
    	if(file.exists() && file.isFile())
    	{
    		return true;
    	}
    	else
    	{
    		return false;
    	}
    }
    
    /**
     * Checks if the given filename exists as a file and is a file (not directory)
     * 
     * @param fullFilename	the complete path and name of the file
     * @return				indicator if the file exists and is a file
     */
    public static boolean fileExists(String fullFilename)
    {
    	File file = new File(fullFilename);
    	if(file.exists() && file.isFile())
    	{
    		return true;
    	}
    	else
    	{
    		return false;
    	}
    }
    
    public static void createFolders(String folder)
    {
    	if(folder!=null && !folder.trim().equals(""))
    	{
    		File directory = new File(folder);
    		directory.mkdirs();
    	}
    }
}
