package com.freitas.commons;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

public class HDFSUtilities {

	private static final Logger LOG = Logger.getLogger(HDFSUtilities.class);

	public static void writeToFile(Configuration conf, String contents, String location) throws IOException {
		Path filePath = new Path(location);
		FileSystem fs = FileSystem.get(filePath.toUri(), conf); //URI conversion is required to support S3 and HDFS
		//if the file exists in that location delete it.
		if(fs.exists(filePath))  {
			fs.delete(filePath,false);
		}
		FSDataOutputStream fin =fs.create(filePath);	
		fin.writeBytes(contents);
		if(fin!=null){
			fin.close();
		}
	}

	public static String readFile(String fileLocation, Configuration conf ) throws IOException {
		StringBuilder fileContents = new StringBuilder();

		Path filePath = new Path(fileLocation);
		FileSystem fs = FileSystem.get(filePath.toUri(), conf); //URI conversion is required to support S3 and HDFS
		if(fs.exists(filePath))
		{
			BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(filePath)));
			String strLine;
			while((strLine=br.readLine())!=null)
			{
				fileContents.append(strLine);	
			}
		}
		else
		{
			LOG.error("File does not exist in the following location: " + fileLocation);
			throw new IOException(" File does not exist in the following location: " + fileLocation);
		}
		return fileContents.toString();
	}

	public static List<String> getDirFiles(FileSystem fs, Path outDir, String search) throws Exception {
        List<String> results = new ArrayList<String>();
        FileStatus[] fileStatus = fs.listStatus(outDir);
        for (FileStatus file : fileStatus) {
            String name = file.getPath().getName();
            if (name.contains(search)){
            	results.add(name);
            }
        }
        return results;
    }
	
	public static boolean deleteDir(String dirLocation, Configuration conf) throws Exception {
		Path dirPath = new Path(dirLocation);
		FileSystem fs = FileSystem.get(dirPath.toUri(), conf);
		if (fs.exists(dirPath)) {
			return fs.delete(dirPath, true);
		} else {
			return true;
		}
	}
	
}

