import java.util.ArrayList;
import java.util.Collections;
import java.util.Scanner;
import java.util.*;
import java.io.*;
import java.net.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;

public class SortPageRank {
	public void sort(String file, Double N)
	{
		try
		{
			Path pt=new Path(file);
			FileSystem fs = pt.getFileSystem(new Configuration());
	        BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
			Map<Double, String> map=new TreeMap<Double, String>(Collections.reverseOrder());
	        String line = new String();
	        while((line = br.readLine())!=null){
	        	String pg = getField(line);
	        	System.out.println(pg);
	        	Double pg_double = Double.parseDouble(pg);
	        	System.out.println((double)(5.0/N));
	        	if ((pg_double >= (double)(5.0/N)))
	        	{
	        		System.out.println("PG: "+pg_double);
	        		map.put(pg_double,line);
	        	}
	        		        	
	        }
	        br.close();
	        FileSystem fs_write = pt.getFileSystem(new Configuration());
	        FSDataOutputStream out = fs_write.create(new Path(file),true);
	        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(out));
	        for(String val : map.values()){
	        	System.out.println("Writing: "+val);
	        	bw.write(val);	
	        	bw.write('\n');
	        }
	        bw.close();
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
      
    }
	private static String getField(String line) {
    	return line.split("\t")[1];
    }
}
