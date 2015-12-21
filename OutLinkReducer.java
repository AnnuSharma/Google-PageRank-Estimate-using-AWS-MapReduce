import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class OutLinkReducer extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    	Integer flag = new Integer(1);
    	
    	StringBuilder sb = new StringBuilder();
        for (Text value : values) {
        	if (flag == 1)
        	{
        		sb.append(String.valueOf(value));
        		flag = flag+1;
        	}
        	else
        		{
        		System.out.println("Token: "+value.toString());
        		sb.append("\t"+String.valueOf(value));
        		}
        }
        context.write(key, new Text(sb.toString()));		   			
    	}
    
}
    	  
