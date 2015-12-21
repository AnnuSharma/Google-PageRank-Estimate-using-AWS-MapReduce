import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PageRankReducer extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    	CheckIfDouble cd=new CheckIfDouble();
    	Double sum = new Double(0);
    	System.out.println("key :"+ key);
    	for(Text v : values){
        	System.out.println("value : "+v);
    		if(cd.isNumeric(v.toString()))
    		{
    			double val=Double.parseDouble(v.toString());
                sum +=  val;	
    		}
        	
    	}	
        context.write(key,new Text(sum.toString()));
    
}
}