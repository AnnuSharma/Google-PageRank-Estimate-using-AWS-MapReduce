import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.NullWritable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class NPagesReducer extends Reducer<Text, IntWritable,NullWritable,Text> {

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
    	IntWritable sum_final = new IntWritable();
    	Integer sum = new Integer(0);
    	Text output = new Text("N=");
        for (IntWritable value : values) {
            sum = sum + 1;
        }
       String temp = new String();
       temp = sum.toString();
       temp = "N="+temp;
       output.set(temp);
       context.write(NullWritable.get(),output);		   			
    }
    
}
    	  
