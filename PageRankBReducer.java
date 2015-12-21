
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PageRankBReducer extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Integer I = new Integer(0);
        String new_tokens=new String();
    	CheckIfDouble cd=new CheckIfDouble();
    	Double sum = new Double(0);
    	Configuration conf = context.getConfiguration();
		String It = conf.get("Iter");
		I = Integer.parseInt(It);
        if(I==8)
        {
        	for(Text v : values){
        		if(cd.isNumeric(v.toString()))
        		{
        			double val=Double.parseDouble(v.toString());
                    sum +=  val;	
        		}
            	
        	}	
            context.write(key,new Text(sum.toString()));
        
        }
        else
           {
        	for(Text v : values)
        	{  
	    		if(cd.isNumeric(v.toString()))
	    		{
	    			double val=Double.parseDouble(v.toString());
	                sum +=  val;	
	    		}
	    		else{
	    			new_tokens=new_tokens+"\t"+v.toString();
	    		}
        	
        	}
			new_tokens=new_tokens+"\t"+sum.toString();
	
	        context.write(key,new Text(new_tokens));
           }    
    }
}