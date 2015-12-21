import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RedLinksReducer extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    	int i;
    	String bool_value = new String("False");
    	ArrayList<Text> cache = new ArrayList<Text>();
    	String value_string = new String();
    	System.out.println("ReducerKey"+key.toString());
    	for (Text val : values){
    		Text val_text = new Text();
    		val_text.set(val);
    		//caching for second iteration
            cache.add(val_text);
    		System.out.println("ReducerValues"+val);
    		value_string = val.toString();
    		if(value_string.matches("!!"))
    		{
    			//System.out.println("HERE_FIRST");
    			bool_value = "True";
    		}
        }
    	int size = cache.size();
    	System.out.println("Cache Size "+size);
    	if(bool_value.matches("True"))
    	{
        for(i = 0; i < size; i++) {
            Text val = cache.get(i);
            if(!((val.toString()).matches("!!")))
    		{
    			//System.out.println("HERE");
    			System.out.println(val.toString());
    			System.out.println("Writing"+val.toString()+" "+key.toString());
    			context.write(val,key);
    		}
            else
        	{	
        		context.write(key,new Text(""));
        	}
        }
    	}  
   }
}
    	  
