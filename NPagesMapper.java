import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.*;
import java.io.BufferedReader;
import java.io.FileReader;

public class NPagesMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	Text key_common = new Text();
	IntWritable one = new IntWritable(1);
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    	//Text temp = new Text();
    	key_common.set("Key");
    	String line = new String();
    	line = value.toString();
    	Scanner sc = null;
        try {
            sc = new Scanner(line);
        } catch (Exception e) {
            e.printStackTrace();  
        }
        while (sc.hasNextLine()) {
        	String line1 = sc.nextLine().toString();
        	context.write(key_common,one);
        }
    }
}