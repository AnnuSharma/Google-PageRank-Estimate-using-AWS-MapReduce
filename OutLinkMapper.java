import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.*;
import java.io.BufferedReader;
import java.io.FileReader;

public class OutLinkMapper extends Mapper<LongWritable, Text, Text, Text>{
	private Text title = new Text();
	private Text link = new Text();
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    	String s2 = new String();
    	String line = new String();
    	line = value.toString();
    	Scanner sc = null;
        try {
            sc = new Scanner(line);
        } catch (Exception e) {
            e.printStackTrace();  
        }
        while (sc.hasNextLine()) {
                Scanner scan = new Scanner(sc.nextLine());
                String s1 = scan.next();
                try
                {
                	s2 = scan.next();   
                }
                catch(Exception e){
                	s2 = "";
                }
                title.set(s1);
                link.set(s2);  
                context.write(title, link);
        }
    }
}