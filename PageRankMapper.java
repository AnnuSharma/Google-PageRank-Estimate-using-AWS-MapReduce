import java.io.IOException;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PageRankMapper extends Mapper<LongWritable, Text, Text, Text> {

	private final IntWritable one = new IntWritable(1);
	private Text word = new Text();
	private Text word_title = new Text();
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Double N = new Double(0);
		Configuration conf = context.getConfiguration();
		String N_String= conf.get("N");
		N = Double.parseDouble(N_String);
        String line = value.toString();
        String wiki_links=new String();
        String title=new String();
        Double initial_wt=(double)(1.0/N);
        String weighted_string=new String();
        
        Scanner sc = null;
        try {
            sc = new Scanner(line);
        } catch (Exception e) {
            e.printStackTrace();  
        }
        while (sc.hasNextLine()) 
        {
            	 wiki_links=sc.nextLine();
            	 System.out.println("Next line: "+wiki_links);
            	 title=	 wiki_links.split("\t")[0];
            	 System.out.println("Next line title: "+title);
               	 weighted_string=wiki_links.toString().split("\t",2)[1];
            	 System.out.println("Weighted String: "+weighted_string);
            	 StringTokenizer itr = new StringTokenizer(weighted_string, "\t");
            	 int length_wikilinks=itr.countTokens();
            	 System.out.println("Num of tokens: "+length_wikilinks);  
            	 word_title.set(title); 
            	 Double inter_outlink_wt=(double)((1.0/N)/length_wikilinks);
            	 while(itr.hasMoreTokens())
            	 {
            		 word.set(itr.nextToken());
            		 System.out.println("Title: "+word_title+"Nodes: "+word);
            		 context.write(word_title,word);
            		 context.write(word,new Text(inter_outlink_wt.toString()));
            		 System.out.println("Key: "+word+"Value: "+new Text(inter_outlink_wt.toString()).toString());
       
            	 }
        
            	 context.write(word_title,new Text(initial_wt.toString()));
        }
        sc.close();
	}
}