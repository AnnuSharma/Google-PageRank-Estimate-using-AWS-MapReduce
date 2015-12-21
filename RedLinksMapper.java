import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.*;

public class RedLinksMapper extends Mapper<LongWritable, Text, Text, Text>{
	private Text title = new Text();
	private Text wikilinks = new Text();
    private Text special_char = new Text("!!");
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    	Set<String> wikilinks_set = new HashSet<String>();
    	//Extracting the title
        String title_token = value.toString();
        title_token = title_token.split("<title>")[1].split("</title>")[0];
        title_token = title_token.replace(" ", "_");
        title.set(title_token);
        context.write(title,special_char);
        //Extracting the wikilinks
        String wiki_token = value.toString();
        //Need to extract each word and test against regex
        String temp1 = wiki_token.split("<text")[1].split("</text>")[0];
        Matcher m = Pattern.compile("\\[\\[[.-[^\\|:\\[\\]{}#<>]]+[\\|]?[.-[^\\|:\\[\\]{}#<>]]*\\]\\]").matcher(temp1);
        while (m.find()) {
        	System.out.println("HERE");
        	String matched_wikilink = m.group(0);
        	System.out.println("Matched Wikilink: "+matched_wikilink);
        	matched_wikilink = matched_wikilink.split("\\[\\[")[1].split("\\]\\]")[0];
        	matched_wikilink = matched_wikilink.split("\\|")[0];
        	matched_wikilink = matched_wikilink.replace(" ", "_");
        	wikilinks.set(matched_wikilink);
        	wikilinks_set.add(matched_wikilink);
        	//System.out.println("Matched Wikilink: "+matched_wikilink);
        }  
        for(String wikilink:wikilinks_set)
        	{   Text wiki = new Text();
        		wiki.set(wikilink);
        	    System.out.println("WikilinkSet"+wiki.toString());
        		context.write(wiki,title);
        	}
    }
}