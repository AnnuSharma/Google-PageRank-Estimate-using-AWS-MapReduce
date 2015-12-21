import java.io.IOException;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PageRankBMapper extends Mapper<LongWritable, Text, Text, Text> {

		
		private Text word = new Text();
		private Text word_title = new Text();
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {   
		Double N = new Double(0);
		Integer I = new Integer(0);
		Configuration conf = context.getConfiguration();
		String N_String= conf.get("N");
		String It = conf.get("Iter");
		N = Double.parseDouble(N_String);
		I = Integer.parseInt(It);
		
        String line = value.toString();
        String wiki_links=new String();
        String title=new String();
        Double initial_wt=(double)(1.0/N);
        String weighted_string=new String();
        
        if(I==1)
        {

	        Scanner sc = null;
	        try {
	            sc = new Scanner(line);
	        } catch (Exception e) {
	            e.printStackTrace();  
	        }
	        while (sc.hasNextLine()) 
	        {
	        	wiki_links=sc.nextLine();
	        	title =	 wiki_links.split("\t")[0];
	            weighted_string=wiki_links.toString().split("\t",2)[1];      
	            StringTokenizer itr = new StringTokenizer(weighted_string, "\t");
	            int length_wikilinks=itr.countTokens();      
	            word_title.set(title);
	            Double inter_outlink_wt=(double)((1.0/N)/length_wikilinks);
	       
	            while(itr.hasMoreTokens()) 
	            {
		            word.set(itr.nextToken());
		            context.write(word_title,word);
		            context.write(word,new Text(inter_outlink_wt.toString()));
	            }
	        
	        context.write(word_title,new Text(initial_wt.toString()));
	    
	        }sc.close();
       }
       else{
    	  
		  
		    //use scanner class to append weight at the beginning then tokenize
		        Scanner sc = null;
		        try {
		            sc = new Scanner(line);
		        } catch (Exception e) {
		            e.printStackTrace();  
		        }
		        while (sc.hasNextLine()) 
		        {
		        	
		        	 wiki_links=sc.nextLine();
	            	 String[] bits = wiki_links.split("\t");
	            	 String lastOne = bits[bits.length-1];
	            	 Double pg = Double.parseDouble(lastOne);
	            	 title=	 wiki_links.split("\t")[0];
	            	 weighted_string=wiki_links.toString().split("\t",2)[1];
	            	 String[] wiki_tokens = weighted_string.split("\t"); 
	            	 String wiki_tokens_final = new String();
	            	 for(int i=0;i<wiki_tokens.length-1;i++)
	            	 {
	            		 wiki_tokens_final = wiki_tokens_final + "\t" + wiki_tokens[i];
	            	 }
			        System.out.println("Final wiki token: "+wiki_tokens_final);
			        StringTokenizer itr = new StringTokenizer(wiki_tokens_final, "\t");
			        int length_wikilinks=itr.countTokens();
			        System.out.println("Num of tokens: "+length_wikilinks);
			       
			        word_title.set(title);
			  
			        Double inter_outlink_wt=(double)((pg)/length_wikilinks);
			     
			        while(itr.hasMoreTokens()) 
			        {
			            word.set(itr.nextToken());
			            System.out.println("Title: "+word_title+"Nodes: "+word);
			            context.write(word_title,word);
			            context.write(word,new Text(inter_outlink_wt.toString()));
			            System.out.println("Key: "+word+"Value: "+new Text(inter_outlink_wt.toString()).toString());
			            
		        
			        }
		        context.write(word_title,new Text(pg.toString()));	    
		        }sc.close();

       		}
    
		}
	}