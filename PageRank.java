import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.*;
import java.io.*;
import java.nio.*;
import java.lang.*;
import org.apache.commons.io.FileUtils;

public class PageRank extends Configured implements Tool  {
	public static void main(String[] args) throws Exception
	{
        int res = ToolRunner.run(new Configuration(), new PageRank(), args);
        System.exit(res);	
	}
	public int run(String args[]) 
	{	
		 //Running Job 1 - Removal of RedLinks
		 try {
	            Configuration conf = new Configuration();
	            conf.set("xmlinput.start", "<page>");
	            conf.set("xmlinput.end", "</page>");
	            conf
	            .set(
	            "io.serializations",
	            "org.apache.hadoop.io.serializer.JavaSerialization,org.apache.hadoop.io.serializer.WritableSerialization");
	            Job job = Job.getInstance(conf);
	            job.setJarByClass(PageRank.class);
	            job.setNumReduceTasks(1);

	            // specify a mapper
	            job.setMapperClass(RedLinksMapper.class);

	            // specify a reducer
	            job.setReducerClass(RedLinksReducer.class);

	            // specify output types
	            job.setOutputKeyClass(Text.class);
	            job.setOutputValueClass(Text.class);

	            // specify input and output DIRECTORIES
	            FileInputFormat.setInputPaths(job, new Path(args[0]));
	            job.setInputFormatClass(XmlInputFormat.class);

	            FileOutputFormat.setOutputPath(job, new Path(args[1]+"/results/RedLinksRemoval.out"));
	            job.setOutputFormatClass(TextOutputFormat.class);

	            job.waitForCompletion(true);
	        } 
		 	catch (InterruptedException|ClassNotFoundException|IOException e) 
		 	{
	            System.err.println("Error during mapreduce job.");
	            e.printStackTrace();
	            return 2;
	        }
		 //Running Job 2 - Creating the OutLink Graph
		 try {
	            Configuration conf = new Configuration();
	            Job job = Job.getInstance(conf);
	            job.setJarByClass(PageRank.class);
	            job.setNumReduceTasks(1);
	            
	            // specify a mapper
	            job.setMapperClass(OutLinkMapper.class);

	            // specify a reducer
	            job.setReducerClass(OutLinkReducer.class);

	            // specify output types
	            job.setOutputKeyClass(Text.class);
	            job.setOutputValueClass(Text.class);

	            // specify input and output DIRECTORIES
	            FileInputFormat.addInputPath(job, new Path(args[1]+"/results/RedLinksRemoval.out"));
	            job.setInputFormatClass(TextInputFormat.class);

	            FileOutputFormat.setOutputPath(job, new Path(args[1]+"/results/PageRank.outlink.out"));
	            job.setOutputFormatClass(TextOutputFormat.class);

	            job.waitForCompletion(true);
	        } 
		 	catch (InterruptedException|ClassNotFoundException|IOException e)
		 	{
	            System.err.println("Error during mapreduce job.");
	            e.printStackTrace();
	            return 2;
	        }
		 //Running Job 3 - Calculating the number of distinct titles
		 try {
	            Configuration conf = new Configuration();
	            conf.set("mapreduce.output.key.field.separator","\n");
	            Job job = Job.getInstance(conf);
	            job.setJarByClass(PageRank.class);
	            job.setNumReduceTasks(1);

	            // specify a mapper
	            job.setMapperClass(NPagesMapper.class);

	            // specify a reducer
	            job.setReducerClass(NPagesReducer.class);
	            //job.setCombinerClass(NPagesReducer.class);
	            
	            // specify output types
	            job.setOutputKeyClass(Text.class);
	            job.setOutputValueClass(IntWritable.class);

	            // specify input and output DIRECTORIES
	            FileInputFormat.addInputPath(job, new Path(args[1]+"/results/PageRank.outlink.out"));
	            job.setInputFormatClass(TextInputFormat.class);

	            FileOutputFormat.setOutputPath(job, new Path(args[1]+"/results/PageRank.n.out"));
	            job.setOutputFormatClass(TextOutputFormat.class);

	            job.waitForCompletion(true);
	        }   
		 	catch (InterruptedException|ClassNotFoundException|IOException e) 
		 	{
	            System.err.println("Error during mapreduce job.");
	            e.printStackTrace();
	            return 2;
	        }
		 //Running Job 4 - First Iteration of PageRank
		 try {
	        	Double N = new Double(0);
	        	String String_N = new String();       	
	        	Path pt=new Path(args[1]+"/results/PageRank.n.out/part-r-00000");
	            FileSystem fs = pt.getFileSystem(new Configuration());
	            BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
	            String line = new String();
		        while((line = br.readLine())!=null){
		        String_N = line.split("=")[1];
		        N = Double.parseDouble(String_N);
		        } 
	            Configuration conf = new Configuration();
	            conf.set("N",String_N);
	            Job job = Job.getInstance(conf);
	            job.setJarByClass(PageRank.class);
	            job.setNumReduceTasks(1);
	            
	            // specify a mapper
	            job.setMapperClass(PageRankMapper.class);

	            // specify a reducer
	            job.setReducerClass(PageRankReducer.class);

	            // specify output types
	            job.setOutputKeyClass(Text.class);
	            job.setOutputValueClass(Text.class);

	            FileInputFormat.addInputPath(job, new Path(args[1]+"/results/PageRank.outlink.out"));
	            job.setInputFormatClass(TextInputFormat.class);

	            FileOutputFormat.setOutputPath(job, new Path(args[1]+"/results/PageRank.iter1.out"));
	            job.setOutputFormatClass(TextOutputFormat.class);

	            job.waitForCompletion(true);
	            SortPageRank obj = new SortPageRank();
	            obj.sort(args[1]+"/results/PageRank.iter1.out/part-r-00000",N);
	        } 
		 	catch (InterruptedException|ClassNotFoundException|IOException e) 
		 	{
	            System.err.println("Error during mapreduce job.");
	            e.printStackTrace();
	            return 2;
	        }
		 //Running Job 5 - 8 Iterations of PageRank
		 //Extracting N from job 3
		   Double N = new Double(0);
		   String String_N = new String();
		   try
		   {
			   String_N = new String();       	
			   Path pt=new Path(args[1]+"/results/PageRank.n.out/part-r-00000");
			   FileSystem fs = pt.getFileSystem(new Configuration());
			   BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
			   String line = new String();
			   while((line = br.readLine())!=null)
		       {
		       String_N = line.split("=")[1];
		       N = Double.parseDouble(String_N);
		       } 
		   }
		   catch(Exception e)
		   {
			   e.printStackTrace();
		   }
	       
	       for(int i=1;i<=8;i++)
	       {
	       if(i==1)
	       {
		   try {
	           Configuration conf = new Configuration();
	           conf.set("N",String_N);
	           conf.set("Iter","1");
	           Job job = Job.getInstance(conf);
	           job.setJarByClass(PageRank.class);
	           job.setNumReduceTasks(1);

	           // specify a mapper
	           job.setMapperClass(PageRankBMapper.class);

	           // specify a reducer
	           job.setReducerClass(PageRankBReducer.class);

	           // specify output types
	           job.setOutputKeyClass(Text.class);
	           job.setOutputValueClass(Text.class);

	           // specify input and output DIRECTORIES
	           FileInputFormat.addInputPath(job,new Path(args[1]+"/results/PageRank.outlink.out"));
	           job.setInputFormatClass(TextInputFormat.class);

	           FileOutputFormat.setOutputPath(job,new Path(args[1]+"/tmp/PageRank.iter"+i+".out"));
	           job.setOutputFormatClass(TextOutputFormat.class);

	           job.waitForCompletion(true);
	           
	       } catch (InterruptedException|ClassNotFoundException|IOException e) {
	           System.err.println("Error during mapreduce job.");
	           e.printStackTrace();
	           return 2;
	           
	       }
	   }
	       else if(i==8)
	       {
	    	   try {
		           Configuration conf = new Configuration();
		           conf.set("N",String_N);
		           conf.set("Iter","8");
		           Job job = Job.getInstance(conf);
		           job.setJarByClass(PageRank.class);
		           job.setNumReduceTasks(1);

		           // specify a mapper
		           job.setMapperClass(PageRankBMapper.class);

		           // specify a reducer
		           job.setReducerClass(PageRankBReducer.class);

		           // specify output types
		           job.setOutputKeyClass(Text.class);
		           job.setOutputValueClass(Text.class);

		           // specify input and output DIRECTORIES
		           FileInputFormat.addInputPath(job, new Path(args[1]+"/tmp/PageRank.iter"+(i-1)+".out"));
		           job.setInputFormatClass(TextInputFormat.class);

		           FileOutputFormat.setOutputPath(job, new Path(args[1]+"/results/PageRank.iter"+i+".out"));
		           job.setOutputFormatClass(TextOutputFormat.class);

		           job.waitForCompletion(true);
		           SortPageRank obj = new SortPageRank();
		           obj.sort(args[1]+"/results/PageRank.iter"+i+".out"+"/part-r-00000",N);
		           
		       } catch (InterruptedException|ClassNotFoundException|IOException e) {
		           System.err.println("Error during mapreduce job.");
		           e.printStackTrace();
		           return 2;
		           
		       }
	       }
	       else
	       {
	    	   try {
		           Configuration conf = new Configuration();
		           conf.set("N",String_N);
		           conf.set("Iter","0");
		         
		           Job job = Job.getInstance(conf);
		           job.setJarByClass(PageRank.class);
		           job.setNumReduceTasks(1);

		           // specify a mapper
		           job.setMapperClass(PageRankBMapper.class);

		           // specify a reducer
		           job.setReducerClass(PageRankBReducer.class);

		           // specify output types
		           job.setOutputKeyClass(Text.class);
		           job.setOutputValueClass(Text.class);

		           // specify input and output DIRECTORIES
		           FileInputFormat.addInputPath(job, new Path(args[1]+"/tmp/PageRank.iter"+(i-1)+".out"));
		           job.setInputFormatClass(TextInputFormat.class);

		           FileOutputFormat.setOutputPath(job, new Path(args[1]+"/tmp/PageRank.iter"+i+".out"));
		           job.setOutputFormatClass(TextOutputFormat.class);

		           job.waitForCompletion(true);
		           
		       } catch (InterruptedException|ClassNotFoundException|IOException e) {
		           System.err.println("Error during mapreduce job.");
		           e.printStackTrace();
		           return 2;
		           
		       }
	    	   
	       }
	       
	     }
	       return 1;
		 
	}
}
