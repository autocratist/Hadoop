package indiaweather;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ChennaiRain {
	public static class Mymapper extends Mapper<LongWritable,Text,Text,Text>
	{
	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	@Override
	public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException
		{
			Text year = new Text();
			Text rainmax = new Text();
			String[] line = null;
			String[] raindata;
			String [] months = {"January","February", "March", "April", "May", "June", "July", "August", "September", "October", "November","December"};
						
			 line = value.toString().split("\\t");
			 float maxTemp =0;	
			 int maxIndex =0;
			 		 
			 
			 if(line[1].contains("Chennai"))
			 {
			 year.set(new Text(line[2]));
			 raindata = Arrays.copyOfRange(line, 3, line.length);
			 rainmax.set(new Text(Arrays.toString(raindata)));
			 maxTemp =0;
			 
			 for(int i=0; i<raindata.length; i++){
				 if(maxTemp < Float.parseFloat(raindata[i]))
			       {
					 maxTemp = Float.parseFloat(raindata[i]);
					 maxIndex = i;
			       }	
			 	}
			 
			 System.out.println("Year:"+year+"Max Temp"+maxTemp+"Month"+ months[maxIndex]);
		 	 context.write(year, new Text(String.valueOf(maxTemp)+" on "+months[maxIndex]));
//		 			 FloatWritable(maxTemp));
			 }		
		}	 		
		}
			    

	public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
		Job job = new Job(conf,"word count");	  
//	   Job job = Job.getInstance(conf, "word count");
	    job.setJarByClass(ChennaiRain.class);
	    job.setMapperClass(Mymapper.class);
//	    job.setReducerClass(Chenreducer.class);
	    
	    FileSystem fs = FileSystem.get(new Configuration());
	 // true stands for recursively deleting the folder you gave
	 fs.delete(new Path("H:\\jars\\output"), true);    
//	    job.setReducerClass(Rainreducer.class);
	    //job.setOutputKeyClass(IntWritable.class);
	   // job.setOutputValueClass(IntWritable.class);
	 	job.setMapOutputKeyClass(Text.class);
	 	job.setMapOutputValueClass(Text.class);
	 	job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    job.setInputFormatClass(ExcelInputFormat.class);
	    FileInputFormat.addInputPath(job, new Path("H:\\jars\\input\\india_-_monthly_rainfall_data_-_1901_to_2002.xls"));
	    FileOutputFormat.setOutputPath(job, new Path("H:\\jars\\output"));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }
	}