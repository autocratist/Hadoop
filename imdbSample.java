import java.io.IOException;
import java.util.Scanner;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class imdbSample {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, Text>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
//      StringTokenizer itr = new StringTokenizer(value.toString());
//      while (itr.hasMoreTokens()) {
//        word.set(itr.nextToken());
//        context.write(word, one);
//      }
    	
//    	String[] lines = value.toString().split(System.getProperty("line.separator"));
 
//    	System.err.println("key"+lines[0] + "\tvalue"+lines[1]);
       	//context.write(new Text(lines[0]), new Text(lines[1]));
        String movie = "null";
        String rating[] = {"null","null"};
    Scanner scanner = new Scanner(value.toString());
    while (scanner.hasNextLine()) {
      String line = scanner.nextLine();
//    String line = value.toString();
      System.err.println("Text"+line);

      if(line.startsWith("MV")){
    	  String splitLine[]= line.split(":");
    	  movie = splitLine[1];
    	
    	}
      if(line.startsWith("RE")){
    	  String splitLine1[]= line.split(":");
//    	  rating[1] = splitLine1[1];
    	  rating = splitLine1[1].split("\\s+");
    	
    	 //break;
   
    	}
  
      
     
//    	context.write(new Text(movie[1]),new Text(rating[1]));
  	  
      // process the line
    }
    scanner.close();
//    
    if(rating[1].equals("Rated")){
    	 System.err.println("key"+movie + "\tvalue"+rating[2]);
    	 context.write(new Text(movie),new Text(rating[2]));
    }
   }

  }

  public static class IntSumReducer
       extends Reducer<Text,Text,Text,Text> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
//      int sum = 0;
//      for (IntWritable val : values) {
//        sum += val.get();
//      }
//      result.set(sum);
    	for (Text val : values) {
      context.write(key, val);
    	}
    }
  }

  public static void main(String[] args) throws Exception {
//    Configuration conf = new Configuration();
    Configuration conf = new Configuration(true);
    conf.set("textinputformat.record.delimiter","-------------------------------------------------------------------------------");
//    conf.set("textinputformat.record.delimiter", ":");
    Job job = new Job(conf);
	job.setJobName("wordcount imdb");

    job.setJarByClass(imdbSample.class);
    job.setMapperClass(TokenizerMapper.class);
    //job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setInputFormatClass(CustomFileInputFormat.class);
   
//    job.setInputFormatClass(NLineInputFormat.class);
//    NLineInputFormat.addInputPath(job, new Path("D:\\input\\imdb.txt"));
//    job.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", 5);

   	
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    
    
    /*Creating Filesystem object with the configuration*/
    FileSystem fs = FileSystem.get(conf);
    /*Check if output path (args[1])exist or not*/
    if(fs.exists(new Path("D:\\output"))){
       /*If exist delete the output path*/
       fs.delete(new Path("D:\\output"),true);
    }
    
    FileInputFormat.addInputPath(job, new Path("D:\\input\\mpaa-ratings-reasons.list"));
    FileOutputFormat.setOutputPath(job, new Path("D:\\output"));
        
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}