import java.io.IOException;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class CobolCombined {

  public static class MyMapper
       extends Mapper<Object, Text, Text, Text>{

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {

    String name = "empty",type = "empty";
    Scanner scanner = new Scanner(value.toString());
    while (scanner.hasNextLine()) {
      String line = scanner.nextLine();
      
     if(line.contains("PROGRAM-ID")){
    	  String splitLine[]= line.split(" ");
    	  name = splitLine[1];    	
    	}      
      
      if(line.contains("EXEC CICS") && !(line.contains("RETURN"))){
    	  String splitLine[]= line.split(" ");
    	  	  type = splitLine[2]+" "+splitLine[3]; 
    	  	  System.err.println("name "+name+" type "+type);
    	  	  context.write(new Text(name),new Text(type));
    	}    
      }
    scanner.close();   
    }

  }

  public static class MyReducer
       extends Reducer<Text,Text,Text,Text> {
    

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
    	String funcs = null;
    	
    	for (Text val : values) {
    		
    		System.err.println("key"+key +" value"+val.toString());
    		if(funcs == null){
    		funcs = val.toString() + "\t";
    		}else
    		{
    		funcs += val.toString()+"\t";
       		}		
    	}
    	
    	context.write(key, new Text(funcs));
    }
  }

  public static void main(String[] args) throws Exception {

	Configuration conf = new Configuration(true);
   // conf.set("textinputformat.record.delimiter","IDENTIFICATION");
    Job job = new Job(conf);

    job.setJarByClass(CobolCombined.class);
    job.setMapperClass(MyMapper.class);
    job.setReducerClass(MyReducer.class);
    	
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    
    job.setInputFormatClass(WholeFileInputFormat.class);
    
     /*Creating Filesystem object with the configuration*/
    FileSystem fs = FileSystem.get(conf);
    /*Check if output path (args[1])exist or not*/
    if(fs.exists(new Path("D:\\output"))){
       /*If exist delete the output path*/
       fs.delete(new Path("D:\\output"),true);
    }
    
    FileInputFormat.addInputPath(job, new Path("D:\\input11"));
   // FileInputFormat.addInputPath(job, new Path("D:\\input\\cobol.txt"));
    FileOutputFormat.setOutputPath(job, new Path("D:\\output"));
        
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}