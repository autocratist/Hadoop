import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SecondarySortBasicDriver {

	public static void main(String[] args) throws Exception {
	Configuration conf= new Configuration();
	
	//conf.setJobName("mywc");
	Job job = new Job(conf,"Secondary sort example");
	

		//Job job = new Job(getConf());
		//job.setJobName("Secondary sort example");

	//job.setJarByClass(SecondarySortBasicDriver.class);
//		FileInputFormat.setInputPaths(job, new Path(args[0]));
//		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
	    FileInputFormat.addInputPath(job, new Path("D:\\input\\input1.txt"));
	    FileOutputFormat.setOutputPath(job, new Path("D:\\output"));
			
		//deleting the output path automatically from hdfs so that we don't have delete it explicitly
			
		Path outputPath= new Path ("D:\\output");
		outputPath.getFileSystem(conf).delete(outputPath);

		job.setMapperClass(SecondarySortBasicMapper.class);
		job.setMapOutputKeyClass(CompositeKeyWritable.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setPartitionerClass(SecondarySortBasicPartitioner.class);
		job.setSortComparatorClass(SecondarySortBasicCompKeySortComparator.class);
		job.setGroupingComparatorClass(SecondarySortBasicGroupingComparator.class);
		job.setReducerClass(SecondarySortBasicReducer.class);
		job.setOutputKeyClass(CompositeKeyWritable.class);
		job.setOutputValueClass(NullWritable.class);

		job.setNumReduceTasks(8);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}