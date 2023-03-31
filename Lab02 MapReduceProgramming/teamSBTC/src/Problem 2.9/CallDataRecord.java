import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.text.ParseException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class CallDataRecord {

  public static class Map
       extends Mapper<Object, Text, Text, LongWritable>{
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
            
                    String[] parts = value.toString().split("[|]");
//                    long duration = 1;
//                    String t = parts[0] + parts[1] + parts[2] + parts[3] + parts[4];
//                    context.write(new Text(t), new LongWritable(duration));
                    if (parts[4].equalsIgnoreCase("1")) {
                    String phoneNumber = parts[0];
                    String callEndTime = parts[3];
                    String callStartTime = parts[2];
                    long duration = toMinutes(callEndTime) - toMinutes(callStartTime);
                    context.write(new Text(phoneNumber), new LongWritable(duration));
                    }

			

    }
    private long toMinutes(String date) {
    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    Date dateFrm = null;
    try {
        dateFrm = format.parse(date);
    } catch (ParseException e) {
        e.printStackTrace();
    }
    return dateFrm.getTime() / (60 * 1000);
}
  }

  public static class Reduce
       extends Reducer<Text, LongWritable, Text, LongWritable> {

    public void reduce(Text key, Iterable<LongWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
                       long sum = 0;
                      	for (LongWritable value : values){
                      	sum+=value.get();
                      	}
                      	if (sum >= 60){
                      	context.write(key, new LongWritable(sum));
                      	}

      }
    }
    	
  public static void main(String[] args) throws Exception { 
 
    	//reads the default configuration of cluster from the configuration xml files
	Configuration conf = new Configuration();
    	Job job = Job.getInstance(conf, "CallDataRecord");
	
	//Assigning the driver class name
	job.setJarByClass(CallDataRecord.class);
	
	//Defining the mapper class name
	job.setMapperClass(Map.class);
	//Defining the reducer class name
	job.setReducerClass(Reduce.class);
	
	//Defining the output key class for the mapper
	job.setMapOutputKeyClass(Text.class);
	//Defining the output value class for the mapper
	job.setMapOutputValueClass(LongWritable.class);
	
	//Defining the output key class for the final output i.e. from reducer
	job.setOutputKeyClass(Text.class);
	//Defining the output value class for the final output i.e. from reduce
	job.setOutputValueClass(LongWritable.class);
	
	//Defining input Format class which is responsible to parse the dataset into a key value pair 
	job.setInputFormatClass(TextInputFormat.class);
	//Defining output Format class which is responsible to parse the final key-value output from MR framework to a text file into the hard disk
	job.setOutputFormatClass(TextOutputFormat.class);
	 
	 //setting the second argument as a path in a path variable
    	Path outputPath = new Path(args[1]);
			
	 //Configuring the input/output path from the filesystem into the job
	FileInputFormat.addInputPath(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));
			
	 //deleting the output path automatically from hdfs so that we don't have delete it explicitly
	outputPath.getFileSystem(conf).delete(outputPath);
	  //exiting the job only if the flag value becomes false
	System.exit(job.waitForCompletion(true) ? 0 : 1);
       }
}


