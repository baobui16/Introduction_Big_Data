import java.io.IOException;
import java.util.StringTokenizer;

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

public class WeatherData {
  public static class Map
       extends Mapper<Object, Text, Text, Text>{
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
					// read each line in file input
                    String line = value.toString();
                    // get year,days,month for Value ling using .substring in java
                    String year = line.substring(6, 10);
                    String days = line.substring(12,14);
                    String month = line.substring(10,12);
                    
					// get value max and min in line ,after that convert to type float by usng Float.parseFloat() in java
                    float temp_max = Float.parseFloat(line.substring(39, 45).trim());
                    float temp_min = Float.parseFloat(line.substring(47, 53).trim());
                    
					// check if the day have temperature max grather than 40 and min temperature less than 10 
					// Save key is the date have max ,min temperature correct and value is "hot day " and "cold day" with the date correct
                    if (temp_max > 40.0) {
                    context.write(new Text(days +"-"+month+"-" + year),new Text( "Hot Day"));
                    }
                    if (temp_min < 10.0) {
                    context.write(new Text(days +"-"+month+"-" + year), new Text("Cold Day"));
                    }	
    }
  }

  public static class Reduce
       extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
						//set output for each key with values's 
                       for (Text value : values) {
    					String temperature = value.toString();	
                    	context.write(key, new Text(temperature));
                    	}
      }
    }	
  public static void main(String[] args) throws Exception { 
 
    //reads the default configuration of cluster from the configuration xml files
	Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "WeatherData");
	
	//Assigning the driver class name
	job.setJarByClass(WeatherData.class);
	
	//Defining the mapper class name
	job.setMapperClass(Map.class);
	//Defining the reducer class name
	job.setReducerClass(Reduce.class);
	
	//Defining the output key class for the mapper
	job.setMapOutputKeyClass(Text.class);
	//Defining the output value class for the mapper
	job.setMapOutputValueClass(Text.class);
	
	//Defining the output key class for the final output i.e. from reducer
	job.setOutputKeyClass(Text.class);
	//Defining the output value class for the final output i.e. from reduce
	job.setOutputValueClass(Text.class);
	
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


