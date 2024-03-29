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

public class MaxTemp {
  public static class Map
       extends Mapper<Object, Text, Text, Text>{
		//set the delimiter to cut string
       final String DELIMITER = " ";
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
					// read line in file input
					String line = value.toString();  
					// cut string using .split with delimiter have been seted and save in array string
					String[] parts = line.split(DELIMITER); 
					// save first element for the variable Date with type String and 
					// second element for the variable Temp with type String
					String Date = parts[0];
					String Temp = parts[1];
					// ửite into hadoop
					context.write(new Text(Date), new Text(Temp));	
    }
  }

  public static class Reduce
       extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
						// create variable for save the max of temperature for each key
                       float max_Temp = 0;
					   // traverse each value in key
					   for (Text value : values) {
						// convert the value to string and after that convert to float and save the value in 
						// to temperature with type float 
						float temperature = Float.parseFloat(value.toString());
						//find the max temperature 
						if (temperature > max_Temp){
							max_Temp = temperature;
						}
					}
						//convert max temperature to string and write into hadoop
                    	String max_Temperature = String.valueOf(max_Temp);	
                    	context.write(key, new Text(max_Temperature));
      }
    }	
  public static void main(String[] args) throws Exception { 
 
    //reads the default configuration of cluster from the configuration xml files
	Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Max Temperature");
	
	//Assigning the driver class name
	job.setJarByClass(MaxTemp.class);
	
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


