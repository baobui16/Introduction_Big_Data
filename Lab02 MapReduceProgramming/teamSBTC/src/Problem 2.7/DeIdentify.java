import java.io.IOException;
import java.util.Arrays;	
import java.util.Collections;
import java.util.List;
import java.util.StringTokenizer;
import java.util.Base64;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
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
import org.apache.log4j.Logger;


public class DeIdentify {
	//Specify the parameters required to encrypt the data table's columns
       public static Integer[] encryptCol={2,3,4,5,6,8};
       private static byte[] key1 = new String("samplekey1234567").getBytes();
	   // Initialize the Logger object to start logging data.
       private static final Logger logger = Logger.getLogger(DeIdentify.class);

       
  public static class Map
       extends Mapper<Object, Text, NullWritable, Text>{

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
                    // convert the input line to string and cut to array by using comma
                    String[] parts = value.toString().split(",");
					// create variable with type StringBuilder for edit string
                    StringBuilder temp = new StringBuilder();
					// travel each element in array part 
                    for (int i = 0; i < parts.length; i++) {
						//check element need to encode
                    	if (Arrays.asList(encryptCol).contains(i)) {
							// add the value after encode into sb variable
							temp.append(encrypt(parts[i], key1));
                    	} // if index of element not need to encode , just add in to temp string 
						else {
							temp.append(parts[i]);
                    	}
						// add the comma for each element
                    	if (i < parts.length - 1) {
							temp.append(",");
                    	}
                    }
					// write into hadoop with key type NullWritable and value is the line after encode 
					context.write(NullWritable.get(), new Text(temp.toString()));
                          
      
    }
  }
  // get function encrypt in file introduce
  public static String encrypt(String strToEncrypt, byte[] key){
	try{
		Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5Padding");
		SecretKeySpec secretKey = new SecretKeySpec(key, "AES");
		cipher.init(Cipher.ENCRYPT_MODE, secretKey);
		String encryptedString = Base64.getEncoder().encodeToString(cipher.doFinal(strToEncrypt.getBytes()));
		return encryptedString.trim();
	}
	catch (Exception e){
		logger.error("Error while encrypting", e);
	}
	return null;
	}
  public static void main(String[] args) throws Exception { 
 
    //reads the default configuration of cluster from the configuration xml files
	Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "DeIdentify");
	
	//Assigning the driver class name
	job.setJarByClass(DeIdentify.class);
	
	//Defining the mapper class name
	job.setMapperClass(Map.class);
	//Defining the reducer class name
	//job.setReducerClass(Reduce.class);
	
	//Defining the output key class for the mapper
	//job.setMapOutputKeyClass(NullWritable.class);
	//Defining the output value class for the mapper
	//job.setMapOutputValueClass(Text.class);
	
	//Defining the output key class for the final output i.e. from reducer
	job.setOutputKeyClass(NullWritable.class);
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

