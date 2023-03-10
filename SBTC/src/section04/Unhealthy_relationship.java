import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Unhealthy_relationship {

  public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{

    private final static  IntWritable firstValue = new IntWritable(100);
    private final static  IntWritable secondValue = new IntWritable(-1);
    
    private Text word = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException { 
    
    	StringTokenizer itr = new StringTokenizer(value.toString());
	int count = 0;
      	while (itr.hasMoreTokens()) {
      		if (count % 2 == 0){		
      			word.set(itr.nextToken());
        		context.write(word, firstValue);
      		}else {
      			word.set(itr.nextToken());
        		context.write(word, secondValue);      			
      		}
		count += 1;
      }
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,Text> {

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      String temp ;
      if (sum % 10 == 0){temp = "pos";}
      else if(sum < 0){temp = "neg";}
      else {temp = "eq";}
      context.write(key, new Text(temp));
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Unhealthy relationship");
    	
    job.setJarByClass(Unhealthy_relationship.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    
    job.setMapperClass(TokenizerMapper.class);
    //job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);

    	
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
