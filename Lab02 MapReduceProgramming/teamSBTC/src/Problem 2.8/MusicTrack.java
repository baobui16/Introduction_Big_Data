import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;

public class MusicTrack {

    public static class MusicMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\\|");
            String userId = fields[0];
            String trackId = fields[1];
            int shared = Integer.parseInt(fields[2]);
            int radio = Integer.parseInt(fields[3]);
            int skip = Integer.parseInt(fields[4]);

            // emit key-value pairs for unique listeners, shared, radio, and total listens
            context.write(new Text("unique listeners"), new IntWritable(1));
            context.write(new Text("shared"), new IntWritable(shared));
            context.write(new Text("radio"), new IntWritable(radio));
            context.write(new Text("total listens"), new IntWritable(1));

            // emit key-value pair for skips on the radio
            if (radio == 1 && skip == 1) {
                context.write(new Text("skips on radio"), new IntWritable(1));
            }
        }
    }

    public static class MusicReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {

        // create a new job
        Job job = Job.getInstance();
        job.setJarByClass(MusicTrack.class);
        job.setJobName("Music Track");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // set mapper and reducer classes
        job.setMapperClass(MusicMapper.class);
        job.setReducerClass(MusicReducer.class);

        // set output key and value classes
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // submit the job and wait for completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

