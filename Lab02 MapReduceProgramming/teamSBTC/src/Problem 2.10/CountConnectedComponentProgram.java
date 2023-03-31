import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CountConnectedComponentProgram {

    public static class Map
            extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] tokens = value.toString().split(" ");
            int node = Integer.parseInt(tokens[0]);
            int[] neighbors = new int[tokens.length - 1];
            for (int i = 1; i < tokens.length; i++) {
                neighbors[i - 1] = Integer.parseInt(tokens[i]);
            }
            Arrays.sort(neighbors);

            for (int i = 0; i < neighbors.length; i++) {
                for (int j = i + 1; j < neighbors.length; j++) {
                    context.write(new IntWritable(neighbors[i]), new IntWritable(neighbors[j]));
                    context.write(new IntWritable(neighbors[j]), new IntWritable(neighbors[i]));
                }
                context.write(new IntWritable(neighbors[i]), new IntWritable(node));
                context.write(new IntWritable(node), new IntWritable(neighbors[i]));
            }
        }
    }

    public static class Reduce
            extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

        public void reduce(IntWritable key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int node = key.get();
            HashSet<Integer> neighbors = new HashSet<Integer>();
            for (IntWritable value : values) {
                neighbors.add(value.get());
            }

            HashSet<Integer> component = new HashSet<Integer>();
            component.add(node);

            boolean added;
            do {
                added = false;
                HashSet<Integer> newComponent = new HashSet<Integer>();
                for (int neighbor : neighbors) {
                    if (component.contains(neighbor)) {
                        continue;
                    }
                    added = true;
                    newComponent.add(neighbor);
                }
                component.addAll(newComponent);
            } while (added);

            context.write(key, new IntWritable(component.size()));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "count connected component program");

        job.setJarByClass(CountConnectedComponentProgram.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

