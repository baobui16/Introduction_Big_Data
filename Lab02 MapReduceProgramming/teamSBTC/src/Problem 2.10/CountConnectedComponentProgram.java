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
            // Extract the list of string token
            String[] tokens = value.toString().split(" ");

            // Set node ID by first token
            int node = Integer.parseInt(tokens[0]);

            // Create list storing neighbor of node
            int[] neighbors = new int[tokens.length - 1];

            // Set other tokens as neighbor id
            for (int i = 1; i < tokens.length; i++) {
                neighbors[i - 1] = Integer.parseInt(tokens[i]);
            }

            // Ascending Sort
            Arrays.sort(neighbors);
            
            // Set key-value for each two of neighbor node
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

            // Set for storing neighbor node
            HashSet<Integer> neighbors = new HashSet<Integer>();

            // For loop to go through input vals and add to set of neighbor
            for (IntWritable value : values) {
                neighbors.add(value.get());
            }

            // Set to store connected components
            HashSet<Integer> component = new HashSet<Integer>();
            component.add(node);

            boolean added;
            do {
                added = false;
                HashSet<Integer> newComponent = new HashSet<Integer>();
                // Go through node neighbors in current connected component
                for (int neighbor : neighbors) {
                    // Pass if neighbor already connected
                    if (component.contains(neighbor)) {
                        continue;
                    }
                    // If neighbor not in connected component, add it to set new node then add to component
                    added = true;
                    newComponent.add(neighbor);
                }
                // Add new nodes to connected component
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

