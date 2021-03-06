package cs455.hadoop.airline;

import cs455.hadoop.mappers.BasicMapper;

import cs455.hadoop.reducers.BasicReducer;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AnalysisJob {
  public static void main(String[] args) {
    try {
      Configuration conf = new Configuration();
      // Give the MapRed job a name. You'll see this name in the Yarn webapp.
      Job job = Job.getInstance(conf, "word count");
      // Current class.
      job.setJarByClass(AnalysisJob.class);
      // Mapper
      job.setMapperClass(BasicMapper.class);

      // Combiner. We use the reducer as the combiner in this case.
      job.setCombinerClass(BasicReducer.class);
      // Reducer
      job.setReducerClass(BasicReducer.class);
      // Outputs from the Mapper.
      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(IntWritable.class);
      // Outputs from Reducer. It is sufficient to set only the following two properties
      // if the Mapper and Reducer has same key and value types. It is set separately for
      // elaboration.
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(IntWritable.class);
      // path to input in HDFS
      FileInputFormat.addInputPath(job, new Path(args[0]));
      // path to output in HDFS

      Path output = new Path(args[1]);
      FileSystem hdfs = FileSystem.get(conf);

// delete existing directory
      if (hdfs.exists(output)) {
        hdfs.delete(output, true);
      }
      FileOutputFormat.setOutputPath(job, output);
      // Block until the job is completed.
      System.exit(job.waitForCompletion(true) ? 0 : 1);
    } catch (IOException e) {
      System.err.println(e.getMessage());
    } catch (InterruptedException e) {
      System.err.println(e.getMessage());
    } catch (ClassNotFoundException e) {
      System.err.println(e.getMessage());
    }

  }

}
