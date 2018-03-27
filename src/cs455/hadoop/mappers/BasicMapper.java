package cs455.hadoop.mappers;

import com.sun.istack.logging.Logger;
import java.util.Arrays;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Mapper: Reads line by line, split them into words. Emit <word, 1> pairs.
 */
public class BasicMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

  private Logger logger = Logger.getLogger(BasicMapper.class);

  @Override
  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String[] items = value.toString().split(",");
    try {
      // tokenize into words.

      //if(items.length ==29)
      //{
     int delay = Integer.getInteger(items[15]) + Integer.getInteger(items[14]);
      //int day = Integer.getInteger(items[3]);
      // emit word, count pairs.
      context.write(new Text(items[3]), new IntWritable(delay));
    } catch (NullPointerException e) {
      System.err.println("Unable to read:\n" + value.toString() +"\n"+ Arrays.toString(items));
      logger.severe("Unable to read:\n" + value.toString() +"\n"+ Arrays.toString(items));
    }
  }
}
