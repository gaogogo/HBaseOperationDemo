import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class InvertedIndex {
    public static void main(String[] args){
        try {
            Configuration conf = new Configuration();
            Job job = new Job(conf, "Invert Index");
            job.setJarByClass(InvertedIndex.class);
            job.setMapperClass(InvertedIndexMapper.class);
            job.setCombinerClass(InvertedIndexCombiner.class);
            job.setPartitionerClass(InvertedIndexPartitioner.class);
            job.setReducerClass(InvertedIndexReducer.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(String.class);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            System.exit(job.waitForCompletion(true)? 0 : 1);
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
