import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class InvertedIndexReducer extends Reducer<Text, IntWritable, Text, String>{

    private String prev;
    private StringBuilder result;
    private float frequency;
    private int bookNum;
    private int wordFrequency;
    private HTable table;
    private List<Put> puts;

    private void record(String word, float frequency){
        Put put = new Put(Bytes.toBytes(word));
        put.add(Bytes.toBytes("word"),Bytes.toBytes(""),Bytes.toBytes(word));
        put.add(Bytes.toBytes("frequency"),Bytes.toBytes(""),Bytes.toBytes(frequency));
        puts.add(put);
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        prev = null;
        result = new StringBuilder();
        frequency = 0;
        bookNum = 0;
        wordFrequency = 0;
        puts = new ArrayList<Put>();
        Configuration hconf = HBaseConfiguration.create();
        //hconf.set("hbase.zookeeper.quorum","zkServer");
        //hconf.set("hbase.zookeeper.property.clientPort","2128");
        table = new HTable(hconf,"wuxia");
    }

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        String current = key.toString().split("#")[0];
        String bookName = key.toString().split("#")[1];
        int sum = 0;

        if(!current.equals(prev)){

            if(prev != null){
                frequency = (float)wordFrequency/bookNum;

                record(prev,frequency);
                context.write(new Text(prev), String.format("%.2f,%s", frequency, result.toString()));

                bookNum = 0;
                wordFrequency = 0;
                result = new StringBuilder();
            }
            prev = current;
        }

        for(IntWritable value : values){
            sum += value.get();
        }
        result.append(String.format("%s:%d;",bookName,sum));
        bookNum ++;
        wordFrequency += sum;
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        frequency = (float)wordFrequency/bookNum;
        record(prev,frequency);
        table.put(puts);
        context.write( new Text(prev), String.format("%.2f,%s", frequency, result.toString()));
    }
}
