import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

public class ScanHBase {
    public static void main(String []args) throws IOException{
        Configuration conf = HBaseConfiguration.create();
        HTable table = new HTable(conf, "wuxia");
        String word = null;
        String frequency;
        Float f = null;

        File file = new File("./wuxia.txt");
        if(!file.exists()){
            file.createNewFile();
        }
        FileOutputStream out = new FileOutputStream(file);

        Scan scan = new Scan();
        ResultScanner results = table.getScanner(scan);
        for(Result result : results){
            for(Cell cell : result.rawCells()){
                word = new String(result.getRow());
                frequency = new String(CellUtil.cloneFamily(cell));
                if("frequency".equals(frequency)){
                    f = Bytes.toFloat(CellUtil.cloneValue(cell));
                }
            }
            out.write((word + "\t" +f.toString()+ "\n").getBytes());
        }
    }
}
