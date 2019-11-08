package hadoop.ch03.xf17124080235;


import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;
public class Dpart extends Partitioner<NullWritable, DDA>{

    @Override
    public int getPartition(NullWritable k2, DDA v2, int DA) {
        if(v2.getURL() == 3 && v2.getDian() == 2) {
            return 1%DA;
        }else {
            return 2%DA;
        }
    }
}
