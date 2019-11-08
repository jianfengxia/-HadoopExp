package hadoop.ch03.xf17124080235;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
// k3    v3   k4   v4
public class WorldCountreduce extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text k3, Iterable<IntWritable> v3,Context context) throws IOException, InterruptedException {
        /*
        *context 是 reduce 的上下文
        * 上文
        * 下文
         */
        //对v3求和
        int total = 0;
        for(IntWritable v:v3){
            total += v.get();
        }
        //输出:        k4 单词     v4频率
        context.write(k3, new IntWritable(total));
    }
}
