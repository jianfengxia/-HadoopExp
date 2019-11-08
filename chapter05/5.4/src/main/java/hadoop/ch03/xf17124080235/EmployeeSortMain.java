package hadoop.ch03.xf17124080235;
//排序代码主函数
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class EmployeeSortMain {
    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance();
        job.setJarByClass(EmployeeSortMain.class);
        job.setMapperClass(DMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(DDA.class);
        job.setPartitionerClass(Dpart.class);
        job.setNumReduceTasks(2);
        job.setReducerClass(DReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);

    }
}
