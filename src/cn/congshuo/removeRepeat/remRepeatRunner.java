package cn.congshuo.removeRepeat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.sun.jersey.spi.MessageBodyWorkers;

public class remRepeatRunner extends Configured implements Tool{

	//实现run接口
	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf=new Configuration();
		Job job=Job.getInstance(conf);
		
		job.setJarByClass(remRepeatRunner.class);       //设定job使用的main函数所在的类
		job.setMapperClass(remRepeatMapper.class);      //设定Mapper所在的类
		job.setReducerClass(remRepeatReducer.class);    //设定Reduce所在的类
		
		//设置Map的输入和输出类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		//设置Reduce的输入和输出的类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		job.setMaxReduceAttempts(2);
		
		FileInputFormat.setInputPaths(job, new Path("hdfs://hadoop01:9000/wc/input"));      // 设置文件的输入位置
		FileOutputFormat.setOutputPath(job, new Path("hdfs://hadoop01:9000/wc/output"));     //设定文件的输出位置
		return job.waitForCompletion(true)?0:1;
		
	}

	
	public static void main(String[] args) throws Exception {
		int res=ToolRunner.run(new Configuration(), new remRepeatRunner(), args);
		System.exit(res);
	}
}
