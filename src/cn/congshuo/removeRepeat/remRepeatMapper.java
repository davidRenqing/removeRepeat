package cn.congshuo.removeRepeat;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class remRepeatMapper extends Mapper<LongWritable, Text, Text, NullWritable>{
	
	
	

	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {
		//得到文本的一行内容，将这行内容作为key输出
		//这里的NullWritable 要使用，nullwritable.get()方法
		context.write(value, NullWritable.get());
	}
}
