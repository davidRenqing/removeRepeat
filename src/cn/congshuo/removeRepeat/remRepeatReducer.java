package cn.congshuo.removeRepeat;


import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class remRepeatReducer extends Reducer<Text, NullWritable, Text, NullWritable>{

	@Override
	protected void reduce(Text key1, Iterable<NullWritable> value1,
			Context context) throws IOException, InterruptedException {
		
		//reduce阶段做的就是利用write函数将程序进行输出。这个程序的含义就是利用shuffle这个机制，将重复的日志进行输出
		context.write(key1,NullWritable.get());
	}
}
