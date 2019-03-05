import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/* 
 * The class definition requires four parameters: 
 *   The data type of the input key (which is the output key type 
 *   from the mapper)
 *   The data type of the input value (which is the output value 
 *   type from the mapper)
 *   The data type of the output key
 *   The data type of the output value
 */   
public class WordReducer extends Reducer<Text, IntWritable, Text, CompositeWritable> {

	/*
	 * The reduce method runs once for each key received from
	 * the shuffle and sort phase of the MapReduce framework.
	 * The method receives a key of type Text, a set of values of type
	 * IntWritable, and a Context object.
	 */

	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int wordCount = 0;

		/*
		 * For each value in the set of values passed to us by the mapper:
		 */
		
		int x = 0; 
		int year = 0;
		int percent = 0;
		
		for (IntWritable value : values) {

			if (x == 0) {
				percent = value.get();
			}
			else {
				year = value.get();
			}
			++x;
		}
		context.write(key, new CompositeWritable(percent, year));

	}
}