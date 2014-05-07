import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PageRankBasic {
	
	/**
	 * NetID used: yg324
	 * fromNetID = 0.423
	 * rejectMin = 0.99 * 0.423 = .41877
	 * rejectLimit = .42877
	 *
	 */



	public static final int totalNodes = 685230;

	private static final int NUM_ITERATIONS = 6;

	// multiplier for residual error, so we can store error as long in counter
	public static final int precision = 10000;
	
	public static void main(String[] args) throws Exception {
		
		String inputFile = args[0];
		String outputPath = args[1];

		for (int i = 0; i < NUM_ITERATIONS; i++) {
			Job job = Job.getInstance();

			job.setJobName("pagerank_" + (i + 1));
			job.setJarByClass(PageRankBasic.class);

			// Set Mapper and Reducer class
			job.setMapperClass(PageRankBasicMapper.class);
			job.setReducerClass(PageRankBasicReducer.class);

			// set the classes for output key and value
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			// on the initial pass, use the preprocessed input file
			// note that we use the default input format which is
			// TextInputFormat (each record is a line of input)
			if (i == 0) {
				FileInputFormat.addInputPath(job, new Path(inputFile));
				// otherwise use the output of the last pass as our input
			} else {
				FileInputFormat.addInputPath(job, new Path(outputPath + "/temp"
						+ i));
			}
			// set the output file path
			FileOutputFormat.setOutputPath(job, new Path(outputPath + "/temp"
					+ (i + 1)));

			// execute the job and wait for completion before starting the next
			// pass
			job.waitForCompletion(true);

			// before starting the next pass, compute the avg residual error for
			// this pass and print it out
			float residualErrorAvg = job.getCounters().findCounter("page_rank", "residual_error").getValue();
			residualErrorAvg = (residualErrorAvg / precision) / totalNodes;
			String residualErrorString = String
					.format("%.4f", residualErrorAvg);
			System.out.println("Residual error for iteration " + i + ": "
					+ residualErrorString);

			// reset the counter for the next round
			job.getCounters().findCounter("page_rank", "residual_error")
					.setValue(0L);
		}

	}

}