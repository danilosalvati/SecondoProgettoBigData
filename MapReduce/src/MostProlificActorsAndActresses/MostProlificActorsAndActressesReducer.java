package MostProlificActorsAndActresses;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import MostProlificActorsAndActresses.Pair;

public class MostProlificActorsAndActressesReducer extends
		Reducer<Text, IntWritable, Text, IntWritable> {

	private static final int TOP_K = 10;
	private PriorityQueue<Pair> queue;

	@Override
	protected void setup(Context ctx) {
		queue = new PriorityQueue<Pair>(TOP_K, new Comparator<Pair>() {
			public int compare(Pair p1, Pair p2) {
				return p1.moviesNumber.compareTo(p2.moviesNumber);
			}
		});
	}

	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {

		/* Incremento il numero di film */
		int moviesNumber = 0;
		for (IntWritable value : values) {
			moviesNumber = moviesNumber + value.get();
		}

		/* Aggiungo la coppia alla coda ed elimino gli elementi eccedenti */
		queue.add(new Pair(key.toString(), moviesNumber));

		if (queue.size() > TOP_K) {
			queue.remove();
		}
	}

	/**
	 * Una volta terminato il task per questo reducer posso scrivere il
	 * risultato svuotando la coda
	 * 
	 */
	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {

		List<Pair> mostProlificActorsAndActresses = new ArrayList<Pair>();

		while (!queue.isEmpty()) {
			mostProlificActorsAndActresses.add(queue.remove());
		}

		/* Riestraggo gli elementi al contrario per avere il giusto ordinamento */
		for (int i = mostProlificActorsAndActresses.size() - 1; i >= 0; i--) {
			Pair topKPair = mostProlificActorsAndActresses.get(i);
			context.write(new Text(topKPair.actor), new IntWritable(
					topKPair.moviesNumber));
		}
	}
}