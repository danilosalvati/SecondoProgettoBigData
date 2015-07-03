package it.uniroma3.bigDataProject;

import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.Text;

public class ArrayContainsSubstringUDF extends GenericUDF {
	private ListObjectInspector listOI;
	private StringObjectInspector elementOI;

	@Override
	public String getDisplayString(String[] arg0) {
		return "";
	}

	/**
	 * Questo metodo si occupa di controllare che gli argomenti passati alla
	 * funzione siano corretti
	 */
	@Override
	public ObjectInspector initialize(ObjectInspector[] arguments)
			throws UDFArgumentException {

		if (arguments.length != 2) {
			throw new UDFArgumentLengthException(
					"ArrayContainsSubstringUDF only takes 2 arguments: String, Array<String>");
		}
		// Controllo che gli argomenti passati siano corretti
		ObjectInspector title = arguments[0];
		ObjectInspector filmArray = arguments[1];
		if (!(title instanceof StringObjectInspector)
				|| !(filmArray instanceof ListObjectInspector)) {
			throw new UDFArgumentException(
					"first argument must be a string, second argument must be an array of strings");
		}
		this.listOI = (ListObjectInspector) filmArray;
		this.elementOI = (StringObjectInspector) title;

		// Controllo che l'array contenga effettivamente delle stringhe
		if (!(listOI.getListElementObjectInspector() instanceof StringObjectInspector)) {
			throw new UDFArgumentException(
					"first argument must be a list of strings");
		}

		// Dato che restituiamo un booleano diamo l'objectInspector relativo
		// return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		return PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;

	}

	@Override
	public Object evaluate(DeferredObject[] arguments) throws HiveException {

		// Prendo i due parametri
		String title = elementOI.getPrimitiveJavaObject(arguments[0].get());
		List<Text> list = (List<Text>) this.listOI.getList(arguments[1].get());

		// Controllo che non ci siano valori nulli
		if (list == null || title == null) {
			return null;
		}

		// A questo punto controllo che l'array contenga una sottostringa del
		// titolo del film
		for (Text film : list) {
			if (film.toString().startsWith(title)) {
				// return new Text(film);
				return new Boolean(true);
			}
		}
		// return new Text("");
		return new Boolean(false);
	}
}
