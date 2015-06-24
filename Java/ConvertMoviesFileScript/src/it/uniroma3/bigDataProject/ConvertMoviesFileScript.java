package it.uniroma3.bigDataProject;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;

/**
 * Questa classe si occupa di prendere in input il file dei film per convertirli
 * in un formato utilizzabile da hive
 * 
 * @author Big Meta
 *
 */
public class ConvertMoviesFileScript {

	public static void main(String[] args) {

		if (args.length != 2) {
			System.err.println("USAGE: java -jar ConvertMoviesFileScript.jar"
					+ " <nome del file di input> <nome del file di output>");
			System.exit(1);
		}

		String file = args[0];
		String output = args[1];

		String movie = null, year = null;
		try {
			/* Apro lo stream in ingresso */
			InputStreamReader inputStreamReader = new InputStreamReader(
					new FileInputStream(new File(file)),
					Charset.forName("ISO-8859-15"));
			BufferedReader br = new BufferedReader(inputStreamReader);

			/* Apro lo stream in uscita */
			File fout = new File(output);
			FileOutputStream fos = new FileOutputStream(fout);
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));

			for (String line; (line = br.readLine()) != null;) {
				String[] parts = line.split("\t");
				movie = parts[0];
				for (int i = 1; i < parts.length; i++) {
					if (!parts[i].isEmpty()) {
						year = parts[i];
					}
				}
				bw.write(movie + "\t" + year);
				bw.newLine();
			}

			/* Chiudo gli stream */
			br.close();
			bw.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
