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
 * Questa classe si occupa di prendere in input il file delle citazioni
 * convertendole in un formato utilizzabile da hive
 * 
 * @author Big Meta
 *
 */
public class ConvertQuotesFileScript {

	public static void main(String[] args) {

		if (args.length != 2) {
			System.err.println("USAGE: java -jar ConvertQuotesFileScript.jar"
					+ " <nome del file di input> <nome del file di output>");
			System.exit(1);
		}

		String file = args[0];
		String output = args[1];

		String film = null, quotes = null;
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

				if (line.startsWith("# ")) {
					if (film != null) {
						/*
						 * Scrivo il nuovo film su file togliendo l'ultimo
						 * separatore perché di troppo
						 */
						bw.write(film + "\t"
								+ quotes.substring(0, quotes.length() - 10));

						bw.newLine();
					}
					/* Posso cominciare a scrivere i quotes per il nuovo film */
					film = line.substring(2);
					quotes = "";

				} else if (line.isEmpty()) {
					/*
					 * Se entro qui allora ho finito una citazione
					 */

					quotes += "<ENDVALUE>"; // Questo è il separatore delle citazioni
				} else {
					/* Aggiorno la citazione corrente */
					quotes += line + "//"; // Questo è il separatore di ciascuna
											// riga delle citazioni
				}
			}

			/* Scrivo l'ultimo attore e la sua filmografia */
			bw.write(film + "\t" + quotes);

			/* Chiudo gli stream */
			br.close();
			bw.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
