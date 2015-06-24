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
 * Questa classe si occupa di prendere in input il file degli attori e li
 * converte in un formato utilizzabile da hive
 * 
 * @author Big Meta
 *
 */
public class ConvertActorsFileScript {

	public static void main(String[] args) {

		if (args.length != 2) {
			System.err.println("USAGE: java -jar ConvertActorsFileScript.jar"
					+ " <nome del file di input> <nome del file di output>");
			System.exit(1);
		}

		String file = args[0];
		String output = args[1];

		String actor = null, films = null;
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
				if (line.isEmpty()) {
					/*
					 * Se entro qui posso chiudere l'attore corrente e scrivere
					 * la nuova riga sul file di output
					 */
					bw.write(actor + "\t" + films);
					bw.newLine();
					actor = null;
					films = null;
				} else {
					/* Aggiorno la filmografia dell'attore corrente */
					String[] lineSplitted = line.split("\t");
					if (!lineSplitted[0].isEmpty()) {
						actor = lineSplitted[0];
					}
					if (films == null) {
						films = lineSplitted[lineSplitted.length - 1];
					} else {
						films += "<ENDVALUE>" + lineSplitted[lineSplitted.length - 1];
					}
				}
			}

			/* Scrivo l'ultimo attore e la sua filmografia */
			bw.write(actor + "\t" + films);

			/* Chiudo gli stream */
			br.close();
			bw.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
