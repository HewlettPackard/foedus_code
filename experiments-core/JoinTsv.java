import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * General script to do something like unix's join command.
 * This program merges tsv files specified in command line
*/
public class JoinTsv {
	private static void printUsage() {
		System.err.println("Usage: javac JoinTsv.java; java JoinTsv <file1> <file2>");
	}
	private static class Pair {
		final double key;
		final double value;
		Pair (double key, double value) {
      this.key = key;
      this.value = value;
		}
  }
  private static class TsvContent {
    ArrayList<Pair> pairs = new ArrayList<Pair>();
    String name;
		TsvContent(File file) throws IOException {
      name = file.getName();
			try (BufferedReader reader = new BufferedReader(new FileReader(file), 1 << 16)) {
        System.out.println(file.getPath() + " header:" + reader.readLine());
				for (String line = reader.readLine(); line != null; line = reader.readLine()) {
          double key = Double.parseDouble(line.substring(0, line.indexOf('\t')));
          double value = Double.parseDouble(line.substring(line.indexOf('\t') + 1));
          pairs.add(new Pair(key, value));
				}
			}

			System.out.println(file.getPath() + " had " + pairs.size() + " lines");
		}
	}
	
	public static void main(String[] args) throws Exception {
		if (args.length < 1) {
			printUsage();
			return;
		}
		ArrayList<TsvContent> files = new ArrayList<TsvContent>();
		for (String arg : args) {
      files.add(new TsvContent(new File(arg)));
		}

		System.out.print("key");
    for (TsvContent file : files) {
      System.out.print("\t" + file.name.substring(file.name.lastIndexOf('_') + 1, file.name.lastIndexOf('.')));
    }
    System.out.println();
    for (int i = 0; i < files.size(); ++i) {
      TsvContent file = files.get(i);
      for (Pair pair : file.pairs) {
        System.out.print(pair.key);
        for (int j = 0; j < files.size(); ++j) {
          System.out.print("\t");
          if (i != j) {
            System.out.print("NULL");
            continue;
          }
          System.out.print(pair.value);
        }
        System.out.println();
      }
    }
	}
}
