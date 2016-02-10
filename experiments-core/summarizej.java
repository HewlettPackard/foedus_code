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
 * General summarization scripts for FOEDUS's experiments.
 * This program assumes the following folder structure:
 *  $data_root$ : a folder that contains result_xxxxx sub folders. Could be "ycsb"/"tpce" folder itself.
 *  $data_root$/result_xxxxx : a folder that contains one set of experimental results.
 *  $data_root$/result_xxxxx/options.txt : a file that contains run time parameters
 *  $data_root$/result_xxxxx/$benchmark$_$storagetype$.$workload$.$machine$.r$reps$.log : the stdout file
 * 
 * This program receives the full or relative path of $data_root$ as the only parameter.
 * The program writes out a tab-separated data to stdout,
 * which you will probably import to some spreadsheet software.
 * 
 * The outputs are in the following format:
 * ----
 * BENCHMARK_NAME WORKLOAD_NAME MACHINE RESULT_FOLDER STORAGE_TYPE PARAM1 PARAM2 ... COMPLETE_RUNS FAILED_RUNS MTPS_AVE MTPS_STDEV RACE_ABORTS_AVE RACE_ABORTS_STDEV 
*/
public class summarizej {
	private static void printUsage() {
		System.err.println("Usage: javac summarizej.java; java -classpath . summarizej $data_root$");
	}
	/** Corresponds to one run */
	private static class PerfResult {
		final boolean failed;
		final double mtps;
		final long total;
		final long userAborted;
		final long raceAborted;
		final long others;
		static String extract(String line, String tag) {
			final String beginTag = "<" + tag + ">";
			final int beginPos = line.indexOf(beginTag);
			if (beginPos < 0) {
				System.err.println("wtf??? tag=" + tag + ", line=" + line);
				return "0";
			}
			final String endTag = "</" + tag + ">";
			final int endPos = line.indexOf(endTag);
			if (endPos < 0) {
				System.err.println("wtf??? tag=" + tag + ", line=" + line);
				return "0";
			}
			return line.substring(beginPos + beginTag.length(), endPos);
		}
		PerfResult(File logFile) throws IOException {
			try (BufferedReader reader = new BufferedReader(new FileReader(logFile), 1 << 16)) {
				for (String line = reader.readLine(); line != null; line = reader.readLine()) {
					// eg
					// final result:<total_result><duration_sec_>10.0015</duration_sec_><worker_count_>224</worker_count_><processed_>3594511</processed_><MTPS>0.359397</MTPS><race_aborts_>0</race_aborts_><lock_aborts_>0</lock_aborts_><largereadset_aborts_>0</largereadset_aborts_><insert_conflict_aborts_>0</insert_conflict_aborts_><total_scan_length_>0</total_scan_length_><total_scans_>0</total_scans_><average_scan_length_>0</average_scan_length_><unexpected_aborts_>0</unexpected_aborts_><snapshot_cache_hits_>0</snapshot_cache_hits_><snapshot_cache_misses_>0</snapshot_cache_misses_></total_result>
					if (!line.contains("final result")) {
						continue;
					}
					failed = false;
					mtps = Double.valueOf(extract(line, "MTPS"));
					total = Long.valueOf(extract(line, "processed_"));
					userAborted = Long.valueOf(extract(line, "processed_"));
					raceAborted = Long.valueOf(extract(line, "race_aborts_"));
					others = 0;  // TODO
					return;
				}
			}

			System.out.println(logFile.getPath() + " doesn't contain a final result line. probably a failed run");
			failed = true;
			mtps = 0;
			total = 0;
			userAborted = 0;
			raceAborted = 0;
			others = 0;
		}
	}
	/** Corresponds to one line in the result */
	private static class StorageResult {
		final TreeMap<String, String> parameters = new TreeMap<String, String>();  // alphabetical order
		final ArrayList<PerfResult> perfResults = new ArrayList<PerfResult>();
		static Pattern parameterPattern = Pattern.compile("\\-([a-zA-Z0-9\\_]+)\\=([a-zA-Z0-9\\_]+)");
		static DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd:HH:mm:ss");

		StorageResult(String resultFolder, File optionsFile, String benchmark, String storageType, String workload, String machine) throws IOException {
			parameters.put("BENCHMARK_NAME", benchmark);
			parameters.put("WORKLOAD_NAME", workload);
			parameters.put("RESULT_FOLDER", resultFolder);
			parameters.put("FOLDER_TIMESTAMP", dateFormat.format(optionsFile.lastModified()));
			parameters.put("STORAGE_TYPE", storageType);
			parameters.put("MACHINE", machine);
			
			BufferedReader reader = new BufferedReader(new FileReader(optionsFile));
			String line = reader.readLine();
			reader.close();
			for (String split : line.split(" ")) {
				Matcher match = parameterPattern.matcher(split);
				if (!match.matches()) {
					System.out.println(split + " was not recognized as a parameter. wtf");
					continue;
				}
				String paramName = match.group(1);
				String paramValue = match.group(2);
				if (parameters.containsKey(paramName)) {
					System.out.println(split + " was a duplicate parameter. wtf");
					continue;
				}
				parameters.put(paramName, paramValue);
			}
		}
		String toLine(ArrayList<String> outParameters) {
			boolean firstParam = true;
			StringBuilder ret = new StringBuilder();
			for (String name : outParameters) {
				if (firstParam) {
					firstParam = false;
				} else {
					ret.append("\t");
				}
				if (parameters.containsKey(name)) {
					ret.append(parameters.get(name));
				} else {
					ret.append("NULL");
				}
			}
			int completedRuns = 0;
			int failedRuns = 0;
			double mtpsSum = 0;
			double mtpsSqSum = 0;
			long raceAbortSum = 0;
			long raceAbortSqSum = 0;
			for (PerfResult perf : perfResults) {
				if (perf.failed) {
					++failedRuns;
					continue;
				}
				++completedRuns;
				mtpsSum += perf.mtps;
				mtpsSqSum += perf.mtps * perf.mtps;
				raceAbortSum += perf.raceAborted;
				raceAbortSqSum += perf.raceAborted * perf.raceAborted;
			}
			if (completedRuns == 0) {
				ret.append("\t" + completedRuns + "\t" + failedRuns
						+ "\t0\t0"
						+ "\t0\t0");
			} else {
				ret.append("\t" + completedRuns + "\t" + failedRuns
					+ "\t" + (mtpsSum / completedRuns)
					+ "\t" + Math.sqrt((mtpsSqSum / completedRuns) - (double) mtpsSum * mtpsSum / (completedRuns * completedRuns))
					+ "\t" + (raceAbortSum / completedRuns)
					+ "\t" + Math.sqrt((raceAbortSqSum / completedRuns) - (double) raceAbortSum * raceAbortSum / (completedRuns * completedRuns)));
			}
			return ret.toString();
		}
	}
	private static class Result {
		final TreeMap<String, StorageResult> storageResults = new TreeMap<String, StorageResult>();
		static Pattern logNamePattern = Pattern.compile("([a-zA-Z0-9\\_]+)\\_([a-zA-Z0-9\\_]+)\\.([a-zA-Z0-9\\_]+)\\.([a-zA-Z0-9\\_]+)\\.r([0-9]+)\\.log");  

		Result(File folder, File optionsFile) throws IOException {
			File[] files = folder.listFiles();
			Arrays.sort(files);
			String the_benchmark = null; // we don't expect more than one benchmark in one folder
			String the_workload = null; // we don't expect more than one workload in one folder
			for (File file : files) {
				if (file.isDirectory()) {
					System.out.println(file.getPath() + " was ignored as it's not a file");
				} else if (file.getName().equals("options.txt")) {
					continue;
				} else {
					Matcher match = logNamePattern.matcher(file.getName());
					if (match.matches()) {
						final String benchmark = match.group(1);
						final String storageType = match.group(2);
						final String workload = match.group(3);
						final String machine = match.group(4);
						if (the_benchmark == null) {
							the_benchmark = benchmark;
						} else if (!the_benchmark.equals(benchmark)) {
							System.out.println(file.getPath() + " ran a different benchmark. ignored");
						}
						if (the_workload == null) {
							the_workload = workload;
						} else if (!the_workload.equals(workload)) {
							System.out.println(file.getPath() + " ran a different workload. ignored");
						}
						
						StorageResult set = storageResults.get(storageType);
						if (set == null) {
							set = new StorageResult(folder.getName(), optionsFile, benchmark, storageType, workload, machine);
							storageResults.put(storageType, set);
						}
						set.perfResults.add(new PerfResult(file));
					} else {
						System.out.println(file.getPath() + " is not a log file or the file name pattern doesn't follow our convention. ignored");
					}
				}
			}
		}
		void appendLines(StringBuilder buffer, ArrayList<String> outParameters) {
			for (StorageResult r : storageResults.values()) {
				buffer.append(r.toLine(outParameters));
				buffer.append("\n");
			}
		}
	}
	private static class AllResults {
		final ArrayList<String> parameterNames = new ArrayList<String>();
		final ArrayList<Result> results = new ArrayList<Result>();

		AllResults(File root) throws IOException {
			parameterNames.add("BENCHMARK_NAME");
			parameterNames.add("WORKLOAD_NAME");
			parameterNames.add("MACHINE");
			parameterNames.add("RESULT_FOLDER");
			parameterNames.add("FOLDER_TIMESTAMP");
			parameterNames.add("STORAGE_TYPE");
			
			File[] folders = root.listFiles();
			Arrays.sort(folders);
			for (File folder : folders) {
				if (folder.isDirectory()) {
					File[] optionFiles = folder.listFiles(new FilenameFilter() {
						@Override
						public boolean accept(File dir, String name) {
							return name.equals("options.txt");
						}
					});
					if (optionFiles.length != 1) {
						System.out.println(folder.getPath() + " doesn't contain options.txt. Ignored");
						continue;
					}
					Result result = new Result(folder, optionFiles[0]);
					results.add(result);
					// The above basic parameters always at the beginning, others in alphabetical order.
					for (StorageResult r : result.storageResults.values()) {
						for (String paramName : r.parameters.keySet()) {
							if (!parameterNames.contains(paramName)) {
								System.out.println("Observed a new parameter:" + paramName);
								parameterNames.add(paramName);
							}
						}
					}
				} else {
					System.out.println(folder.getPath() + " was ignored as it's not a folder");
				}
			}
		}
		public String toString() {
			StringBuilder buffer = new StringBuilder();
			for (String name : parameterNames) {
				if (buffer.length() > 0) {
					buffer.append("\t");
				}
				buffer.append(name);
			}
			buffer.append("\tCOMPLETED_RUNS\tFAILED_RUNS\tMTPS_AVE\tMTPS_STDEV\tRACE_ABORTS_AVE\tRACE_ABORTS_STDEV");
			buffer.append("\n");
			for (Result r : results) {
				r.appendLines(buffer, parameterNames);
			}
			return buffer.toString();
		}
	}
	
	public static void main(String[] args) throws Exception {
		if (args.length < 1) {
			printUsage();
			return;
		}
		String rootPath = args[0];
		File rootFolder = new File(rootPath);
		if (!rootFolder.exists() || !rootFolder.isDirectory()) {
			System.err.println("'" + rootPath + "' doesn't exist as a folder");
			printUsage();
			return;
		}

		AllResults allResults = new AllResults(rootFolder);
		System.out.println(allResults);
		
	}

}
