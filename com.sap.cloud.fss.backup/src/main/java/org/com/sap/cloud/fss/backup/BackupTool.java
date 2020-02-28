package org.com.sap.cloud.fss.backup;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.Console;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.json.simple.JSONObject;

public class BackupTool {
	private static Date date = new Date();
	private static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH-mm-ss");
	private static String currentDate = dateFormat.format(date);

	private static final String VOLUME_ID_FILENAME = "." + File.separator + "volumes.hana.ondemand-volumeId-backup-" + currentDate + ".csv";
	private static final String ID_FILENAME = "." + File.separator + "volumes.hana.ondemand-lastID.csv";
	private static final String COPY_ID_FILENAME = "." + File.separator + "volumes.hana.ondemand-lastID-copy" + ".csv";
	
	private static final String NEW_LINE = "\n";
	
	public static void main(String[] args) throws Exception {
		List<String> allVolumeIDs = readInputIDFile();
		Map<String,List<String>> inputDBParameters = readInputDBParameters();
		List<String> allFactoryNames = inputDBParameters.get("factoryNames");
		List<String> allHosts = inputDBParameters.get("hosts");
		List<String> allUserNamesDB = inputDBParameters.get("userNamesDB");
		List<String> allPasswordsDB = inputDBParameters.get("passwordsDB");
		
		User user = userLogin();
	    TunnelManager tunnelManager = new TunnelManager();
		validatePassword(allHosts.get(0), user, tunnelManager);
		renameVolumeIDsFile();

		List<String> volumeIdsData = new ArrayList<String>();
		List<String> resultVolumeIDs = Collections.synchronizedList(volumeIdsData);
		List<String> lastIdsData = new ArrayList<String>();
		List<String> resultLastIds = Collections.synchronizedList(lastIdsData);
		
		ExecutorService threadPool = null;
		try {
			threadPool = Executors.newFixedThreadPool(allHosts.size());
			for (int i = 0; i < allHosts.size(); i++) {
				Runnable worker = new BackupRunnable(tunnelManager, user, allFactoryNames.get(i), allHosts.get(i), allUserNamesDB.get(i), allPasswordsDB.get(i), allVolumeIDs.get(i), resultVolumeIDs, resultLastIds);
				threadPool.execute(worker);
			}
		} finally {
			if (threadPool != null) {
				threadPool.shutdown();
			}
		}
		if (threadPool != null) {
			threadPool.awaitTermination(15, TimeUnit.MINUTES);
			if (threadPool.isTerminated()) {
				writeResultToFile(resultVolumeIDs, resultLastIds);
				deleteFilesVolumeIDAndCopyFileIDs();
			}
		}
	}

	private static User userLogin() {
		Console console = System.console();
		String userName = console.readLine("Enter your userName: ");
		char[] passwordArray = console.readPassword("Enter your password: ");
		User user = new User();
		user.setUsername(userName);
		user.setPassword(passwordArray);
		return user;
	}

	private static List<String> readInputIDFile() throws IOException {
		String lastIDData = "";
		List<String> allVolumeIDs = new ArrayList<String>();
		File fileIDs = new File(ID_FILENAME);
		try (BufferedReader readerLastIDFile = new BufferedReader(new FileReader(fileIDs))) {
			while ((lastIDData = readerLastIDFile.readLine()) != null) {
				String[] lastDataLine = lastIDData.split(",");
				allVolumeIDs.add(lastDataLine[1]);
			}
		}
		return allVolumeIDs;
	}

	private static Map<String, List<String>> readInputDBParameters() throws IOException {
		String dataInputFile;
		List<String> factoryNames = new ArrayList<String>();
		List<String> hosts = new ArrayList<String>();
		List<String> userNameDB = new ArrayList<String>();
		List<String> passwordDB = new ArrayList<String>();
		File inputFile = new File("." + File.separator + "InputFile.csv");
		try (BufferedReader readerInputFile = new BufferedReader(new FileReader(inputFile))) {
			while ((dataInputFile = readerInputFile.readLine()) != null) {
				String[] arrayInputFile = dataInputFile.split(",");
				factoryNames.add(arrayInputFile[0]);
				hosts.add(arrayInputFile[1]);
				userNameDB.add(arrayInputFile[2]);
				passwordDB.add(arrayInputFile[3]);
			}
		}
		Map<String, List<String>> inputDBParameters = new HashMap<String, List<String>>();
		inputDBParameters.put("factoryNames", factoryNames);
		inputDBParameters.put("hosts", hosts);
		inputDBParameters.put("userNamesDB", userNameDB);
		inputDBParameters.put("passwordsDB", passwordDB);
		return inputDBParameters;
	}
	
	private static void validatePassword(String host, User user, TunnelManager tunnelManager) throws Exception {
		try (BufferedReader reader = tunnelManager.openDbTunnel(host, user)) {
			JSONObject jsonObject = tunnelManager.parseJson(reader);
			String errorMsg = (String) jsonObject.get("errorMsg");

			long exitCode = ((Long) jsonObject.get("exitCode")).intValue();
			if (exitCode > 0) {
				throw new Exception(errorMsg);
			}
			System.out.println("Your password is correct!");
			JSONObject ID = (JSONObject) jsonObject.get("result");
			String sessionID = (String) ID.get("sessionId");
			tunnelManager.closeDbTunnel(sessionID);
		}
	}

	private static void renameVolumeIDsFile() throws IOException {
		File fileIDs = new File(ID_FILENAME);
		File copyFileIDs = new File(COPY_ID_FILENAME);
		if (fileIDs.renameTo(copyFileIDs)) {
			System.out.println("Rename of the file with ID is succesful");
		} else {
			throw new IOException("Rename of the file with ID failed");
		}
	}

	private static void writeResultToFile(List<String> resultVolumeIDs, List<String> resultLastIds) throws InterruptedException {
		Collections.sort(resultLastIds);
		try (BufferedWriter bwVolumeID = new BufferedWriter(new FileWriter(VOLUME_ID_FILENAME));
				BufferedWriter lastID = new BufferedWriter(new FileWriter(ID_FILENAME))) {		
			for (String volumeID : resultVolumeIDs) {
				bwVolumeID.write(volumeID + NEW_LINE);
			}

			for (String lastId : resultLastIds) {
				lastID.write(lastId + NEW_LINE);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private static void deleteFilesVolumeIDAndCopyFileIDs() {
		File fileVolumeID = new File(VOLUME_ID_FILENAME);
		if (fileVolumeID.length() == 0) {
			if (fileVolumeID.delete()) {
				System.out.println("There isn't new volumes ID in the file!");
			} else {
				System.out.println("File volumeId-backup isn't deleted!!! There isn't new volumes ID in the file!");
			}
		} else {
			System.out.println("You have new volumes ID!");
		}

		File fileIDs = new File(ID_FILENAME);
		File copyFileIDs = new File(COPY_ID_FILENAME);
		if (fileIDs.length() == 0) {
			if (fileIDs.delete()) {
				copyFileIDs.renameTo(fileIDs);
				System.out.println("Empty file lastID was successfully deleted! The copy of the file lastID was renamed!");
			} else {
				System.out.println("Empty file lastID isn't deleted!");
			}
		} else {
			if (copyFileIDs.delete()) {
				System.out.println("The copy of the file lastID was successfully deleted!");
			} else {
				System.out.println("The copy of the file lastID isn't deleted!");
			}
		}
	}
}


