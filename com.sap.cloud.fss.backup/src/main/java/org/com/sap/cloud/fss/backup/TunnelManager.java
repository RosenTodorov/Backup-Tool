package org.com.sap.cloud.fss.backup;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class TunnelManager {
	private static final String NEO_BAT_OPEN_DB_TUNNEL = "neo.bat open-db-tunnel -a fsservice -h ";
	private static final String NEO_SH_OPEN_DB_TUNNEL = "neo.sh open-db-tunnel -a fsservice -h ";
	private static final String NEO_BAT_CLOSE_DB_TUNNEL = "neo.bat close-db-tunnel --session-id ";
	private static final String NEO_SH_CLOSE_DB_TUNNEL = "neo.sh close-db-tunnel --session-id ";
	private static final String I_FSSERVICE_P = " -i fsservice -p ";
	private static final String BACKGROUND_OUTPUT = " --background --output json";
	
	public BufferedReader openDbTunnel(String host, User user) throws IOException {
		String neoCommand = selectOpenDBCommand();
		StringBuilder openTunnelParameters = createOpenTunnelData(neoCommand, host, user);
		Process process = Runtime.getRuntime().exec(openTunnelParameters.toString());
		BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
		return reader;
	}
	
	private StringBuilder createOpenTunnelData(String neoOpenDBCommand, String host, User user) {
		StringBuilder openTunnelParameters = new StringBuilder();              
		openTunnelParameters.append(neoOpenDBCommand);
		openTunnelParameters.append(host);
		openTunnelParameters.append(" -u ");
		openTunnelParameters.append(user.getUsername());
		openTunnelParameters.append(I_FSSERVICE_P);
		openTunnelParameters.append(user.getPassword());
		openTunnelParameters.append(BACKGROUND_OUTPUT);
		return openTunnelParameters;
	}
	
	private String selectOpenDBCommand() {
		String neoOpenDBCommand = "";
		String operatingSystem = System.getProperty("os.name").toLowerCase();
		if (operatingSystem.indexOf("win") >= 0) {
			neoOpenDBCommand = NEO_BAT_OPEN_DB_TUNNEL;
		} else {
			neoOpenDBCommand = NEO_SH_OPEN_DB_TUNNEL;
		}
        return neoOpenDBCommand;
	}
	
	private String selectCloseDBCommand() {
		String neoCloseDBCommand = "";
		String operatingSystem = System.getProperty("os.name").toLowerCase();
		if (operatingSystem.indexOf("win") >= 0) {
			neoCloseDBCommand = NEO_BAT_CLOSE_DB_TUNNEL;
		} else {
			neoCloseDBCommand = NEO_SH_CLOSE_DB_TUNNEL;
		}
        return neoCloseDBCommand;
	}
           
	public JSONObject parseJson(BufferedReader reader) throws IOException, ParseException {
		StringBuilder content = new StringBuilder();
		String result = null;
		String line = null;
		while ((line = reader.readLine()) != null) {
			content.append(line);
		}
		reader.close();
		result = content.toString();
		JSONParser parser = new JSONParser();	
		Object obj = parser.parse(result);
		JSONObject jsonObject = (JSONObject) obj;
		return jsonObject;
	}

	public void closeDbTunnel(String sessionID) throws IOException, InterruptedException {
		String neoCommand =  selectCloseDBCommand();
		Process processClose = Runtime.getRuntime().exec(neoCommand + sessionID);
		processClose.waitFor();
	}
}
