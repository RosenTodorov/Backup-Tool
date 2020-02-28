package org.com.sap.cloud.fss.backup;

import java.io.BufferedReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import org.json.simple.JSONObject;

public class BackupRunnable implements Runnable {
	private static final String DB_DRIVER = "com.sybase.jdbc4.jdbc.SybDriver";
	private static final String TUNNEL_NOT_OPEN_ERROR_MESSAGE = "ERROR: The DB tunnel can't open/No response from server";
	private static final String COMMMA = ",";
	
	private static Date date = new Date();
	private static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH-mm-ss");
	private static String currentDate = dateFormat.format(date);
	
	private final String factoryName;
	private final String host;
	private final String userNameDB;
	private final String passwordDB;
	private final String volumeID;
	private final List<String> resultVolumeIDs;
	private final List<String> resultLastIds;
	private TunnelManager tunnelManager;
	private User user = new User();

	public BackupRunnable(TunnelManager tunnelManager, User user, String factoryName, String host, String userNameDB, String passwordDB, String volumeID, List<String> resultVolumeIDs, List<String> resultLastIds) {
		this.tunnelManager = tunnelManager;
		this.user = user;
		this.factoryName = factoryName;
		this.host = host;
		this.userNameDB = userNameDB;
		this.passwordDB = passwordDB;
		this.volumeID = volumeID;
		this.resultVolumeIDs = resultVolumeIDs;
		this.resultLastIds = resultLastIds;
	}

	@Override
	public void run() {
		try {
			try (BufferedReader reader = this.tunnelManager.openDbTunnel(host, user)) {
				JSONObject jsonObject = this.tunnelManager.parseJson(reader);
				String errorMsg = (String) jsonObject.get("errorMsg");	
				int biggestID = Integer.parseInt(volumeID);

				JSONObject ID = (JSONObject) jsonObject.get("result");
				if (ID != null) {
					String jdbcUrl = (String) ID.get("jdbcUrl");
					try (Connection connection = getConnection(jdbcUrl, userNameDB, passwordDB);
							PreparedStatement query = createPreparedStatement(connection, biggestID)) {
						writeVolumeIDs(query, biggestID, factoryName, resultVolumeIDs, resultLastIds);
					}
					String sessionID = (String) ID.get("sessionId");
					this.tunnelManager.closeDbTunnel(sessionID);
				} else {
					errorResult(errorMsg, biggestID, factoryName, resultVolumeIDs, resultLastIds);
				}
			}
			System.out.println(factoryName + " is Checked!");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private Connection getConnection(String jdbcUrl, String userNameDB, String passwordDB) throws SQLException {
		try {
			Class.forName(DB_DRIVER);
		} catch (ClassNotFoundException e) {
			throw new SQLException(e);
		}
		return DriverManager.getConnection(jdbcUrl, userNameDB, passwordDB);
	}
	
	private PreparedStatement createPreparedStatement(Connection connection, int biggestID) throws SQLException {
		PreparedStatement query = connection.prepareStatement("select * from \"VOLUME\" where VOLUME.ID > ?");
		query.setInt(1, biggestID);
		return query;
	}

	private void writeVolumeIDs(PreparedStatement query, int biggestID, String factoryName, List<String> resultVolumeIDs, List<String> resultLastIds) throws SQLException {
		try (ResultSet rsVolumeID = query.executeQuery()) {
			int id = 0;
			while (rsVolumeID.next()) {
				String volumeID = rsVolumeID.getString(5);
				id = rsVolumeID.getInt(1);
				resultVolumeIDs.add(factoryName + COMMMA + volumeID + COMMMA);
			}

			if (id > 0) {
				resultLastIds.add(factoryName + COMMMA + id + COMMMA + currentDate);
			} else {
				resultLastIds.add(factoryName + COMMMA + biggestID + COMMMA + currentDate);
			}
		}
	}

	private void errorResult(String errorMsg, int biggestID, String factoryName, List<String> resultVolumeIDs, List<String> resultLastIds) throws IOException {
		System.out.println(factoryName + " - " + errorMsg);
		if (errorMsg != null) {
			resultVolumeIDs.add(factoryName + COMMMA + errorMsg);
		} else {
			resultVolumeIDs.add(factoryName + COMMMA + TUNNEL_NOT_OPEN_ERROR_MESSAGE);
		}
		resultLastIds.add(factoryName + COMMMA + biggestID + COMMMA + currentDate);
	}
}
