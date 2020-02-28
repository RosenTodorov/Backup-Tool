package org.com.sap.cloud.fss.backup;

public class User {
	private String username;
	private char[] password;

	public User() {
	}

	public char[] getPassword() {
		return password;
	}

	public void setPassword(char[] password) {
		validatePassword(password);
		this.password = password;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		validateUserName(username);
		this.username = username;
	}

	private void validateUserName(String userName) {
		if (userName == null || userName.isEmpty()) {
			throw new IllegalArgumentException("username parameter cannot be empty");
		}
	}
	
	private void validatePassword(char[] password) {
		if (password == null || password.length == 0) {
			throw new IllegalArgumentException("password parameter cannot be empty");
		}
	}
}
