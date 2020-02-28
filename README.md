# Backup-Tool

Description: Temporary storage backup application for internal use. The tool used a thread pool which parallel opens the DB tunnels of all factories in the different cities. The tool makes connections to their databases and writes the selected data (id and name) of newly created volumes for backup. The tool then closed the connections and the DB tunnels. Jenkins runs the tool every week to check for new volumes and send the information by email. The tool can run on Windows and Linux.
Technologies: Java, ExecutorService, JSON, JDBC, Shell Script, Jenkins
Development Tools: Eclipse, Notepad++
