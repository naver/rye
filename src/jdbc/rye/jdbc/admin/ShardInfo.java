/*
 * Copyright 2017 NAVER Corp.
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted
 * provided that the following conditions are met:
 *
 *  - Redistributions of source code must retain the above copyright notice, this list of conditions and the
 * following disclaimer.
 *  - Redistributions in binary form must reproduce the above copyright notice, this list of conditions and
 * the following disclaimer in the documentation and/or other materials provided with the distribution.
 *  - Neither the name of Search Solution Coporation nor the names of its contributors may be used to
 * endorse or promote products derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL
 * THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF
 * THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package rye.jdbc.admin;

import java.io.PrintStream;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import rye.jdbc.driver.RyeConnection;

class ShardInfo extends ShardCommand
{
    private NodeAddress shardMgmtHost;
    private String[] dbaPasswordArr;
    private String[] globalDbnameArr;
    private final int COL_SPACE_SIZE = 4;
    private final int INDENT_SIZE = 4;

    String commandName()
    {
	return "info";
    }

    void printUsage(PrintStream out, String className)
    {
	out.printf("usage: java %s %s GLOBAL_DBNAME SHARD_MGMT_HOST %n%n", className, commandName());
	out.printf("valid options:%n");
	out.printf("\t--local-mgmt-port=PORT		local mgmt port (default:%d)%n", DEFAULT_LOCAL_MGMT_PORT);
	out.printf("\t--password=PASSWORD		dba password%n");
    }

    void getArgs(String[] optArgs, String[] args, PrintStream out) throws Exception
    {
	String password = "";
	int localMgmtPort = ShardCommand.DEFAULT_LOCAL_MGMT_PORT;

	for (int i = 0; i < optArgs.length; i++) {
	    String[] tmpArr = splitArgNameValue(optArgs[i]);
	    String argName = tmpArr[0];
	    String argValue = tmpArr[1];

	    if (argName.equals("--local-mgmt-port")) {
		localMgmtPort = Integer.parseInt(argValue);
		if (localMgmtPort <= 0) {
		    throw makeAdminRyeException(null, "invalid option value: %s", optArgs[i]);
		}
	    }
	    else if (argName.equals("--password")) {
		password = argValue;
	    }
	    else if (argName.equals("-v")) {
		setVerboseOut(out);
	    }
	    else {
		throw makeAdminRyeException(null, "invalid option: %s", optArgs[i]);
	    }
	}

	if (args.length != 2) {
	    throw makeAdminRyeException(null, "invalid option");
	}

	String argGlobalDbname = args[0];
	String argShardMgmtHost = args[1];

	globalDbnameArr = splitList(argGlobalDbname, ",", false);
	if (globalDbnameArr.length == 0) {
	    throw makeAdminRyeException(null, "invalid global dbname '%s'", argGlobalDbname);
	}

	shardMgmtHost = new NodeAddress(argShardMgmtHost, localMgmtPort);

	dbaPasswordArr = new String[globalDbnameArr.length];
	for (int i = 0; i < dbaPasswordArr.length; i++) {
	    dbaPasswordArr[i] = password;
	}
    }

    boolean run() throws Exception
    {

	LocalMgmt localMgmt = new LocalMgmt(shardMgmtHost.toJciConnectionInfo());
	ShardMgmtInfo[] shardMgmtInfoArr = localMgmt.getShardMgmtInfo();

	PrintStream out = System.out;

	for (int i = 0; i < globalDbnameArr.length; i++) {
	    out.printf("DBNAME: %s%n", globalDbnameArr[i]);

	    ShardMgmtInfo shardMgmtInfo = ShardMgmtInfo.find(shardMgmtInfoArr, globalDbnameArr[i]);
	    String localDbname = NodeInfo.getLocalDbname(globalDbnameArr[i], FIRST_NODEID);

	    RyeConnection con = makeLocalConnection(shardMgmtHost.getIpAddr(), shardMgmtInfo.getPort(), localDbname,
			    "dba", dbaPasswordArr[i], "rw", null);

	    String nodeInfoSql = "SELECT nodeid, dbname as `local-dbname`, host, port FROM shard_node";
	    printShardInfo(con, "SHARD NODE:", nodeInfoSql, out);

	    String groupInfoSql = "SELECT nodeid, count(*) as `groupid-count` FROM shard_groupid GROUP BY nodeid";
	    printShardInfo(con, "GROUPID:", groupInfoSql, out);

	    String migInfoSql = "SELECT src_nodeid, dest_nodeid, "
			    + "decode(status, 1, 'SCHEDULED', 2, 'READY', 3, 'RUNNING', 4, 'COMPLETE', 5, 'FAILED', '-') as status, "
			    + "count(*) as `groupid-count` FROM shard_migration GROUP BY src_nodeid, status, dest_nodeid";
	    printShardInfo(con, "MIGRATION:", migInfoSql, out);

	    con.close();
	}

	return true;
    }

    private void printShardInfo(RyeConnection con, String title, String sql, PrintStream out) throws Exception
    {
	Statement stmt = con.createStatement();
	ResultSet rs = stmt.executeQuery(sql);
	out.println(title);
	printResultSet(rs, out, INDENT_SIZE);
	rs.close();
	stmt.close();
    }

    private void printResultSet(ResultSet rs, PrintStream out, int indentSize) throws SQLException
    {
	ResultSetMetaData rsmd = rs.getMetaData();
	int numCols = rsmd.getColumnCount();
	ArrayList<String[]> resArr = new ArrayList<String[]>();
	int[] colDisplayLen = new int[numCols];
	String[] title = new String[numCols];

	for (int i = 0; i < numCols; i++) {
	    title[i] = rsmd.getColumnLabel(i + 1);
	    colDisplayLen[i] = title[i].length();
	}
	while (rs.next()) {
	    String[] data = new String[numCols];
	    for (int i = 0; i < numCols; i++) {
		data[i] = rs.getString(i + 1);
		if (data[i].length() > colDisplayLen[i]) {
		    colDisplayLen[i] = data[i].length();
		}
	    }
	    resArr.add(data);
	}

	int lineSize = (numCols - 1) * COL_SPACE_SIZE;
	String[] colFormat = new String[numCols];
	for (int i = 0; i < numCols; i++) {
	    colFormat[i] = String.format("%%-%ds", colDisplayLen[i]);
	    lineSize += colDisplayLen[i];
	}

	printRow(out, indentSize, title, colFormat);
	printChar(out, ' ', indentSize);
	printChar(out, '-', lineSize);
	printNewline(out);
	for (String[] row : resArr) {
	    printRow(out, indentSize, row, colFormat);
	}

	printNewline(out);
    }

    private void printRow(PrintStream out, int indentSize, String[] cols, String[] colFormat)
    {
	printChar(out, ' ', indentSize);
	for (int i = 0; i < cols.length; i++) {
	    if (i != 0) {
		printChar(out, ' ', COL_SPACE_SIZE);
	    }
	    out.printf(colFormat[i], cols[i]);
	}
	printNewline(out);
    }

    private void printChar(PrintStream out, char ch, int size)
    {
	for (int i = 0; i < size; i++) {
	    out.print(ch);
	}
    }

    private void printNewline(PrintStream out)
    {
	out.println("");
    }
}
