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
import java.sql.SQLException;

import rye.jdbc.driver.RyeConnection;
import rye.jdbc.sharding.ShardAdmin;

class ShardNodeDrop extends ShardCommand
{
    private String[] globalDbnameArr;
    private NodeAddress shardMgmtHost;
    private String[] dbaPasswordArr;
    private int dropAllNodeid;

    private ShardMgmtInfo[] shardMgmtInfoArr;

    String commandName()
    {
	return "nodeDrop";
    }

    void printUsage(PrintStream out, String className)
    {
	out.printf("usage: java %s %s GLOBAL_DBNAME SHARD_MGMT_HOST NODEID%n%n", className, commandName());
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

	if (args.length != 3) {
	    throw makeAdminRyeException(null, "invalid option");
	}

	String argGlobalDbname = args[0];
	String argShardMgmtHost = args[1];
	String argNodeid = args[2];

	globalDbnameArr = splitList(argGlobalDbname, ",", false);
	if (globalDbnameArr.length == 0) {
	    throw makeAdminRyeException(null, "invalid global dbname '%s'", argGlobalDbname);
	}

	shardMgmtHost = new NodeAddress(argShardMgmtHost, localMgmtPort);

	dropAllNodeid = Integer.parseInt(argNodeid);
	if (dropAllNodeid <= 1) {
	    throw makeAdminRyeException(null, "invalid nodeid '%d'", dropAllNodeid);
	}

	dbaPasswordArr = new String[globalDbnameArr.length];
	for (int i = 0; i < dbaPasswordArr.length; i++) {
	    dbaPasswordArr[i] = password;
	}
    }

    boolean run() throws Exception
    {
	verifyPassword(shardMgmtHost, globalDbnameArr, dbaPasswordArr);

	shardMgmtInfoArr = getAllShardMgmtInfoFromLocalMgmt(shardMgmtHost);

	for (int i = 0; i < globalDbnameArr.length; i++) {
	    ShardMgmtInfo shardMgmtInfo = ShardMgmtInfo.find(shardMgmtInfoArr, globalDbnameArr[i]);

	    RyeConnection con = makeConnection(shardMgmtHost.getIpAddr(), shardMgmtInfo.getPort(), globalDbnameArr[i],
			    "dba", dbaPasswordArr[i], "rw", null);

	    ShardAdmin shardAdmin = getShardAdmin(con, globalDbnameArr[i], shardMgmtInfo);

	    if (shardAdmin.hasNodeid(dropAllNodeid) == false) {
		throw makeAdminRyeException(null, "nodeid '%d' is not found", dropAllNodeid);
	    }

	    con.close();
	}

	for (int i = 0; i < globalDbnameArr.length; i++) {
	    printStatus(true, "%s: drop node%n", globalDbnameArr[i]);

	    ShardMgmtInfo shardMgmtInfo = ShardMgmtInfo.find(shardMgmtInfoArr, globalDbnameArr[i]);
	    shardMgmtDropAllNodeid(shardMgmtInfo, globalDbnameArr[i], dropAllNodeid, dbaPasswordArr[i]);
	}

	waitDriverSynctime(System.currentTimeMillis());

	return true;
    }

    private void shardMgmtDropAllNodeid(ShardMgmtInfo shardMgmtInfo, String globalDbname, int nodeid, String dbaPasswd)
		    throws SQLException
    {
	RyeConnection con = null;

	con = makeConnection(shardMgmtInfo.getIpAddr(), shardMgmtInfo.getPort(), globalDbname, "dba", dbaPasswd, "rw",
			null);
	ShardAdmin shardAdmin = getShardAdmin(con, globalDbname, shardMgmtInfo);

	shardAdmin.dropNodeidAll(nodeid);

	con.close();
    }
}
