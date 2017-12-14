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
import java.util.ArrayList;

import rye.jdbc.driver.RyeConnection;
import rye.jdbc.sharding.ShardAdmin;
import rye.jdbc.sharding.ShardNodeInstance;

class ShardChangeConf extends ShardCommand
{
    private String[] globalDbnameArr;
    private NodeAddress shardMgmtHost;
    private String[] dbaPasswordArr;
    private ArrayList<RyeConfValue> changeRyeConfList = null;
    private ShardMgmtInfo[] shardMgmtInfoArr;

    String commandName()
    {
	return "changeConf";
    }

    void printUsage(PrintStream out, String className)
    {
	out.printf("usage: java %s %s GLOBAL_DBNAME SHARD_MGMT_HOST%n%n", className, commandName());
	out.printf("valid options:%n");
	out.printf("\t--local-mgmt-port=PORT            local mgmt port (default:%d)%n", DEFAULT_LOCAL_MGMT_PORT);
	out.printf("\t--password=PASSWORD		dba password%n");
	out.printf("\t--rye-server-conf=PARAM		rye-auto.conf server parameter%n");
	out.printf("\t--rye-broker-conf=PARAM	rye-auto.conf broker parameter%n");
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
	    else if (argName.equals("--rye-server-conf")) {
		if (changeRyeConfList == null) {
		    changeRyeConfList = new ArrayList<RyeConfValue>();
		}
		changeRyeConfList.add(RyeServerConfValue.valueOf(argValue));
	    }
	    else if (argName.equals("--rye-broker-conf")) {
		if (changeRyeConfList == null) {
		    changeRyeConfList = new ArrayList<RyeConfValue>();
		}
		changeRyeConfList.add(RyeBrokerConfValue.valueOf(argValue));
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

	shardMgmtHost = new NodeAddress(argShardMgmtHost.trim().toLowerCase(), localMgmtPort);

	dbaPasswordArr = new String[globalDbnameArr.length];
	for (int i = 0; i < dbaPasswordArr.length; i++) {
	    dbaPasswordArr[i] = password;
	}
    }

    boolean run() throws Exception
    {
	if (changeRyeConfList == null) {
	    return true;
	}

	verifyPassword(shardMgmtHost, globalDbnameArr, dbaPasswordArr);

	shardMgmtInfoArr = getAllShardMgmtInfoFromLocalMgmt(shardMgmtHost);

	String globalDbname = globalDbnameArr[0];
	String dbaPasswd = dbaPasswordArr[0];

	ShardMgmtInfo shardMgmtInfo = ShardMgmtInfo.find(shardMgmtInfoArr, globalDbname);

	RyeConnection con = makeConnection(shardMgmtHost.getIpAddr(), shardMgmtInfo.getPort(), globalDbname, "dba",
			dbaPasswd, "rw", null);

	ShardAdmin shardAdmin = getShardAdmin(con, globalDbname, shardMgmtInfo);

	con.close();

	ShardNodeInstance[] instArr = shardAdmin.getShardNodeInstance();

	int failCount = 0;

	if (changeConf(instArr, globalDbname) == false) {
	    failCount++;
	}

	return (failCount == 0);
    }

    private boolean changeConf(ShardNodeInstance[] instArr, String globalDbname)
    {
	if (instArr == null) {
	    return false;
	}

	boolean res = true;

	for (int i = 0; i < instArr.length; i++) {
	    LocalMgmt localMgmt = null;

	    String host = instArr[i].getHost();
	    int port = instArr[i].getPort();

	    try {
		localMgmt = new LocalMgmt(host, port);
	    } catch (Exception e) {
		errStream.printf("ERROR: %s:%d connection fail%n", host, port);
		if (verboseOut != null) {
		    verboseOut.printf("ERROR: %s:%d connection fail%n", host, port);

		}
		res = false;
	    }

	    for (RyeConfValue ryeConfValue : changeRyeConfList) {
		if (verboseOut != null) {
		    verboseOut.printf("%s:%s	%s.%s.%s ... ", globalDbname, host, ryeConfValue.getProcName(),
				    ryeConfValue.getSectName(), ryeConfValue.getKeyName());
		}
		try {
		    changeRyeConf(localMgmt, ryeConfValue);

		    if (verboseOut != null) {
			verboseOut.printf("success%n");
		    }
		} catch (Exception e) {
		    errStream.printf("%s:%s	%s.%s.%s ... fail%n", globalDbname, host, ryeConfValue.getProcName(),
				    ryeConfValue.getSectName(), ryeConfValue.getKeyName());
		    if (verboseOut != null) {
			verboseOut.printf("fail%n");
		    }
		    res = false;
		}
	    }
	}

	return res;
    }
}
