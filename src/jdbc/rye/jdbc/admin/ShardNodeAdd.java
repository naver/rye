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
import java.util.ArrayList;

import rye.jdbc.driver.RyeConnection;
import rye.jdbc.sharding.ShardAdmin;

class ShardNodeAdd extends ShardCommand
{
    protected String[] globalDbnameArr;
    protected NodeAddress shardMgmtHost;
    protected NodeInfo[] addNodeArr;
    protected String[] dbaPasswordArr;

    protected byte[] ryeConfContents;
    protected byte[] brokerAcl;
    protected ShardMgmtInfo[] shardMgmtInfoArr;
    protected String haGroupId;
    private ArrayList<String> createdbOption;

    String commandName()
    {
	return "nodeAdd";
    }

    void printUsage(PrintStream out, String className)
    {
	boolean isInstanceAdd = (this instanceof ShardInstanceAdd);

	out.printf("usage: java %s %s GLOBAL_DBNAME SHARD_MGMT_HOST NODE_INFO%n%n", className, commandName());
	out.printf("valid options:%n");
	out.printf("\t--local-mgmt-port=PORT		local mgmt port (default:%d)%n", DEFAULT_LOCAL_MGMT_PORT);
	out.printf("\t--password=PASSWORD		dba password%n");
	if (isInstanceAdd == false) {
	    out.printf("\t--createdb-option=PARAM	rye createdb option%n");
	}
    }

    void getArgs(String[] optArgs, String[] args, PrintStream out) throws Exception
    {
	String password = "";
	boolean isInstanceAdd = (this instanceof ShardInstanceAdd);
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
	    else if (isInstanceAdd == false && argName.equals("--createdb-option")) {
		if (createdbOption == null) {
		    createdbOption = new ArrayList<String>();
		}
		createdbOption.add(argValue);
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
	String argNodeInfo = args[2];

	globalDbnameArr = splitList(argGlobalDbname, ",", false);
	if (globalDbnameArr.length == 0) {
	    throw makeAdminRyeException(null, "invalid global dbname '%s'", argGlobalDbname);
	}

	shardMgmtHost = new NodeAddress(argShardMgmtHost, localMgmtPort);

	addNodeArr = NodeInfo.makeNodeInfoArr(splitList(argNodeInfo, ",", false), localMgmtPort);
	if (addNodeArr == null) {
	    throw makeAdminRyeException(null, "invalid node info '%s'", argNodeInfo);
	}

	dbaPasswordArr = new String[globalDbnameArr.length];
	for (int i = 0; i < dbaPasswordArr.length; i++) {
	    dbaPasswordArr[i] = password;
	}
    }

    protected NodeInfo[] preProcessing(boolean isNodeAdd) throws Exception
    {
	NodeInfo[] existingNodeInfo = null;

	checkLocalMgmtOccupied(addNodeArr);

	verifyPassword(shardMgmtHost, globalDbnameArr, dbaPasswordArr);

	LocalMgmt localMgmt = new LocalMgmt(shardMgmtHost.toJciConnectionInfo());
	shardMgmtInfoArr = localMgmt.getShardMgmtInfo();

	for (int i = 0; i < globalDbnameArr.length; i++) {
	    ShardMgmtInfo shardMgmtInfo = ShardMgmtInfo.find(shardMgmtInfoArr, globalDbnameArr[i]);

	    RyeConnection con = makeConnection(shardMgmtHost.getIpAddr(), shardMgmtInfo.getPort(), globalDbnameArr[i],
			    "dba", dbaPasswordArr[i], "rw", "");

	    ShardAdmin shardAdmin = getShardAdmin(con, globalDbnameArr[i], shardMgmtInfo);

	    if (isNodeAdd) {
		isAvailableNodeid(shardAdmin);
	    }
	    else {
		existingNodeInfo = getExistingNodeInfo(shardAdmin, addNodeArr, existingNodeInfo, shardMgmtHost.getPort());
	    }

	    con.close();
	}

	brokerAcl = localMgmt.readBrokerAclConf();

	ryeConfContents = localMgmt.readRyeConf();

	haGroupId = getHaGroupId(localMgmt);

	return existingNodeInfo;
    }

    boolean run() throws Exception
    {
	preProcessing(true);

	initDB(addNodeArr, globalDbnameArr, brokerAcl, ryeConfContents, null, null, haGroupId, createdbOption);

	for (int i = 0; i < globalDbnameArr.length; i++) {
	    printStatus(true, "%s: add node%n", globalDbnameArr[i]);

	    ShardMgmtInfo shardMgmtInfo = ShardMgmtInfo.find(shardMgmtInfoArr, globalDbnameArr[i]);
	    shardMgmtNodeAdd(shardMgmtInfo, globalDbnameArr[i], addNodeArr, dbaPasswordArr[i]);
	}

	long lastTime = System.currentTimeMillis();

	for (int i = 0; i < addNodeArr.length; i++) {
	    checkNodeidInitialized(addNodeArr[i], globalDbnameArr);
	}

	for (int i = 0; i < globalDbnameArr.length; i++) {
	    ShardMgmtInfo shardMgmtInfo = ShardMgmtInfo.find(shardMgmtInfoArr, globalDbnameArr[i]);
	    refreshShardInfo(shardMgmtInfo, globalDbnameArr[i], dbaPasswordArr[i], NodeInfo.getAllNodeidArr(addNodeArr));
	}

	brokerRestartForAddDrop(addNodeArr, globalDbnameArr[0], dbaPasswordArr[0]);

	waitDriverSynctime(lastTime);

	return true;
    }

    private void isAvailableNodeid(ShardAdmin shardAdmin) throws SQLException
    {
	for (int i = 0; i < addNodeArr.length; i++) {
	    if (shardAdmin.hasNodeid(addNodeArr[i].getNodeid())) {
		throw makeAdminRyeException(null, "nodeid '%d' is in use", addNodeArr[i].getNodeid());
	    }
	}
    }

    void shardMgmtNodeAdd(ShardMgmtInfo shardMgmtInfo, String globalDbname, NodeInfo[] addNode, String dbaPasswd)
		    throws SQLException
    {
	for (int i = 0; i < addNode.length; i++) {
	    shardMgmtNodeAdd(shardMgmtInfo, globalDbname, addNode[i], dbaPasswd);
	}
    }

    private void shardMgmtNodeAdd(ShardMgmtInfo shardMgmtInfo, String globalDbname, NodeInfo addNode, String dbaPasswd)
		    throws SQLException
    {
	RyeConnection con = null;

	con = makeConnection(shardMgmtInfo.getIpAddr(), shardMgmtInfo.getPort(), globalDbname, "dba", dbaPasswd, "rw",
			"");
	ShardAdmin shardAdmin = getShardAdmin(con, globalDbname, shardMgmtInfo);

	String[] addArg = addNode.toNodeAddArg(globalDbname);
	for (int i = 0; i < addArg.length; i++) {
	    shardAdmin.addNode(addArg[i]);
	}

	con.close();
    }
}
