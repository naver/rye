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

class ShardInit extends ShardCommand
{
    private static final int HA_GROUP_ID_LEN = 16;

    private String[] globalDbnameArr;
    private NodeInfo shardMgmtHost;
    private NodeInfo[] addNodeArr;
    private int numGroups = DEFAULT_NUM_GROUPS;
    private ArrayList<RyeConfValue> changeRyeConf = null;
    private ArrayList<String> createdbOption = null;
    private boolean registerFirstNodeOnly = false;
    private byte[] brokerAcl = null;

    String commandName()
    {
	return "init";
    }

    void printUsage(PrintStream out, String className)
    {
	out.printf("usage: java %s %s GLOBAL_DBNAME SHARD_MGMT_HOST_LIST%n%n", className, commandName());
	out.printf("valid options:%n");
	out.printf("\t--add-node=NODE_INFO		additional node infomation%n");
	out.printf("\t				nodeid:host[:port],...%n");
	out.printf("\t--local-mgmt-port=PORT		local mgmt port (default:%d)%n", DEFAULT_LOCAL_MGMT_PORT);
	out.printf("\t--num-groups=GROUPS		number of shard key hash groups (default:%d)%n", DEFAULT_NUM_GROUPS);
	out.printf("\t--createdb-option=PARAM	rye createdb option%n");
	out.printf("\t--rye-server-conf=PARAM		rye-auto.conf server parameter%n");
	out.printf("\t--rye-broker-conf=PARAM	rye-auto.conf broker parameter%n");
    }

    void getArgs(String[] optArgs, String[] args, PrintStream out) throws Exception
    {
	String argAddNode = null;
	int localMgmtPort = ShardCommand.DEFAULT_LOCAL_MGMT_PORT;

	for (int i = 0; i < optArgs.length; i++) {
	    String[] tmpArr = splitArgNameValue(optArgs[i]);
	    String argName = tmpArr[0];
	    String argValue = tmpArr[1];

	    if (argName.equals("--add-node")) {
		argAddNode = argValue;
	    }
	    else if (argName.equals("--local-mgmt-port")) {
		localMgmtPort = Integer.parseInt(argValue);
		if (localMgmtPort <= 0) {
		    throw makeAdminRyeException(null, "invalid option value: %s", optArgs[i]);
		}
	    }
	    else if (argName.equals("--num-groups")) {
		numGroups = Integer.parseInt(argValue);
		if (numGroups <= 0 || numGroups > 1000000) {
		    throw makeAdminRyeException(null, "invalid option value: %s", optArgs[i]);
		}
	    }
	    else if (argName.equals("--rye-server-conf")) {
		if (changeRyeConf == null) {
		    changeRyeConf = new ArrayList<RyeConfValue>();
		}
		changeRyeConf.add(RyeServerConfValue.valueOf(argValue));
	    }
	    else if (argName.equals("--rye-broker-conf")) {
		if (changeRyeConf == null) {
		    changeRyeConf = new ArrayList<RyeConfValue>();
		}
		changeRyeConf.add(RyeBrokerConfValue.valueOf(argValue));
	    }
	    else if (argName.equals("--createdb-option")) {
		if (createdbOption == null) {
		    createdbOption = new ArrayList<String>();
		}
		createdbOption.add(argValue);
	    }
	    else if (argName.equals("--register-node1-only")) {
		if (argValue == null) {
		    registerFirstNodeOnly = true;
		}
		else {
		    registerFirstNodeOnly = getBooleanArg(argValue);
		}
	    }
	    else if (argName.equals("--broker-acl")) {
		if (argValue == null) {
		    throw makeAdminRyeException(null, "invalid option value: %s", optArgs[i]);
		}
		brokerAcl = argValue.getBytes();
	    }
	    else if (argName.equals("-v")) {
		setVerboseOut(out);
	    }
	    else {
		throw makeAdminRyeException(null, "invalid option: %s", optArgs[i]);
	    }
	}

	if (args.length < 2) {
	    throw makeAdminRyeException(null, "invalid option");
	}

	if (argAddNode != null && argAddNode.length() > 0) {
	    addNodeArr = NodeInfo.makeNodeInfoArr(splitList(argAddNode, ",", false), localMgmtPort);
	    if (addNodeArr == null) {
		throw makeAdminRyeException(null, "invalid option value: %s", argAddNode);
	    }
	}

	String argGlobalDbanme = args[0];
	String argShardMgmtHost = args[1];

	globalDbnameArr = splitList(argGlobalDbanme, ",", false);
	if (globalDbnameArr.length == 0) {
	    throw makeAdminRyeException(null, "invalid global dbname '%s'", argGlobalDbanme);
	}

	shardMgmtHost = NodeInfo.makeNodeInfo(FIRST_NODEID, splitList(argShardMgmtHost, ",", true), localMgmtPort);
	if (shardMgmtHost == null) {
	    throw makeAdminRyeException(null, "invalid shard mgmt host '%s'", argShardMgmtHost);
	}

	if (addNodeArr == null) {
	    addNodeArr = new NodeInfo[1];
	    addNodeArr[0] = shardMgmtHost;
	}
	else {
	    int node1Idx = NodeInfo.findNode(FIRST_NODEID, addNodeArr);
	    if (node1Idx < 0) {
		NodeInfo[] tmpArr = new NodeInfo[addNodeArr.length + 1];
		tmpArr[0] = shardMgmtHost;
		System.arraycopy(addNodeArr, 0, tmpArr, 1, addNodeArr.length);
		addNodeArr = tmpArr;
	    }
	    else {
		addNodeArr[node1Idx] = new NodeInfo(FIRST_NODEID, shardMgmtHost.getHostArr(),
				addNodeArr[node1Idx].getHostArr());
	    }

	}
    }

    boolean run() throws Exception
    {
	checkLocalMgmtOccupied(addNodeArr);

	initDB(addNodeArr, globalDbnameArr, brokerAcl, null, changeRyeConf, null, makeRandomeId(HA_GROUP_ID_LEN),
			createdbOption);

	NodeAddress[] hosts = shardMgmtHost.getHostArr();
	String[] node1LocalDbname = shardMgmtHost.getLocalDbnameArr(globalDbnameArr);

	for (int i = 0; i < hosts.length; i++) {
	    printStatus(true, "%s: add shard_mgmt settings %n", hosts[i].getIpAddr());

	    createShardMgmt(hosts[i], node1LocalDbname);
	}

	for (int i = 0; i < hosts.length; i++) {
	    printStatus(true, "%s: broker restart %n", hosts[i].getIpAddr());

	    brokerRestart(hosts[i]);
	}

	ShardMgmtInfo[] primaryShardMgmtInfo = new ShardMgmtInfo[globalDbnameArr.length];
	for (int i = 0; i < globalDbnameArr.length; i++) {
	    primaryShardMgmtInfo[i] = new ShardMgmtInfo(globalDbnameArr[i], hosts[0]);
	}

	for (int i = 0; i < globalDbnameArr.length; i++) {
	    testLocalConnection(hosts[0], node1LocalDbname[i], "dba", "", "rw", true, 0);
	    testGlobalConnection(hosts[0], globalDbnameArr[i]);
	}

	initShardMgmt(primaryShardMgmtInfo);

	long lastTime = System.currentTimeMillis();

	for (int i = 0; i < addNodeArr.length; i++) {
	    if (registerFirstNodeOnly && addNodeArr[i].getNodeid() != FIRST_NODEID) {
		continue;
	    }
	    checkNodeidInitialized(addNodeArr[i], globalDbnameArr);
	}

	int[] checkAllNodeidArr;
	if (registerFirstNodeOnly) {
	    checkAllNodeidArr = new int[1];
	    checkAllNodeidArr[0] = FIRST_NODEID;
	}
	else {
	    checkAllNodeidArr = NodeInfo.getAllNodeidArr(addNodeArr);
	}
	for (int i = 0; i < globalDbnameArr.length; i++) {
	    printStatus(true, "%s: refresh shard info%n", globalDbnameArr[i]);

	    refreshShardInfo(primaryShardMgmtInfo[i], globalDbnameArr[i], "", checkAllNodeidArr);
	}

	waitDriverSynctime(lastTime);

	return true;
    }

    private void initShardMgmt(ShardMgmtInfo[] primaryShardMgmtInfo) throws SQLException
    {
	for (int i = 0; i < globalDbnameArr.length; i++) {
	    printStatus(true, "%s: initialize shard infomation%n", globalDbnameArr[i]);

	    RyeConnection con = makeConnection(primaryShardMgmtInfo[i].getIpAddr(), primaryShardMgmtInfo[i].getPort(),
			    globalDbnameArr[i], "dba", "", "rw", "useLazyConnection=true");
	    ShardAdmin shardAdmin = con.getShardAdmin();

	    String[] initNodeArg = NodeInfo.toNodeAddArg(addNodeArr, globalDbnameArr[i], registerFirstNodeOnly);

	    shardAdmin.init(globalDbnameArr[i], numGroups, initNodeArg);

	    con.close();
	}
    }

    private void createShardMgmt(NodeAddress host, String[] localDbname) throws SQLException
    {
	String dbnameList = concatStrArr(localDbname, ",", false);

	LocalMgmt localMgmt = new LocalMgmt(host.toJciConnectionInfo());

	changeRyeConf(localMgmt, new RyeBrokerShardmgmtConfValue("shard_mgmt_metadb", dbnameList));
	changeRyeConf(localMgmt, new RyeBrokerShardmgmtConfValue("shard_mgmt_num_migrator", "10"));
    }
}
