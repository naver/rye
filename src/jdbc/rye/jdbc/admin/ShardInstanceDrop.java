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

class ShardInstanceDrop extends ShardCommand
{
    private String[] globalDbnameArr;
    private String shardMgmtHost;
    private String[] dbaPasswordArr;
    protected NodeInfo dropNode;
    private ShardMgmtInfo[] shardMgmtInfoArr;
    String[] argNodeInfo;

    String commandName()
    {
	return "instanceDrop";
    }

    void printUsage(PrintStream out, String className)
    {
	out.printf("usage: java %s %s GLOBAL_DBNAME SHARD_MGMT_HOST NODE_INFO%n%n", className, commandName());
	out.printf("valid options:%n");
	out.printf("\t--local-mgmt-port=PORT		local mgmt port (default:%d)%n", DEFAULT_LOCAL_MGMT_PORT);
	out.printf("\t--password=PASSWORD		dba password%n");
    }

    void getArgs(String[] optArgs, String[] args, PrintStream out) throws Exception
    {
	String password = "";

	for (int i = 0; i < optArgs.length; i++) {
	    String[] tmpArr = splitArgNameValue(optArgs[i]);
	    String argName = tmpArr[0];
	    String argValue = tmpArr[1];

	    if (argName.equals("--local-mgmt-port")) {
		if (setLocalMgmtPort(argValue) == false) {
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
	argNodeInfo = new String[1];
	argNodeInfo[0] = args[2];

	globalDbnameArr = splitList(argGlobalDbname, ",", false);
	if (globalDbnameArr.length == 0) {
	    throw makeAdminRyeException(null, "invalid global dbname '%s'", argGlobalDbname);
	}

	shardMgmtHost = argShardMgmtHost.trim().toLowerCase();
	if (shardMgmtHost.length() == 0) {
	    throw makeAdminRyeException(null, "invalid shard mgmt host '%s'", argShardMgmtHost);
	}

	NodeInfo[] tmpArr = NodeInfo.makeNodeInfoArr(argNodeInfo);
	if (tmpArr == null) {
	    throw makeAdminRyeException(null, "invalid node info '%s'", argNodeInfo[0]);
	}
	dropNode = tmpArr[0];

	dbaPasswordArr = new String[globalDbnameArr.length];
	for (int i = 0; i < dbaPasswordArr.length; i++) {
	    dbaPasswordArr[i] = password;
	}
    }

    boolean run() throws Exception
    {
	NodeInfo[] existingNodeInfo = null;
	NodeInfo[] dropNodeInfoArr = { dropNode };

	verifyPassword(shardMgmtHost, globalDbnameArr, dbaPasswordArr);

	shardMgmtInfoArr = getAllShardMgmtInfoFromLocalMgmt(shardMgmtHost);

	String haGroupId = getHaGroupId(new LocalMgmt(shardMgmtHost, getLocalMgmtPort()));

	for (int i = 0; i < globalDbnameArr.length; i++) {
	    ShardMgmtInfo shardMgmtInfo = ShardMgmtInfo.find(shardMgmtInfoArr, globalDbnameArr[i]);

	    RyeConnection con = makeConnection(shardMgmtHost, shardMgmtInfo.getPort(), globalDbnameArr[i], "dba",
			    dbaPasswordArr[i], "rw", "");

	    ShardAdmin shardAdmin = getShardAdmin(con, globalDbnameArr[i], shardMgmtInfo);

	    existingNodeInfo = getExistingNodeInfo(shardAdmin, dropNodeInfoArr, existingNodeInfo);

	    con.close();
	}

	excludeDropNode(existingNodeInfo, dropNode);

	for (int i = 0; i < globalDbnameArr.length; i++) {
	    printStatus(true, "%s: drop instance%n", globalDbnameArr[i]);

	    ShardMgmtInfo shardMgmtInfo = ShardMgmtInfo.find(shardMgmtInfoArr, globalDbnameArr[i]);
	    shardMgmtDropNode(shardMgmtInfo, globalDbnameArr[i], dropNode, dbaPasswordArr[i]);
	}

	long lastTime = System.currentTimeMillis();

	deleteShardMgmtConfForDrop(dropNodeInfoArr);
	brokerRestartForAddDrop(dropNodeInfoArr, globalDbnameArr[0], dbaPasswordArr[0]);

	try {
	    hbStop(dropNodeInfoArr);
	} catch (Exception e) {
	    /* server might be already stopped. ignore stop exception */
	}

	changeHbExistingNode(existingNodeInfo, haGroupId);

	waitDriverSynctime(lastTime);

	return true;
    }

    private void deleteShardMgmtConfForDrop(NodeInfo[] dropNodeInfoArr) throws Exception
    {
	int node1Count = 0;
	NodeInfo[] tmpArr = new NodeInfo[dropNodeInfoArr.length];
	for (int i = 0; i < dropNodeInfoArr.length; i++) {
	    if (dropNodeInfoArr[i].getNodeid() == FIRST_NODEID) {
		tmpArr[node1Count++] = dropNodeInfoArr[i];
	    }
	}

	if (node1Count > 0) {
	    NodeInfo[] node1Arr = new NodeInfo[node1Count];
	    System.arraycopy(tmpArr, 0, node1Arr, 0, node1Count);

	    NodeAddress[] hostArr = NodeInfo.getDistinctHostArr(node1Arr);
	    for (int i = 0; i < hostArr.length; i++) {
		printStatus(true, "%s: delete shard mgmt conf%n", hostArr[i].getIpAddr());
		LocalMgmt localMgmt = new LocalMgmt(hostArr[i].getIpAddr(), getLocalMgmtPort());
		deleteShardMgmtConf(localMgmt);
	    }

	}
    }

    private void excludeDropNode(NodeInfo[] existingNodeArr, NodeInfo dropNode) throws Exception
    {
	for (int i = 0; i < existingNodeArr.length; i++) {
	    existingNodeArr[i] = NodeInfo.excludeDropNode(existingNodeArr[i], dropNode);
	    if (existingNodeArr[i] == null) {
		throw makeAdminRyeException(null, "cannot find the node '%s'", argNodeInfo[0]);
	    }
	}
    }

    private void changeHbExistingNode(NodeInfo[] existingNodeArr, String haGroupId) throws SQLException
    {
	printStatus(true, "change rye_conf%n");

	for (int i = 0; i < existingNodeArr.length; i++) {
	    changeConfHaNodeList(existingNodeArr[i].getHostArr(), null, haGroupId);
	}
	hbReload(existingNodeArr);
    }

    private void shardMgmtDropNode(ShardMgmtInfo shardMgmtInfo, String globalDbname, NodeInfo dropNode, String dbaPasswd)
		    throws SQLException
    {
	RyeConnection con = makeConnection(shardMgmtInfo.getIpAddr(), shardMgmtInfo.getPort(), globalDbname, "dba",
			dbaPasswd, "rw", "");
	ShardAdmin shardAdmin = getShardAdmin(con, globalDbname, shardMgmtInfo);

	String[] dropNodeArg = dropNode.toNodeAddArg(globalDbname);
	for (int i = 0; i < dropNodeArg.length; i++) {
	    shardAdmin.dropNode(dropNodeArg[i]);
	}

	con.close();
    }
}
