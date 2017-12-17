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
import java.util.Calendar;
import java.util.Random;
import java.util.regex.Pattern;

import rye.jdbc.driver.RyeCommand;
import rye.jdbc.driver.RyeConnection;
import rye.jdbc.driver.RyeDriver;
import rye.jdbc.driver.RyeException;
import rye.jdbc.sharding.ShardAdmin;
import rye.jdbc.sharding.ShardInfoManager;
import rye.jdbc.sharding.ShardNodeInstance;

abstract class ShardCommand
{
    protected final static int DEFAULT_LOCAL_MGMT_PORT = 30000;
    protected final static int DEFAULT_NUM_GROUPS = 100000;
    protected final static int FIRST_NODEID = 1;
    private final static String[] booleanOptValueTrue = { "true", "yes", "on" };
    private final static String[] booleanOptValueFalse = { "false", "no", "off" };

    private final static Pattern wsPattern = Pattern.compile("\\p{Space}");

    protected static RyeDriver ryeDriver = new RyeDriver();

    private final ShardInfoManager shardInfoManager;
    private final Calendar cal;

    protected PrintStream errStream = null;
    protected PrintStream verboseOut = null;

    ShardCommand()
    {
	shardInfoManager = new ShardInfoManager();
	cal = Calendar.getInstance();
    }

    void clear()
    {
	shardInfoManager.clear();
    }

    abstract String commandName();

    abstract void getArgs(String[] optargs, String[] args, PrintStream out) throws Exception;

    abstract void printUsage(PrintStream out, String className);

    abstract boolean run() throws Exception;

    protected void setVerboseOut(PrintStream out)
    {
	this.verboseOut = out;
    }

    void setErrStream(PrintStream err)
    {
	this.errStream = err;
    }

    void printStatus(boolean printTime, String format, Object... args)
    {
	if (verboseOut != null) {
	    if (printTime) {
		cal.setTimeInMillis(System.currentTimeMillis());
		verboseOut.printf("%d-%02d-%02d %02d:%02d:%02d ", cal.get(Calendar.YEAR), cal.get(Calendar.MONTH) + 1,
				cal.get(Calendar.DAY_OF_MONTH), cal.get(Calendar.HOUR_OF_DAY),
				cal.get(Calendar.MINUTE), cal.get(Calendar.SECOND));
	    }

	    verboseOut.printf(format, args);
	}
    }

    static String[] splitList(String str, String delimiter, boolean toLower)
    {
	String[] tmpArr = str.split(delimiter);

	int count = 0;

	for (int i = 0; i < tmpArr.length; i++) {
	    tmpArr[i] = tmpArr[i].trim();
	    if (toLower) {
		tmpArr[i] = tmpArr[i].toLowerCase();
	    }

	    if (tmpArr[i].length() > 0) {
		count++;
	    }
	}

	String[] arr = new String[count];
	count = 0;
	for (int i = 0; i < tmpArr.length; i++) {
	    if (tmpArr[i].length() > 0) {
		arr[count++] = tmpArr[i];
	    }
	}

	return arr;
    }

    static String[] splitArgNameValue(String arg)
    {
	String[] nameValue = new String[2];

	int idx = arg.indexOf('=');
	if (idx >= 0) {
	    nameValue[0] = arg.substring(0, idx).trim();
	    nameValue[1] = arg.substring(idx + 1).trim();
	}
	else {
	    nameValue[0] = arg;
	    nameValue[1] = null;
	}
	return nameValue;
    }

    static boolean getBooleanArg(String value) throws Exception
    {
	for (int i = 0; i < booleanOptValueTrue.length; i++) {
	    if (value.equalsIgnoreCase(booleanOptValueTrue[i])) {
		return true;
	    }
	}
	for (int i = 0; i < booleanOptValueFalse.length; i++) {
	    if (value.equalsIgnoreCase(booleanOptValueFalse[i])) {
		return false;
	    }
	}
	throw makeAdminRyeException(null, "invalid option value: %s", value);
    }

    static Integer makeInteger(String str)
    {
	try {
	    return Integer.parseInt(str);
	} catch (Exception e) {
	    return null;
	}
    }

    private static String[] getHostInfoArr(ShardAdmin shardAdmin, int nodeid)
    {
	ShardNodeInstance[] instArr = shardAdmin.getShardNodeInstance(nodeid);
	if (instArr == null || instArr.length == 0) {
	    return null;
	}

	String[] hostInfoArr = new String[instArr.length];
	for (int i = 0; i < instArr.length; i++) {
	    hostInfoArr[i] = instArr[i].getHost() + ":" + instArr[i].getPort();
	}

	return hostInfoArr;
    }

    static NodeInfo[] getExistingNodeInfo(ShardAdmin shardAdmin, NodeInfo[] addDropNodeArr, NodeInfo[] prevNodeArr,
		    int localMgmtPort) throws Exception
    {
	NodeInfo[] nodeInfoArr = new NodeInfo[addDropNodeArr.length];

	for (int i = 0; i < addDropNodeArr.length; i++) {
	    int nodeid = addDropNodeArr[i].getNodeid();
	    String[] hostInfoArr = getHostInfoArr(shardAdmin, nodeid);
	    if (hostInfoArr == null) {
		throw makeAdminRyeException(null, "nodeid '%d' is not found", nodeid);
	    }

	    nodeInfoArr[i] = NodeInfo.makeNodeInfo(nodeid, hostInfoArr, localMgmtPort);
	}

	if (prevNodeArr != null) {
	    for (int i = 0; i < nodeInfoArr.length; i++) {
		if (nodeInfoArr[i].hasSameHosts(prevNodeArr[i]) == false) {
		    throw makeAdminRyeException(null, "invalid node configuration");
		}
	    }
	}

	return nodeInfoArr;
    }

    ShardMgmtInfo[] getAllShardMgmtInfoFromLocalMgmt(NodeAddress localMgmtHost) throws SQLException
    {
	LocalMgmt localMgmt = new LocalMgmt(localMgmtHost.toJciConnectionInfo());
	return localMgmt.getShardMgmtInfo();
    }

    static ShardAdmin getShardAdmin(RyeConnection con, String globalDbname, ShardMgmtInfo shardMgmtInfo)
		    throws SQLException
    {
	ShardAdmin shardAdmin = con.getShardAdmin();
	if (shardAdmin == null) {
	    throw makeAdminRyeException(null, "cannot get shard node info of '%s' from '%s'", globalDbname,
			    shardMgmtInfo.getOrgHostname());
	}
	return shardAdmin;
    }

    RyeConnection makeConnection(String brokerHost, int brokerPort, String dbName, String dbUser, String dbPassword,
		    String portName, String urlProperty) throws SQLException
    {
	String url = makeConnectionUrl(brokerHost, brokerPort, dbName, "", "", portName, urlProperty, true);
	return (RyeConnection) ryeDriver.connect(url, dbUser, dbPassword, shardInfoManager);
    }

    RyeConnection makeLocalConnection(String brokerHost, int brokerPort, String dbName, String dbUser,
		    String dbPassword, String portName, String urlProperty) throws SQLException
    {
	String url = makeConnectionUrl(brokerHost, brokerPort, dbName, "", "", portName, urlProperty, false);
	return (RyeConnection) ryeDriver.connect(url, dbUser, dbPassword, shardInfoManager);
    }

    private static String makeConnectionUrl(String brokerHost, int brokerPort, String dbName, String dbUser,
		    String dbPassword, String portName, String urlProperty, boolean isGlobalConn)
    {
	if (dbUser == null) {
	    dbUser = "";
	}
	if (dbPassword == null) {
	    dbPassword = "";
	}
	if (urlProperty == null) {
	    urlProperty = "";
	}
	if (isGlobalConn == false) {
	    if (urlProperty.length() > 0) {
		urlProperty = urlProperty + "&";
	    }
	    urlProperty = urlProperty + "connectionType=local";
	}

	return String.format("jdbc:rye://%s:%d/%s:%s:%s/%s?%s", brokerHost, brokerPort, dbName, dbUser, dbPassword,
			portName, urlProperty);

    }

    static String[] concatStringArr(String[] arr1, String[] arr2)
    {
	if (arr1 == null || arr2 == null) {
	    return (arr1 == null ? arr2 : arr1);
	}

	String[] tmp = new String[arr1.length + arr2.length];
	System.arraycopy(arr1, 0, tmp, 0, arr1.length);
	System.arraycopy(arr2, 0, tmp, arr1.length, arr2.length);

	return tmp;
    }

    static String concatStrArr(String[] arr, String delimiter, boolean strQuote)
    {
	StringBuffer sb = new StringBuffer();
	for (int i = 0; i < arr.length; i++) {
	    if (i != 0) {
		sb.append(delimiter);
	    }

	    if (strQuote == true && wsPattern.matcher(arr[i]).find() == false) {
		strQuote = false;
	    }

	    if (strQuote) {
		sb.append('"');
	    }

	    sb.append(arr[i]);

	    if (strQuote) {
		sb.append('"');
	    }
	}
	return sb.toString();
    }

    void executeRyeCommand(int timeout, NodeAddress host, String... args) throws SQLException
    {
	if (verboseOut != null) {
	    StringBuffer msg = new StringBuffer();
	    msg.append(host.toString());
	    msg.append(":");
	    for (int i = 0; i < args.length; i++) {
		msg.append(' ');
		msg.append(args[i]);
	    }
	    printStatus(true, "%s\n", msg.toString());
	}

	RyeCommand ryeCommand = new RyeCommand(host.toJciConnectionInfo());
	if (timeout != 0) {
	    ryeCommand.setTimeout(timeout);
	}
	ryeCommand.exec(args);

	int exitStatus = ryeCommand.getCommandExitStatus();

	if (exitStatus != 0) {
	    String[] printArgs = new String[args.length];
	    for (int i = 0; i < args.length; i++) {
		if (args[i].indexOf(' ') >= 0) {
		    printArgs[i] = "\"" + args[i] + "\"";
		}
		else {
		    printArgs[i] = args[i];
		}
	    }
	    String command = concatStrArr(printArgs, " ", true);

	    String cmdErrMsg = "";
	    byte[] stderrMsg = ryeCommand.getCommandStderr();
	    if (stderrMsg != null) {
		cmdErrMsg = String.format("(%s)", (new String(stderrMsg)).trim());
	    }
	    throw makeAdminRyeException(null, "%s: rye command fail: %s %s", host.toString(), command, cmdErrMsg);
	}
    }

    void initDB(NodeInfo[] addNode, String[] globalDbname, byte[] brokerAcl, byte[] ryeConf,
		    ArrayList<RyeConfValue> changeRyeConf, NodeInfo[] existingNodeInfoArr, String haGroupId,
		    ArrayList<String> createdbOptionList) throws SQLException
    {
	NodeInfo[] tmpAddNode = new NodeInfo[addNode.length];
	System.arraycopy(addNode, 0, tmpAddNode, 0, addNode.length);

	printStatus(true, "change rye_conf\n");

	while (true) {
	    if (changeHaSettingRyeConf(tmpAddNode, globalDbname, brokerAcl, ryeConf, changeRyeConf,
			    existingNodeInfoArr, haGroupId) == false) {
		break;
	    }
	}

	if (existingNodeInfoArr == null) {
	    for (int i = 0; i < addNode.length; i++) {
		createDatabase(addNode[i].getLocalDbnameArr(globalDbname), addNode[i].getHostArr(), createdbOptionList);
	    }
	}
	else {
	    for (int i = 0; i < addNode.length; i++) {
		makeSlaveDb(addNode[i].getLocalDbnameArr(globalDbname), addNode[i].getHostArr());
	    }
	}

	hbReload(existingNodeInfoArr);

	hbRestart(addNode);

	if (existingNodeInfoArr == null) {
	    for (int i = 0; i < addNode.length; i++) {
		testLocalConnection(addNode[i], globalDbname);
	    }
	}
    }

    void testLocalConnection(NodeInfo nodeInfo, String[] globalDbname)
    {
	NodeAddress[] hosts = nodeInfo.getHostArr();
	String[] localDbname = nodeInfo.getLocalDbnameArr(globalDbname);

	for (int i = 0; i < localDbname.length; i++) {
	    testLocalConnection(hosts[0], localDbname[i], "dba", "", "rw", true, 0);
	}
    }

    void testLocalConnection(NodeAddress host, String dbname, String dbuser, String dbpasswd, String portName,
		    boolean checkMaster, int checkNodeid)
    {
	int retryCount = 300;

	printStatus(true, "%s:%s connection test. check_master=%s, check_nodeid=%d ... ", host.toString(), dbname,
			checkMaster, checkNodeid);

	while (retryCount-- > 0) {
	    try {
		RyeConnection con = makeLocalConnection(host.getIpAddr(), host.getPort(), dbname, dbuser, dbpasswd,
				portName, null);

		int serverHaMode = con.getServerHaMode();
		int serverNodeid = con.getStatusInfoServerNodeid();

		con.close();

		boolean checkRes = true;

		if (checkMaster) {
		    if (serverHaMode != rye.jdbc.jci.Protocol.HA_STATE_MASTER) {
			checkRes = false;
		    }
		}
		if (checkNodeid > 0) {
		    if (serverNodeid != checkNodeid) {
			checkRes = false;
		    }
		}

		if (checkRes) {
		    printStatus(false, "OK\n");
		    return;
		}
	    } catch (SQLException e) {
	    }

	    try {
		Thread.sleep(1000);
	    } catch (Exception e) {
	    }
	}
	printStatus(false, "fail\n");
    }

    void testGlobalConnection(NodeAddress host, String dbname)
    {
	int retryCount = 300;

	printStatus(true, "%s:%s global connection test ... ", host.toString(), dbname);

	try {
	    while (retryCount-- > 0) {
		boolean res = ShardAdmin.ping(host.toJciConnectionInfo(), dbname, 1000);
		if (res == true) {
		    printStatus(false, "OK\n");
		    return;
		}

		try {
		    Thread.sleep(1000);
		} catch (Exception e) {
		}
	    }
	} catch (SQLException e) {
	}
	printStatus(false, "fail\n");
    }

    void checkNodeidInitialized(NodeInfo nodeInfo, String[] globalDbnameArr)
    {
	String[] localDbname = nodeInfo.getLocalDbnameArr(globalDbnameArr);
	NodeAddress[] hosts = nodeInfo.getHostArr();
	int checkNodeid = nodeInfo.getNodeid();

	for (int j = 0; j < localDbname.length; j++) {
	    testLocalConnection(hosts[0], localDbname[j], "dba", "", "rw", true, checkNodeid);
	}
    }

    private static boolean hasNodeid(ShardAdmin shardAdmin, int[] checkNodeidArr)
    {
	if (checkNodeidArr != null) {
	    for (int i = 0; i < checkNodeidArr.length; i++) {
		if (shardAdmin.hasNodeid(checkNodeidArr[i]) == false) {
		    return false;
		}
	    }
	}

	return true;
    }

    void refreshShardInfo(ShardMgmtInfo shardMgmt, String globalDbname, String dbaPasswd, int[] checkNodeidArr)
    {
	try {
	    RyeConnection con = makeConnection(shardMgmt.getIpAddr(), shardMgmt.getPort(), globalDbname, "dba",
			    dbaPasswd, "rw", null);
	    ShardAdmin shardAdmin = con.getShardAdmin();
	    for (int i = 0; i < 100; i++) {
		if (hasNodeid(shardAdmin, checkNodeidArr)) {
		    break;
		}

		shardAdmin.refreshShardInfo();

		try {
		    Thread.sleep(1000);
		} catch (Exception e) {
		}
	    }
	    con.close();
	} catch (SQLException e) {
	}
    }

    void waitDriverSynctime(long lastTime)
    {
	long waitUntil = lastTime + ShardInfoManager.SYNC_INTERVAL_MILLISEC;

	long sleepTime = waitUntil - System.currentTimeMillis() + 500;
	printStatus(true, "sleep %d sec\n", (sleepTime > 0 ? sleepTime / 1000 : 0));

	while (true) {
	    long curTime = System.currentTimeMillis();
	    if (curTime > waitUntil) {
		break;
	    }

	    try {
		Thread.sleep(waitUntil - curTime + 10);
	    } catch (Exception e) {
	    }
	}
    }

    private boolean changeHaSettingRyeConf(NodeInfo[] nodeInfoArr, String[] globalDbname, byte[] brokerAcl,
		    byte[] ryeConf, ArrayList<RyeConfValue> changeRyeConf, NodeInfo[] existingNodeInfoArr,
		    String haGroupId) throws SQLException
    {
	NodeInfo nodeInfo = null;
	ArrayList<NodeInfo> sameHostNodes = new ArrayList<NodeInfo>();
	NodeInfo existingNodeInfo = null;

	for (int i = 0; i < nodeInfoArr.length; i++) {
	    if (nodeInfoArr[i] == null) {
		continue;
	    }
	    if (nodeInfo == null) {
		nodeInfo = nodeInfoArr[i];
	    }
	    else {
		if (nodeInfo.hasSameHosts(nodeInfoArr[i]) == false) {
		    continue;
		}
	    }

	    if (existingNodeInfoArr != null) {
		int idx = NodeInfo.findNode(nodeInfo.getNodeid(), existingNodeInfoArr);
		NodeInfo tmpExistingNodeInfo = existingNodeInfoArr[idx];
		if (existingNodeInfo != null) {
		    if (existingNodeInfo.hasSameHosts(tmpExistingNodeInfo) == false) {
			throw makeAdminRyeException(null, "invalid node configuration");
		    }
		}
		existingNodeInfo = tmpExistingNodeInfo;
	    }

	    sameHostNodes.add(nodeInfoArr[i]);
	    nodeInfoArr[i] = null;
	}

	if (sameHostNodes.size() == 0) {
	    return false;
	}

	String[] localDbname = new String[sameHostNodes.size() * globalDbname.length];
	int idx = 0;
	for (NodeInfo n : sameHostNodes) {
	    for (int i = 0; i < globalDbname.length; i++) {
		localDbname[idx++] = n.getLocalDbname(globalDbname[i]);
	    }
	}

	NodeAddress[] existingHostArr = (existingNodeInfo == null ? null : existingNodeInfo.getHostArr());

	brokerAclReload(nodeInfo, brokerAcl);

	writeRyeConf(nodeInfo, ryeConf);

	changeConfHaDbList(localDbname, nodeInfo.getHostArr());
	changeConfHaNodeList(nodeInfo.getHostArr(), existingHostArr, haGroupId);

	changeRyeConfList(nodeInfo.getHostArr(), changeRyeConf, true);

	return true;
    }

    private void makeSlaveDb(String[] localDbname, NodeAddress[] host) throws SQLException
    {
	for (int i = 0; i < localDbname.length; i++) {
	    for (int j = 0; j < host.length; j++) {
		makeSlaveDb(localDbname[i], host[j]);
	    }
	}
    }

    private void makeSlaveDb(String dbname, NodeAddress host) throws SQLException
    {
	printStatus(true, "make slave database %s@%s\n", dbname, host);

	String bkvFile = String.format("%s_bkv000", dbname);

	executeRyeCommand(-1, host, "rye", "backupdb", dbname, "-m");
	executeRyeCommand(-1, host, "rye", "restoredb", dbname, "-m", "-B", bkvFile);
    }

    private void createDatabase(String[] localDbname, NodeAddress[] host, ArrayList<String> optionList)
		    throws SQLException
    {
	for (int i = 0; i < localDbname.length; i++) {
	    for (int j = 0; j < host.length; j++) {
		createDatabase(localDbname[i], host[j], optionList);
	    }
	}
    }

    private void createDatabase(String dbname, NodeAddress host, ArrayList<String> optionList) throws SQLException
    {
	int optSize = (optionList == null ? 0 : optionList.size());
	String[] createdbCommand = new String[optSize + 3];
	int idx = 0;
	createdbCommand[idx++] = "rye";
	createdbCommand[idx++] = "createdb";
	createdbCommand[idx++] = dbname;
	if (optionList != null) {
	    for (String opt : optionList) {
		createdbCommand[idx++] = opt;
	    }
	}

	executeRyeCommand(-1, host, createdbCommand);
    }

    private void changeConfHaDbList(String[] localDbname, NodeAddress[] hosts) throws SQLException
    {
	String dbnameList = concatStrArr(localDbname, ",", false);

	for (int i = 0; i < hosts.length; i++) {
	    LocalMgmt localMgmt = new LocalMgmt(hosts[i].getIpAddr(), hosts[i].getPort());
	    changeRyeServerConf(localMgmt, RyeConfValue.KEY_HA_DB_LIST, dbnameList);
	}

    }

    void changeConfHaNodeList(NodeAddress[] hosts, NodeAddress[] existingHosts, String haGroupId) throws SQLException
    {
	// String hostList = AdminUtil.concatStrArr(NodeAddress.getHostnameArr(hosts), ":");
	StringBuffer hostList = new StringBuffer();
	String nodeDelimiter = ",";

	if (existingHosts != null) {
	    hostList.append(concatStrArr(NodeAddress.getIpPortArr(existingHosts), nodeDelimiter, false));
	    hostList.append(nodeDelimiter);
	}
	hostList.append(concatStrArr(NodeAddress.getIpPortArr(hosts), nodeDelimiter, false));

	String nodeListParamValue = String.format("%s@%s", haGroupId, hostList.toString());

	for (int i = 0; i < hosts.length; i++) {
	    LocalMgmt localMgmt = new LocalMgmt(hosts[i].toJciConnectionInfo());
	    changeRyeServerConf(localMgmt, RyeConfValue.KEY_HA_NODE_LIST, nodeListParamValue);
	}

	if (existingHosts != null) {
	    for (int i = 0; i < existingHosts.length; i++) {
		LocalMgmt localMgmt = new LocalMgmt(existingHosts[i].toJciConnectionInfo());
		changeRyeServerConf(localMgmt, RyeConfValue.KEY_HA_NODE_LIST, nodeListParamValue);
	    }
	}
    }

    private void hbRestart(NodeInfo[] nodeInfoArr) throws SQLException
    {
	if (nodeInfoArr != null) {
	    NodeAddress[] hostArr = NodeInfo.getDistinctHostArr(nodeInfoArr);
	    for (int i = 0; i < hostArr.length; i++) {
		hbRestart(hostArr[i]);
	    }
	}
    }

    private void hbRestart(NodeAddress host) throws SQLException
    {
	printStatus(true, "%s: hb start \n", host);

	try {
	    executeRyeCommand(0, host, "rye", "service", "restart");
	} catch (SQLException e) {
	}

	// executeRyeCommand(0, host, "rye", "heartbeat", "start");
    }

    void hbStop(NodeInfo[] nodeInfoArr) throws SQLException
    {
	if (nodeInfoArr != null) {
	    NodeAddress[] hostArr = NodeInfo.getDistinctHostArr(nodeInfoArr);
	    for (int i = 0; i < hostArr.length; i++) {
		hbStop(hostArr[i]);
	    }
	}
    }

    private void hbStop(NodeAddress host) throws SQLException
    {
	printStatus(true, "%s: hb stop\n", host);

	executeRyeCommand(0, host, "rye", "heartbeat", "stop");
    }

    void hbReload(NodeInfo[] nodeInfoArr) throws SQLException
    {
	if (nodeInfoArr != null) {
	    NodeAddress[] hostArr = NodeInfo.getDistinctHostArr(nodeInfoArr);
	    for (int i = 0; i < hostArr.length; i++) {
		hbReload(hostArr[i]);
	    }
	}
    }

    private void hbReload(NodeAddress host) throws SQLException
    {
	printStatus(true, "%s: hb reload \n", host);

	executeRyeCommand(0, host, "rye", "heartbeat", "reload");
    }

    private void checkBrokerRunning(NodeAddress[] hostArr, String localDbname, String dbaPasswd) throws Exception
    {
	for (int i = 0; i < hostArr.length; i++) {
	    testLocalConnection(hostArr[i], localDbname, "dba", dbaPasswd, "rw", false, 0);
	}
    }

    void brokerRestartForAddDrop(NodeInfo[] nodeArr, String globalDbname, String dbaPasswd) throws Exception
    {
	NodeAddress[] allHostArr = NodeInfo.getDistinctHostArr(nodeArr);
	for (int i = 0; i < allHostArr.length; i++) {
	    printStatus(true, "%s: broker restart \n", allHostArr[i].getIpAddr());

	    brokerRestart(allHostArr[i]);
	}

	for (int i = 0; i < nodeArr.length; i++) {
	    NodeAddress[] hostArr = nodeArr[i].getHostArr();
	    String localDbname = nodeArr[i].getLocalDbname(globalDbname);
	    checkBrokerRunning(hostArr, localDbname, dbaPasswd);
	}
    }

    void brokerRestart(NodeAddress host) throws SQLException
    {
	/* do not check rye command result. 'rye broker restart' command always returns error */
	try {
	    executeRyeCommand(0, host, "rye", "broker", "restart");
	} catch (SQLException e) {
	}
    }

    private void brokerAclReload(NodeInfo nodeInfo, byte[] acl) throws SQLException
    {
	if (acl == null) {
	    return;
	}

	NodeAddress[] hosts = nodeInfo.getHostArr();
	for (int i = 0; i < hosts.length; i++) {
	    LocalMgmt localMgmt = new LocalMgmt(hosts[i].toJciConnectionInfo());

	    localMgmt.brokerAclReload(acl);
	}
    }

    private void writeRyeConf(NodeInfo nodeInfo, byte[] ryeConf) throws SQLException
    {
	if (ryeConf == null) {
	    return;
	}

	NodeAddress[] hosts = nodeInfo.getHostArr();

	for (int i = 0; i < hosts.length; i++) {
	    LocalMgmt localMgmt = new LocalMgmt(hosts[i].toJciConnectionInfo());

	    localMgmt.writeRyeConf(ryeConf);

	    if (nodeInfo.getNodeid() != FIRST_NODEID) {
		deleteShardMgmtConf(localMgmt);
	    }
	}
    }

    void deleteShardMgmtConf(LocalMgmt localMgmt) throws SQLException
    {
	localMgmt.deleteConf(RyeConfValue.PROC_BROKER, RyeConfValue.SECT_SHARD_MGMT, null);
    }

    private void changeRyeConfList(NodeAddress[] hosts, ArrayList<RyeConfValue> ryeConfList, boolean setHaNodeMyself)
		    throws SQLException
    {
	for (int i = 0; i < hosts.length; i++) {
	    LocalMgmt localMgmt = new LocalMgmt(hosts[i].toJciConnectionInfo());
	    if (ryeConfList != null) {
		for (RyeConfValue confValue : ryeConfList) {
		    changeRyeConf(localMgmt, confValue);
		}
	    }
	    if (setHaNodeMyself) {
		changeRyeServerConf(localMgmt, "ha_node_myself", hosts[i].getIpAddr());
	    }
	}
    }

    private void changeRyeServerConf(LocalMgmt localMgmt, String key, String value) throws SQLException
    {
	changeRyeConf(localMgmt, new RyeServerConfValue(key, value));
    }

    void changeRyeConf(LocalMgmt localMgmt, RyeConfValue ryeConfValue) throws SQLException
    {
	localMgmt.updateConf(ryeConfValue);
    }

    void checkLocalMgmtOccupied(NodeInfo[] addNode) throws SQLException
    {
	for (int i = 0; i < addNode.length; i++) {
	    checkLocalMgmtOccupied(addNode[i].getHostArr());
	}
    }

    private void checkLocalMgmtOccupied(NodeAddress[] hosts) throws SQLException
    {
	for (int i = 0; i < hosts.length; i++) {
	    printStatus(true, "check broker port %s \n", hosts[i].toString());

	    if (isOccupiedLocalMgmt(hosts[i]) == true) {
		throw makeAdminRyeException(null, "node '%s' is in use", hosts[i].getOrgHostname());
	    }
	}
    }

    private boolean isOccupiedLocalMgmt(NodeAddress host) throws SQLException
    {
	LocalMgmt localMgmt = new LocalMgmt(host.toJciConnectionInfo());
	int numShardVersionInfo = -1;
	int retryCount = 20;

	while (retryCount-- > 0) {
	    numShardVersionInfo = localMgmt.numShardVersionInfo();
	    if (numShardVersionInfo > 0) {
		return true;
	    }
	    else if (numShardVersionInfo == 0) {
		return false;
	    }
	    else {
		if (retryCount > 0) {
		    try {
			Thread.sleep(5 * 1000);
		    } catch (Exception e) {
		    }
		}
	    }
	}
	return true;
    }

    void verifyPassword(NodeAddress shardMgmtHost, String[] globalDbnameArr, String[] dbaPasswordArr)
		    throws SQLException
    {
	printStatus(true, "verify password\n");

	// Console console = System.console();

	for (int i = 0; i < globalDbnameArr.length; i++) {
	    String localDbname = NodeInfo.getLocalDbname(globalDbnameArr[i], FIRST_NODEID);

	    RyeConnection con = makeLocalConnection(shardMgmtHost.getIpAddr(), shardMgmtHost.getPort(), localDbname,
			    "dba", dbaPasswordArr[i], "rw", null);
	    con.close();

	}
    }

    static RyeException makeAdminRyeException(Throwable cause, String format, Object... args)
    {
	return new RyeException(String.format(format, args), cause);
    }

    String getHaGroupId(LocalMgmt localMgmt) throws Exception
    {
	String haNodeList;
	haNodeList = localMgmt.getConf(RyeConfValue.PROC_SERVER, RyeConfValue.SECT_COMMON,
			RyeConfValue.KEY_HA_NODE_LIST);
	if (haNodeList == null) {
	    throw makeAdminRyeException(null, "conf '%s' not found", RyeConfValue.KEY_HA_NODE_LIST);
	}

	int idx = haNodeList.indexOf('@');
	if (idx <= 0) {
	    throw makeAdminRyeException(null, "invalid '%s' conf value", RyeConfValue.KEY_HA_NODE_LIST);
	}
	return haNodeList.substring(0, idx);
    }

    String makeRandomeId(int len)
    {
	Random r = new Random();
	byte[] b = new byte[len];
	for (int i = 0; i < len; i++) {
	    b[i] = (byte) ('a' + r.nextInt(26));
	}
	return new String(b);
    }
}
