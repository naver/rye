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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;

import rye.jdbc.jci.JciConnectionInfo;

class NodeAddress
{
    private final String orgHostname;
    private final String hostname;
    private final int port;
    private final InetAddress inetAddr;
    private final String ipaddr;

    NodeAddress(String argHostname, int port) throws SQLException
    {
	argHostname = argHostname.trim().toLowerCase();

	String[] tmp = argHostname.split(":");
	if (tmp.length == 1) {
	}
	else if (tmp.length == 2) {
	    port = Integer.parseInt(tmp[1]);
	}
	else {
	    throw ShardCommand.makeAdminRyeException(null, "invalid host string: %s", argHostname);
	}
	argHostname = tmp[0];

	this.orgHostname = argHostname;
	this.port = port;

	try {
	    if (argHostname.equalsIgnoreCase("localhost") || argHostname.equals("127.0.0.1")) {
		inetAddr = InetAddress.getLocalHost();
	    }
	    else {
		inetAddr = InetAddress.getByName(argHostname);
	    }
	} catch (UnknownHostException e) {
	    throw ShardCommand.makeAdminRyeException(e, "unknown host '%s'", argHostname);
	}

	this.hostname = inetAddr.getHostName();
	this.ipaddr = inetAddr.getHostAddress();
    }

    JciConnectionInfo toJciConnectionInfo() throws SQLException
    {
	return new JciConnectionInfo(this.getIpAddr(), this.getPort(), "rw");
    }

    public String toString()
    {
	return String.format("%s:%d", this.getIpAddr(), this.getPort());
    }

    String getIpAddr()
    {
	return this.ipaddr;
    }

    static String[] getHostnameArr(NodeAddress[] nodeAddr)
    {
	String[] hosts = new String[nodeAddr.length];
	for (int i = 0; i < nodeAddr.length; i++) {
	    hosts[i] = nodeAddr[i].hostname;
	}
	return hosts;
    }

    static String[] getIpPortArr(NodeAddress[] nodeAddr)
    {
	String[] ipaddr = new String[nodeAddr.length];
	for (int i = 0; i < nodeAddr.length; i++) {
	    ipaddr[i] = String.format("%s:%d", nodeAddr[i].getIpAddr(), nodeAddr[i].getPort());
	}
	return ipaddr;
    }

    int getPort()
    {
	return this.port;
    }

    String getOrgHostname()
    {
	return this.orgHostname;
    }

    public boolean equals(Object o)
    {
	if (o != null && o instanceof NodeAddress) {
	    NodeAddress n = (NodeAddress) o;
	    if (this.inetAddr.equals(n.inetAddr) && this.port == n.port) {
		return true;
	    }
	}

	return false;
    }

    boolean equalHost(NodeAddress other)
    {
	return this.inetAddr.equals(other.inetAddr);
    }

    static void checkDuplicateNode(NodeAddress[] nodeArr) throws SQLException
    {
	for (int i = 0; i < nodeArr.length; i++) {
	    for (int j = i + 1; j < nodeArr.length; j++) {
		if (nodeArr[i].equals(nodeArr[j])) {
		    throw ShardCommand.makeAdminRyeException(null, "duplicate node info:%s, %s",
				    nodeArr[i].orgHostname, nodeArr[j].orgHostname);
		}
	    }
	}
    }
}

class NodeInfo
{
    private final int nodeid;
    private final NodeAddress[] hostArr;

    NodeInfo(int nodeid, NodeAddress[] hostArr1, NodeAddress[] hostArr2) throws Exception
    {
	if (nodeid <= 0) {
	    throw ShardCommand.makeAdminRyeException(null, "invalid nodeid '%d'", nodeid);
	}

	this.nodeid = nodeid;

	if (hostArr2 == null) {
	    this.hostArr = hostArr1;
	}
	else {
	    this.hostArr = new NodeAddress[hostArr1.length + hostArr2.length];
	    System.arraycopy(hostArr1, 0, hostArr, 0, hostArr1.length);
	    System.arraycopy(hostArr2, 0, hostArr, hostArr1.length, hostArr2.length);
	}

	NodeAddress.checkDuplicateNode(hostArr);
    }

    int getNodeid()
    {
	return this.nodeid;
    }

    NodeAddress[] getHostArr()
    {
	return this.hostArr;
    }

    static NodeInfo excludeDropNode(NodeInfo existingNode, NodeInfo dropNode) throws Exception
    {
	NodeAddress[] existingHostArr = existingNode.getHostArr();
	NodeAddress[] dropHostArr = dropNode.getHostArr();
	int dropCount = 0;

	for (int i = 0; i < existingHostArr.length; i++) {
	    for (int j = 0; j < dropHostArr.length; j++) {
		if (existingHostArr[i] != null) {
		    if (existingHostArr[i].equalHost(dropHostArr[j])) {
			existingHostArr[i] = null;
			dropCount++;
		    }
		}
	    }
	}

	if (dropCount == 0) {
	    return null;
	}

	NodeAddress[] tmpArr = new NodeAddress[existingHostArr.length - dropCount];
	int idx = 0;
	for (int i = 0; i < existingHostArr.length; i++) {
	    if (existingHostArr[i] != null) {
		tmpArr[idx++] = existingHostArr[i];
	    }
	}

	return new NodeInfo(existingNode.getNodeid(), tmpArr, null);
    }

    static NodeAddress[] getDistinctHostArr(NodeInfo[] nodeArr)
    {
	ArrayList<NodeAddress> hostList = new ArrayList<NodeAddress>();

	for (int i = 0; i < nodeArr.length; i++) {
	    NodeAddress[] tmpArr = nodeArr[i].getHostArr();
	    for (int j = 0; j < tmpArr.length; j++) {
		if (hostList.contains(tmpArr[j]) == false) {
		    hostList.add(tmpArr[j]);
		}
	    }
	}

	NodeAddress[] arr = new NodeAddress[hostList.size()];
	hostList.toArray(arr);
	return arr;
    }

    String getLocalDbname(String globalDbname)
    {
	return getLocalDbname(globalDbname, nodeid);
    }

    static String getLocalDbname(String globalDbname, int nodeid)
    {
	return globalDbname + nodeid;
    }

    boolean hasSameHosts(NodeInfo other) throws SQLException
    {
	int sameHostCount = 0;

	for (int i = 0; i < hostArr.length; i++) {
	    boolean hasSameHost = false;
	    for (int j = 0; j < other.hostArr.length; j++) {
		if (hostArr[i].equals(other.hostArr[j])) {
		    hasSameHost = true;
		    break;
		}
	    }
	    if (hasSameHost) {
		sameHostCount++;
	    }
	}

	if (sameHostCount == 0) {
	    return false;
	}
	else {
	    if (hostArr.length == other.hostArr.length && hostArr.length == sameHostCount) {
		return true;
	    }
	    else {
		throw ShardCommand.makeAdminRyeException(null, "invalid node configuration");
	    }
	}
    }

    String[] getLocalDbnameArr(String[] globalDbname)
    {
	String[] localDbname = new String[globalDbname.length];
	for (int i = 0; i < globalDbname.length; i++) {
	    localDbname[i] = this.getLocalDbname(globalDbname[i]);
	}
	return localDbname;
    }

    String[] toNodeAddArg(String globalDbname)
    {
	StringBuffer sb = new StringBuffer();
	String[] args = new String[hostArr.length];
	String localDbname = getLocalDbname(globalDbname);

	for (int i = 0; i < args.length; i++) {
	    sb.setLength(0);
	    sb.append(nodeid);
	    sb.append(':');
	    sb.append(localDbname);
	    sb.append(':');
	    sb.append(hostArr[i].getIpAddr());
	    sb.append(':');
	    sb.append(hostArr[i].getPort());

	    args[i] = sb.toString();
	}

	return args;
    }

    static String[] toNodeAddArg(NodeInfo[] nodeInfoArr, String globalDbname, boolean registerFirstNodeOnly)
    {
	if (nodeInfoArr == null) {
	    return null;
	}

	String[] args = null;

	for (int i = 0; i < nodeInfoArr.length; i++) {
	    if (registerFirstNodeOnly && nodeInfoArr[i].getNodeid() != ShardCommand.FIRST_NODEID) {
		continue;
	    }

	    String[] tmp = nodeInfoArr[i].toNodeAddArg(globalDbname);
	    args = ShardCommand.concatStringArr(args, tmp);
	}

	return args;
    }

    static NodeInfo makeNodeInfo(int nodeid, String[] hostInfo, int localMgmtPort) throws Exception
    {
	if (nodeid <= 0 || hostInfo == null || hostInfo.length == 0) {
	    return null;
	}

	NodeAddress[] hostArr = new NodeAddress[hostInfo.length];

	for (int i = 0; i < hostInfo.length; i++) {
	    hostArr[i] = new NodeAddress(hostInfo[i], localMgmtPort);
	}

	return new NodeInfo(nodeid, hostArr, null);
    }

    static NodeInfo[] makeNodeInfoArr(String[] nodeHostInfo, int localMgmtPort) throws Exception
    {
	if (nodeHostInfo == null || nodeHostInfo.length == 0) {
	    return null;
	}

	HashMap<Integer, ArrayList<NodeAddress>> nodeMap = new HashMap<Integer, ArrayList<NodeAddress>>();

	for (int i = 0; i < nodeHostInfo.length; i++) {
	    int idx = nodeHostInfo[i].indexOf(':');
	    if (idx < 0) {
		throw ShardCommand.makeAdminRyeException(null, "invalid node info '%s'", nodeHostInfo[i]);
	    }

	    int nodeid = Integer.parseInt(nodeHostInfo[i].substring(0, idx));
	    NodeAddress nodeAddr = new NodeAddress(nodeHostInfo[i].substring(idx + 1), localMgmtPort);

	    ArrayList<NodeAddress> list = nodeMap.get(nodeid);
	    if (list == null) {
		list = new ArrayList<NodeAddress>();
		nodeMap.put(nodeid, list);
	    }
	    list.add(nodeAddr);
	}

	Set<Integer> nodeidSet = nodeMap.keySet();
	NodeInfo[] nodeInfoArr = new NodeInfo[nodeidSet.size()];
	int idx = 0;

	for (Integer nodeid : nodeidSet) {
	    ArrayList<NodeAddress> list = nodeMap.get(nodeid);
	    NodeAddress[] nodeArr = new NodeAddress[list.size()];
	    list.toArray(nodeArr);

	    nodeInfoArr[idx++] = new NodeInfo(nodeid.intValue(), nodeArr, null);
	}

	return nodeInfoArr;
    }

    static int findNode(int nodeid, NodeInfo[] nodeInfoArr)
    {
	for (int i = 0; i < nodeInfoArr.length; i++) {
	    if (nodeInfoArr[i].nodeid == nodeid) {
		return i;
	    }
	}
	return -1;
    }

    static int[] getAllNodeidArr(NodeInfo[] nodeInfoArr)
    {
	int[] nodeidArr = new int[nodeInfoArr.length];
	for (int i = 0; i < nodeInfoArr.length; i++) {
	    nodeidArr[i] = nodeInfoArr[i].getNodeid();
	}
	return nodeidArr;
    }
}

class ShardMgmtInfo
{
    private final String globalDbname;
    private final NodeAddress nodeAddr;

    ShardMgmtInfo(String globalDbname, NodeAddress nodeAddr)
    {
	this.globalDbname = globalDbname;
	this.nodeAddr = nodeAddr;
    }

    int getPort()
    {
	return this.nodeAddr.getPort();
    }

    String getIpAddr()
    {
	return this.nodeAddr.getIpAddr();
    }

    String getOrgHostname()
    {
	return this.nodeAddr.getOrgHostname();
    }

    static ShardMgmtInfo find(ShardMgmtInfo[] infoArr, String dbname) throws SQLException
    {
	for (int i = 0; i < infoArr.length; i++) {
	    if (infoArr[i].globalDbname.equals(dbname)) {
		return infoArr[i];
	    }
	}
	throw ShardCommand.makeAdminRyeException(null, "shard_mgmt for '%s' not found", dbname);
    }
}
