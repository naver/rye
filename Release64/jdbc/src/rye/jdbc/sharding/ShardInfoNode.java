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

package rye.jdbc.sharding;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Set;

import rye.jdbc.driver.ConnectionProperties;
import rye.jdbc.driver.RyeErrorCode;
import rye.jdbc.driver.RyeException;
import rye.jdbc.jci.InputBuffer;
import rye.jdbc.jci.JciConnection;
import rye.jdbc.jci.JciConnectionInfo;
import rye.jdbc.jci.JciException;
import rye.jdbc.log.BasicLogger;

class ShardInfoNode
{
    private class BaseNodeInfoIdComparator implements Comparator<ShardNodeInstance>
    {
	public int compare(ShardNodeInstance n1, ShardNodeInstance n2)
	{
	    return (n1.getNodeid() - n2.getNodeid());
	}
    }

    private class ShardConnectionPool
    {
	private final ShardNodeId nodeId;
	private final ShardNodeInstance[] baseNodeInfoArr;
	private final ArrayList<JciConnectionInfo> conInfoList;
	private String strHostInfo;
	private final String dbname;
	private final ShardDataSource[] conPoolArray;
	private int numDsInfo;
	private boolean isValid;

	private ShardConnectionPool(ShardNodeId nodeid, ShardNodeInstance[] baseInfoArr, int startIdx, int endIdx)
			throws RyeException
	{
	    conPoolArray = new ShardDataSource[ShardInfoManager.MAX_SERVICE_BROKER];
	    numDsInfo = 0;

	    this.nodeId = nodeid;

	    baseNodeInfoArr = new ShardNodeInstance[endIdx - startIdx];
	    System.arraycopy(baseInfoArr, startIdx, baseNodeInfoArr, 0, endIdx - startIdx);

	    conInfoList = new ArrayList<JciConnectionInfo>();
	    for (int i = 0; i < baseNodeInfoArr.length; i++) {
		conInfoList.add(new JciConnectionInfo(baseNodeInfoArr[i].getHost(), baseNodeInfoArr[i].getPort(), null));
	    }

	    this.dbname = baseNodeInfoArr[0].getDbname();
	    this.isValid = true;
	}

	JciShardConnection getConnection(String portName, ConnectionProperties conProperties, String user,
			String passwd, ShardNodeId nodeid, boolean autoCommit, JciConnection jciCon)
			throws RyeException
	{
	    int idx = findConnectionPool(portName);

	    if (idx < 0) {
		idx = addConnectionPool(portName, jciCon);
	    }

	    if (idx < 0) {
		if (isValid) {
		    throw RyeException.createRyeException(jciCon, RyeErrorCode.ER_SHARD_NODE_CONNECTION_INVALID, null);
		}
		else {
		    throw RyeException.createRyeException(jciCon, RyeErrorCode.ER_SHARD_NODE_INFO_OBSOLETE, null);
		}
	    }

	    return conPoolArray[idx].getConnection(user, passwd, conProperties, nodeid, autoCommit);
	}

	private int addConnectionPool(String portName, JciConnection jciCon) throws RyeException
	{
	    int newIndex = -1;

	    synchronized (conPoolArray) {
		if (isValid) {
		    newIndex = findConnectionPool(portName);
		    if (newIndex < 0) {
			if (numDsInfo >= conPoolArray.length) {
			    throw RyeException.createRyeException(jciCon,
					    RyeErrorCode.ER_SHARD_NODE_MAX_SERVICE_PORT_EXCEED, null);
			}

			ArrayList<JciConnectionInfo> newConInfoList = JciConnectionInfo.copyList(conInfoList, portName);
			newIndex = numDsInfo;
			conPoolArray[newIndex] = new ShardDataSource(newConInfoList, dbname, portName);
			numDsInfo++;
		    }
		}
	    }

	    return newIndex;
	}

	private int findConnectionPool(String portName)
	{
	    if (isValid == false) {
		return -1;
	    }

	    for (int i = 0; i < numDsInfo; i++) {
		if (conPoolArray[i].getServicePortName().equals(portName)) {
		    return i;
		}
	    }
	    return -1;
	}

	void setInvalid(ArrayList<ShardDataSource> obsoleteDsList)
	{
	    synchronized (conPoolArray) {
		isValid = false;

		for (int i = 0; i < numDsInfo; i++) {
		    obsoleteDsList.add(conPoolArray[i]);
		}
	    }
	}

	public String toString()
	{
	    if (strHostInfo == null) {
		StringBuffer sb = null;
		for (JciConnectionInfo conInfo : conInfoList) {
		    if (sb == null) {
			sb = new StringBuffer();
		    }
		    else {
			sb.append(",");
		    }

		    sb.append(conInfo.getHostname());
		    sb.append(":");
		    sb.append(conInfo.getPort());
		}
		strHostInfo = sb.toString();
	    }

	    return (String.format("%d %s %s", nodeId.getNodeId(), strHostInfo, dbname));
	}

	String getPoolInfo()
	{
	    StringBuffer sb = new StringBuffer();
	    for (int i = 0; i < this.numDsInfo; i++) {
		sb.append(String.format("	%s: total=%d, available=%d\n", this.conPoolArray[i].getServicePortName(),
				this.conPoolArray[i].getTotalConns(), this.conPoolArray[i].getAvailableConns()));
	    }
	    return sb.toString();
	}
    }

    private final long version;
    private final HashMap<ShardNodeId, ShardConnectionPool> nodeMap;
    private ShardNodeId[] allShardNodeid = null;
    private ShardNodeId[] allShardNodeidAndMeta = null;
    private final ShardInfo shardInfo;

    ShardInfoNode(byte[] netStream, ShardInfo shardInfo) throws JciException, RyeException
    {
	// unpacking message
	InputBuffer in = new InputBuffer(netStream);

	long version = in.readLong();
	int count = in.readInt();

	if (count <= 0) {
	    throw new JciException(RyeErrorCode.ER_SHARD_INFO_RECV_FAIL);
	}

	ShardNodeInstance[] nodeArray = new ShardNodeInstance[count];

	for (int i = 0; i < count; i++) {
	    short nodeid = in.readShort();

	    int strSize = in.readInt();
	    String dbname = in.readString(strSize);

	    strSize = in.readInt();
	    String hostname = in.readString(strSize);

	    int port = in.readInt();

	    nodeArray[i] = new ShardNodeInstance(nodeid, dbname, hostname, port);
	}

	this.version = version;

	this.nodeMap = new HashMap<ShardNodeId, ShardConnectionPool>();

	// grouping for each nodeid
	Arrays.sort(nodeArray, new BaseNodeInfoIdComparator());
	int startIdx = 0, endIdx;
	while (startIdx < nodeArray.length) {
	    for (endIdx = startIdx + 1; endIdx < nodeArray.length; endIdx++) {
		if (nodeArray[endIdx].getNodeid() != nodeArray[endIdx - 1].getNodeid()) {
		    break;
		}
	    }
	    ShardNodeId nodeid = ShardNodeId.valueOf(nodeArray[startIdx].getNodeid());
	    nodeMap.put(nodeid, new ShardConnectionPool(nodeid, nodeArray, startIdx, endIdx));
	    startIdx = endIdx;
	}

	this.shardInfo = shardInfo;

	dump();
    }

    JciShardConnection getShardConnection(ShardNodeId nodeid, String portName, ConnectionProperties conProperties,
		    String user, String passwd, boolean autoCommit, JciConnection jciCon) throws RyeException
    {
	if (nodeid == null || nodeid.getNodeId() < 0) {
	    throw RyeException.createRyeException(jciCon, RyeErrorCode.ER_SHARD_NODE_CONNECTION_INVALID, null);
	}

	ShardConnectionPool nodeConPool = nodeMap.get(nodeid);
	if (nodeConPool == null) {
	    throw RyeException.createRyeException(jciCon, RyeErrorCode.ER_SHARD_NODE_CONNECTION_INVALID, null);
	}

	return nodeConPool.getConnection(portName, conProperties, user, passwd, nodeid, autoCommit, jciCon);
    }

    ShardNodeId[] getAllNodeid(boolean includeMetadb)
    {
	makeAllShardNodeid();

	if (includeMetadb) {
	    return allShardNodeidAndMeta;
	}
	else {
	    return allShardNodeid;
	}
    }

    private void makeAllShardNodeid()
    {
	synchronized (nodeMap) {
	    if (allShardNodeidAndMeta == null) {
		// make node id array of all shard ids and metadb
		Set<ShardNodeId> keySet = nodeMap.keySet();
		allShardNodeidAndMeta = new ShardNodeId[keySet.size()];
		keySet.toArray(allShardNodeidAndMeta);
		Arrays.sort(allShardNodeidAndMeta);

		// make node id array of all shard ids
		for (int i = 0; i < allShardNodeidAndMeta.length; i++) {
		    if (allShardNodeidAndMeta[i].getNodeId() > ShardInfo.metadbNodeId.getNodeId()) {
			allShardNodeid = new ShardNodeId[allShardNodeidAndMeta.length - i];
			System.arraycopy(allShardNodeidAndMeta, i, allShardNodeid, 0, allShardNodeidAndMeta.length - i);
			break;
		    }
		}

		if (allShardNodeid == null) {
		    allShardNodeidAndMeta = null;
		}
	    }
	}
    }

    void setInvalid(ArrayList<ShardDataSource> outDsList)
    {
	Collection<ShardConnectionPool> allNodeConPools = nodeMap.values();
	for (ShardConnectionPool nodeConPool : allNodeConPools) {
	    nodeConPool.setInvalid(outDsList);
	}
	// nodeMap.clear();
    }

    long getVersion()
    {
	return version;
    }

    void dump()
    {
	BasicLogger logger = shardInfo.getShardInfoManager().logger;

	if (logger != null) {
	    String msg = dumpString(2);
	    synchronized (logger) {
		logger.logInfo(msg);
	    }
	}
    }

    String dumpString(int level)
    {
	StringBuffer sb = new StringBuffer();

	if (level > 1) {
	    sb.append(String.format("NODE version = %d\n", version));
	}

	Collection<ShardConnectionPool> allNodeConPools = nodeMap.values();
	for (ShardConnectionPool nodeConPool : allNodeConPools) {
	    sb.append(nodeConPool.toString());
	    sb.append("\n");
	    if (level > 1) {
		sb.append(nodeConPool.getPoolInfo());
		sb.append("\n");
	    }
	}

	return sb.toString();
    }

    ShardNodeInstance[] getShardNodeInstance(int nodeid)
    {
	ShardConnectionPool conPool = nodeMap.get(ShardNodeId.valueOf((short) nodeid));
	if (conPool == null) {
	    return null;
	}

	ShardNodeInstance[] instanceArr = new ShardNodeInstance[conPool.baseNodeInfoArr.length];
	System.arraycopy(conPool.baseNodeInfoArr, 0, instanceArr, 0, conPool.baseNodeInfoArr.length);
	return instanceArr;
    }

    ShardNodeInstance[] getShardNodeInstance()
    {

	int numInstance = 0;

	Collection<ShardConnectionPool> allNodeConPools = nodeMap.values();
	for (ShardConnectionPool nodeConPool : allNodeConPools) {
	    numInstance += nodeConPool.baseNodeInfoArr.length;
	}

	ShardNodeInstance[] instanceArr = new ShardNodeInstance[numInstance];
	int idx = 0;
	for (ShardConnectionPool nodeConPool : allNodeConPools) {
	    System.arraycopy(nodeConPool.baseNodeInfoArr, 0, instanceArr, idx, nodeConPool.baseNodeInfoArr.length);
	    idx += nodeConPool.baseNodeInfoArr.length;
	}

	return instanceArr;
    }

    boolean hasNodeid(int nodeid)
    {
	ShardNodeId[] allNodeid = getAllNodeid(false);

	for (int i = 0; i < allNodeid.length; i++) {
	    if (allNodeid[i].getNodeId() == nodeid) {
		return true;
	    }
	}

	return false;
    }
}
