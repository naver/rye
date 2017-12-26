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

import java.nio.charset.Charset;
import java.util.ArrayList;

import rye.jdbc.driver.ConnectionProperties;
import rye.jdbc.driver.RyeDriver;
import rye.jdbc.driver.RyeErrorCode;
import rye.jdbc.driver.RyeException;
import rye.jdbc.jci.BrokerHandler;
import rye.jdbc.jci.BrokerResponse;
import rye.jdbc.jci.InputBuffer;
import rye.jdbc.jci.JciConnection;
import rye.jdbc.jci.JciConnectionInfo;
import rye.jdbc.jci.JciException;
import rye.jdbc.log.BasicLogger;

public class ShardInfo
{
    static final ShardNodeId metadbNodeId = ShardNodeId.valueOf((short) 0);

    private boolean ready = false;
    private ShardMgmtConnectionInfo mgmtConInfo;

    private ShardInfoNode nodeInfo;
    private ShardInfoGroupid groupidInfo;
    private int waiterCount = 0;
    private long lastSyncTime;
    private long lastShardInfoUpdateTime;
    private long expectShardInfoVersion;
    private long svrShardInfoCreatedAt;
    private final ShardInfoManager shardInfoManager;

    ShardInfo(ShardMgmtConnectionInfo conInfo, ShardInfoManager shardInfoManager)
    {
	this.mgmtConInfo = conInfo;
	nodeInfo = null;
	groupidInfo = null;
	lastSyncTime = 0;
	lastShardInfoUpdateTime = 0;
	svrShardInfoCreatedAt = 0;
	this.shardInfoManager = shardInfoManager;
    }

    ShardInfoManager getShardInfoManager()
    {
	return this.shardInfoManager;
    }

    boolean isReady()
    {
	return ready;
    }

    synchronized boolean needSync(ShardMgmtConnectionInfo newConInfo)
    {
	if (this.ready == false) {
	    return true;
	}

	if (System.currentTimeMillis() - this.lastShardInfoUpdateTime >= 60000
			&& this.mgmtConInfo.hasSameHost(newConInfo) == false) {
	    /* maybe shard mgmt host changed. reset */
	    this.mgmtConInfo = newConInfo;
	    return true;
	}

	return false;
    }

    void checkServerShardInfoVersion(long serverShardInfoVersion)
    {
	long driverShardInfoVersion = getShardInfoVersion();

	if (serverShardInfoVersion > driverShardInfoVersion) {
	    if (serverShardInfoVersion > expectShardInfoVersion) {
		boolean doSync = false;

		synchronized (this) {
		    if (serverShardInfoVersion > expectShardInfoVersion) {
			expectShardInfoVersion = serverShardInfoVersion;
			doSync = true;
		    }
		}

		if (doSync) {
		    this.refreshShardInfo(0, 0);
		}
	    }

	}
    }

    long getShardInfoVersion()
    {
	ShardInfoGroupid curGroupidInfo = this.groupidInfo;
	ShardInfoNode curNodeInfo = this.nodeInfo;

	long groupidVersion = (curGroupidInfo == null ? 0 : curGroupidInfo.getVersion());
	long nodeVersion = (curNodeInfo == null ? 0 : curNodeInfo.getVersion());

	return (groupidVersion > nodeVersion ? groupidVersion : nodeVersion);
    }

    private ShardInfoGroupid getGroupidInfo(JciConnection jciCon) throws RyeException
    {
	ShardInfoGroupid curGroupidInfo = this.groupidInfo;
	if (curGroupidInfo == null) {
	    throw RyeException.createRyeException(jciCon, RyeErrorCode.ER_SHARD_NODE_CONNECTION_INVALID, null);
	}

	return curGroupidInfo;
    }

    ShardKey makeShardKey(String shardKey, Charset conCharset, JciConnection jciCon) throws RyeException
    {
	return getGroupidInfo(jciCon).makeShardKey(shardKey, conCharset, jciCon);
    }

    ShardNodeId getRandomNodeid(JciConnection jciCon) throws RyeException
    {
	return getGroupidInfo(jciCon).getRandomNodeid();
    }

    ShardNodeId getMetadbNodeid() throws RyeException
    {
	return metadbNodeId;
    }

    ShardNodeId[] getDistinctNodeIdArray(ShardKey[] shardKeyArr, JciConnection jciCon) throws RyeException
    {
	return getGroupidInfo(jciCon).getDistinctNodeIdArray(shardKeyArr);
    }

    ShardKey[] getDistinctShardKeyArray(String[] strShardKeyArr, Charset charset, JciConnection jciCon)
		    throws RyeException
    {
	return getGroupidInfo(jciCon).getDistinctShardKeyArray(strShardKeyArr, charset, jciCon);
    }

    private ShardInfoNode getNodeInfo(JciConnection jciCon) throws RyeException
    {
	ShardInfoNode curNodeInfo = this.nodeInfo;
	if (curNodeInfo == null) {
	    throw RyeException.createRyeException(jciCon, RyeErrorCode.ER_SHARD_NODE_CONNECTION_INVALID, null);
	}

	return curNodeInfo;
    }

    ShardNodeId[] getAllNodeidArray(boolean includeMetadb, JciConnection jciCon) throws RyeException
    {
	return getNodeInfo(jciCon).getAllNodeid(includeMetadb);
    }

    JciShardConnection getOneConnection(ShardNodeId nodeid, String portName, ConnectionProperties conProperties,
		    String user, String passwd, boolean autoCommit, JciConnection jciCon) throws RyeException
    {
	for (int retryCount = 2; retryCount > 0; retryCount--) {
	    ShardInfoNode curNodeInfo = getNodeInfo(jciCon);

	    try {
		return curNodeInfo
				.getShardConnection(nodeid, portName, conProperties, user, passwd, autoCommit, jciCon);
	    } catch (RyeException e) {
		if (e.getErrorCode() == RyeErrorCode.ER_SHARD_NODE_INFO_OBSOLETE) {
		    // if curNodeInfo is obsolete, retry
		}
		else {
		    throw e;
		}
	    }
	}

	throw RyeException.createRyeException(jciCon, RyeErrorCode.ER_SHARD_NODE_CONNECTION_INVALID, null);
    }

    JciShardConnection[] getNConnectionArray(ShardNodeId[] nodeids, String portName,
		    ConnectionProperties conProperties, String user, String passwd, boolean autoCommit,
		    JciConnection jciCon) throws RyeException
    {
	for (int retryCount = 2; retryCount > 0; retryCount--) {
	    ShardInfoNode curNodeInfo = getNodeInfo(jciCon);

	    if (nodeids == null) {
		throw RyeException.createRyeException(jciCon, RyeErrorCode.ER_SHARD_NODE_CONNECTION_INVALID, null);
	    }

	    JciShardConnection[] conArray = new JciShardConnection[nodeids.length];

	    for (int i = 0; i < nodeids.length; i++) {
		try {
		    conArray[i] = curNodeInfo.getShardConnection(nodeids[i], portName, conProperties, user, passwd,
				    autoCommit, jciCon);
		} catch (RyeException e) {
		    // if node info is obsolete while creating connection,
		    // all connections should be remade with new node info
		    if (e.getErrorCode() == RyeErrorCode.ER_SHARD_NODE_INFO_OBSOLETE) {
			closeConnection(conArray, 0, i);
			conArray = null;
			break;
		    }
		    else {
			throw e;
		    }
		}
	    }

	    if (conArray != null) {
		return conArray;
	    }
	}

	throw RyeException.createRyeException(jciCon, RyeErrorCode.ER_SHARD_NODE_CONNECTION_INVALID, null);
    }

    private void closeConnection(JciShardConnection[] con, int startIndex, int endIndex)
    {
	for (int i = startIndex; i < endIndex; i++) {
	    try {
		if (con[i] != null) {
		    con[i].close();
		}
	    } catch (Exception e) {
	    }
	}
    }

    void clear(ArrayList<ShardDataSource> outObsoleteDsList)
    {
	ShardInfoNode oldNodeInfo = null;

	synchronized (this) {
	    oldNodeInfo = this.nodeInfo;
	    this.nodeInfo = null;
	}

	if (oldNodeInfo != null && outObsoleteDsList != null) {
	    synchronized (outObsoleteDsList) {
		oldNodeInfo.setInvalid(outObsoleteDsList);
	    }
	}

    }

    void sync(ArrayList<ShardDataSource> outObsoleteDsList)
    {
	JciConnectionInfo[] conInfoArr = mgmtConInfo.getConnInfoArray();
	BrokerResponse brRes = null;

	for (int i = 0; i < conInfoArr.length; i++) {
	    try {
		brRes = BrokerHandler.syncShardInfo(conInfoArr[i], mgmtConInfo.getDbname(), (nodeInfo == null ? 0
				: nodeInfo.getVersion()), (groupidInfo == null ? 0 : groupidInfo.getVersion()),
				svrShardInfoCreatedAt, ShardInfoManager.SYNC_WAIT_MILLISEC);
		break;
	    } catch (JciException e) {
	    }
	}

	ShardInfoNode newNodeInfo = null;
	ShardInfoNode oldNodeInfo = null;
	ShardInfoGroupid newGroupidInfo = null;
	boolean resetAll = false;

	if (brRes != null) {
	    byte[] netStream;

	    netStream = brRes.getAdditionalMsg(0);
	    resetAll = unpackShardInfoHdr(netStream);

	    netStream = brRes.getAdditionalMsg(1);
	    if (netStream != null) {
		try {
		    newNodeInfo = new ShardInfoNode(netStream, this);
		} catch (Exception e) {
		    newNodeInfo = null;
		}
	    }

	    short[] oldGroupidMapping = null;
	    if (groupidInfo != null) {
		oldGroupidMapping = groupidInfo.getNodeidArray();
	    }
	    netStream = brRes.getAdditionalMsg(2);
	    if (netStream != null) {
		try {
		    newGroupidInfo = new ShardInfoGroupid(netStream, oldGroupidMapping, this);
		} catch (Exception e) {
		    newGroupidInfo = null;
		}
	    }

	    netStream = brRes.getAdditionalMsg(3);
	    try {
		updateNodeState(netStream, (newNodeInfo == null ? nodeInfo : newNodeInfo));
	    } catch (Exception e) {
	    }
	}

	synchronized (this) {
	    if (resetAll == true || newNodeInfo != null) {
		oldNodeInfo = nodeInfo;
		nodeInfo = newNodeInfo;
	    }
	    if (resetAll == true || newGroupidInfo != null) {
		groupidInfo = newGroupidInfo;
	    }
	    if (brRes != null) {
		ready = true;
	    }
	    this.notifyAll();

	    BasicLogger logger = shardInfoManager.logger;
	    if (logger != null && waiterCount > 0) {
		synchronized (logger) {
		    logger.logInfo(String.format("ShardInfo(%s) waiter = %d%n", mgmtConInfo.getDbname(), waiterCount));
		}
	    }

	    this.lastSyncTime = System.currentTimeMillis();
	}

	if (oldNodeInfo != null && outObsoleteDsList != null) {
	    synchronized (outObsoleteDsList) {
		oldNodeInfo.setInvalid(outObsoleteDsList);
	    }
	}
    }

    private boolean unpackShardInfoHdr(byte[] netStream)
    {
	boolean resetAll = false;

	if (netStream != null) {
	    long oldCreatedAt = svrShardInfoCreatedAt;

	    try {
		InputBuffer in = new InputBuffer(netStream, RyeDriver.sysCharset);
		svrShardInfoCreatedAt = in.readLong();

		if (oldCreatedAt != svrShardInfoCreatedAt) {
		    resetAll = true;
		}

		this.lastShardInfoUpdateTime = System.currentTimeMillis();
	    } catch (Exception e) {
	    }
	}

	return resetAll;
    }

    private void updateNodeState(byte[] netStream, ShardInfoNode targetNodeInfo) throws JciException
    {
	if (netStream == null || targetNodeInfo == null) {
	    return;
	}

	ShardNodeInstance[] all = targetNodeInfo.getShardNodeInstance();
	if (all == null) {
	    return;
	}

	InputBuffer in = new InputBuffer(netStream, RyeDriver.sysCharset);

	int count = in.readInt();

	for (int i = 0; i < count; i++) {
	    int strSize = in.readInt();
	    String host = in.readString(strSize);
	    byte state = in.readByte();

	    if (host != null) {
		for (int j = 0; j < all.length; j++) {
		    if (all[j] != null && host.equals(all[j].getHost())) {
			all[j].setStatus(state);
		    }
		}
	    }
	}
    }

    String getDbname()
    {
	return mgmtConInfo.getDbname();
    }

    void decrWaiterCount()
    {
	this.waiterCount--;
    }

    void incrWaiterCount()
    {
	this.waiterCount++;
    }

    void dump()
    {
	if (nodeInfo != null) {
	    this.nodeInfo.dump();
	}
    }

    String dumpNodeInfo(int level)
    {
	if (nodeInfo == null) {
	    return null;
	}
	else {
	    return nodeInfo.dumpString(level);
	}
    }

    public ShardNodeInstance[] getShardNodeInstance()
    {
	if (nodeInfo == null) {
	    return null;
	}
	else {
	    return this.nodeInfo.getShardNodeInstance();
	}
    }

    public ShardNodeInstance[] getShardNodeInstance(int nodeid)
    {
	if (nodeInfo == null) {
	    return null;
	}
	else {
	    return this.nodeInfo.getShardNodeInstance(nodeid);
	}
    }

    String dumpGroupidInfo(int level)
    {
	if (groupidInfo == null) {
	    return null;
	}
	else {
	    return groupidInfo.dumpString(level);
	}
    }

    boolean hasNodeid(int nodeid)
    {
	if (nodeInfo == null) {
	    return false;
	}
	else {
	    return nodeInfo.hasNodeid(nodeid);
	}
    }

    void refreshShardInfo(int timeout, long expectShardInfoVersion)
    {
	if (timeout < 0) {
	    timeout = 600000; // 600 sec
	}
	long endTs = System.currentTimeMillis() + timeout;

	long prevSyncTime = this.lastSyncTime;

	boolean isAsyncCall = shardInfoManager.syncAllRequest();

	if (isAsyncCall) {
	    if (timeout > 0) {
		synchronized (this) {
		    while (true) {
			if (this.lastSyncTime != prevSyncTime || timeout <= 0) {
			    break;
			}
			if (expectShardInfoVersion > 0 && expectShardInfoVersion <= this.getShardInfoVersion()) {
			    break;
			}

			try {
			    this.wait(timeout);
			} catch (InterruptedException e) {
			}

			timeout = (int) (endTs - System.currentTimeMillis());
		    }
		}
	    }
	}
    }

    public static ShardInfo makeShardInfo(String shardMgmtHost, int shardMgmtPort, String globalDbname,
		    ShardInfoManager shardInfoManager) throws RyeException
    {
	JciConnectionInfo[] conInfo = new JciConnectionInfo[1];
	conInfo[0] = new JciConnectionInfo(shardMgmtHost, shardMgmtPort, "rw");

	ShardMgmtConnectionInfo shardMgmtConInfo = new ShardMgmtConnectionInfo(conInfo, globalDbname);

	ShardInfo shardInfo = new ShardInfo(shardMgmtConInfo, shardInfoManager);

	shardInfo.sync(null);

	return shardInfo;
    }
}
