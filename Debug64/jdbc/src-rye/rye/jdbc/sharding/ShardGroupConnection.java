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

import rye.jdbc.driver.RyeConnectionUrl;
import rye.jdbc.driver.RyeErrorCode;
import rye.jdbc.driver.RyeException;
import rye.jdbc.jci.JciConnection;
import rye.jdbc.jci.JciConnectionInfo;
import rye.jdbc.jci.JciNormalStatement;
import rye.jdbc.jci.JciUtil;
import rye.jdbc.jci.Protocol;

public class ShardGroupConnection extends JciConnection
{
    private enum SHARD_CONNECTION_TYPE {
	NONE, ONE_SHARD, ANY_SHARD, METADB, N_SHARD_SELECT, ALL_SHARD_SELECT, ALL_DML, ALL_DDL
    };

    final ShardInfo shardInfo;
    private Object curShardConn;
    private SHARD_CONNECTION_TYPE curShardConnType = SHARD_CONNECTION_TYPE.NONE;
    private Object cancelJobLock = new Object();
    private ShardAdmin shardAdmin = null;
    private boolean reuseShardStatement = true;

    StringBuffer traceConMsg = null;
    int traceShardConnLevel = 0; /* for shard connection test */

    private ArrayList<ShardGroupStatement> openCursorList;

    public ShardGroupConnection(RyeConnectionUrl connUrl, ShardInfo shardInfo, String user, String passwd)
		    throws RyeException
    {
	super(connUrl, user, passwd);

	this.shardInfo = shardInfo;
	openCursorList = new ArrayList<ShardGroupStatement>();

	if (this.getUseLazyConnection() == false) {
	    getVersionRequest(); /* test connection */
	}
    }

    public int getServerStartTime()
    {
	return -1;
    }

    synchronized boolean isInTransaction()
    {
	return (curShardConnType != SHARD_CONNECTION_TYPE.NONE);
    }

    synchronized public void close()
    {
	if (openCursorList.size() > 0) {
	    ShardGroupStatement[] stmtArr = new ShardGroupStatement[openCursorList.size()];
	    openCursorList.toArray(stmtArr);
	    openCursorList.clear();

	    for (int i = 0; i < stmtArr.length; i++) {
		stmtArr[i].close();
	    }
	}

	if (shardAdmin != null) {
	    shardAdmin.close();
	    shardAdmin = null;
	}

	try {
	    endTranRequest(false);
	} catch (RyeException e) {
	}
    }

    synchronized public void endTranRequest(boolean type) throws RyeException
    {
	RyeException retException = null;

	synchronized (cancelJobLock) {
	    if (curShardConn != null && openCursorList.size() == 0) {
		if (curShardConn instanceof JciShardConnection) {
		    retException = closeShardConnection((JciShardConnection) curShardConn, type);
		    curShardConn = null;
		}
		else {
		    JciShardConnection[] conArr = (JciShardConnection[]) curShardConn;

		    for (int i = 0; i < conArr.length; i++) {
			RyeException tmpException = closeShardConnection(conArr[i], type);

			if (tmpException != null) {
			    retException = RyeException.appendException(retException, tmpException);
			}
		    }

		    curShardConn = null;
		}
	    }
	}

	if (curShardConn == null) {
	    if (this.traceShardConnLevel > 1) {
		if (curShardConnType != SHARD_CONNECTION_TYPE.NONE) {
		    appendTraceShardConnectionMsg(type ? "COMMIT" : "ROLLBACK", null);
		}
	    }

	    curShardConnType = SHARD_CONNECTION_TYPE.NONE;
	}

	if (retException != null) {
	    throw retException;
	}
    }

    synchronized public String getVersionRequest() throws RyeException
    {
	try {
	    JciShardConnection con = this.getAnyShardConnection();
	    return con.getVersionRequest();
	} finally {
	    shardAutoCommit();
	}
    }

    synchronized public String qeryplanRequest(String sql) throws RyeException
    {
	try {
	    JciShardConnection con = this.getAnyShardConnection();
	    return con.qeryplanRequest(sql);
	} finally {
	    shardAutoCommit();
	}
    }

    synchronized public ShardGroupStatement schemaInfoRequest(int type, String arg1, String arg2) throws RyeException
    {
	try {
	    JciShardConnection con = this.getAnyShardConnection();
	    JciNormalStatement shardStmt = con.schemaInfoRequest(type, arg1, arg2);
	    return new ShardGroupStatement(this, shardStmt.getQueryInfo(), shardStmt);
	} finally {
	    shardAutoCommit();
	}
    }

    synchronized public ShardGroupStatement prepareRequest(String sql, byte flag) throws RyeException
    {
	try {
	    JciShardConnection con = this.getAnyShardConnection();
	    JciShardStatement shardStmt = con.prepareShard(null, sql, flag, false);
	    ShardGroupStatement stmt = new ShardGroupStatement(this, shardStmt.getQueryInfo(), null);
	    shardStmt.close();
	    return stmt;
	} finally {
	    shardAutoCommit();
	}
    }

    public void cancelRequest() throws RyeException
    {
	synchronized (cancelJobLock) {
	    if (curShardConn == null) {
		return;
	    }

	    if (curShardConn instanceof JciShardConnection) {
		((JciShardConnection) curShardConn).cancelRequest();
	    }
	    else if (curShardConn instanceof JciShardConnection[]) {
		JciShardConnection[] shardConArr = (JciShardConnection[]) curShardConn;
		RyeException exception = null;
		for (int i = 0; i < shardConArr.length; i++) {
		    try {
			shardConArr[i].cancelRequest();
		    } catch (RyeException e) {
			exception = e;
		    }
		}

		if (exception != null) {
		    throw exception;
		}
	    }
	}
    }

    public void setAutoCommit(boolean autoCommit)
    {
	super.setAutoCommit(autoCommit);

	if (curShardConn == null) {
	    return;
	}

	if (curShardConn instanceof JciShardConnection) {
	    ((JciShardConnection) curShardConn).setAutoCommit(autoCommit);
	}
	else {
	    JciShardConnection[] conArr = (JciShardConnection[]) curShardConn;

	    for (int i = 0; i < conArr.length; i++) {
		conArr[i].setAutoCommit(autoCommit);
	    }
	}
    }

    synchronized void appendTraceShardConnectionMsg(String msg, Object shardKeyArg)
    {
	if (traceShardConnLevel > 0) {
	    if (traceShardConnLevel == 1) {
		traceConMsg.setLength(0);
	    }
	    else {
		traceConMsg.append(" ");
	    }

	    traceConMsg.append(msg);

	    if (shardKeyArg != null) {
		try {
		    traceConMsg.append('(');
		    if (shardKeyArg instanceof ShardKey) {
			ShardKey shardKey = ((ShardKey) shardKeyArg);
			traceConMsg.append(new String(shardKey.getKey(), this.getShardkeyCharset()));
		    }
		    else if (shardKeyArg instanceof ShardKey[]) {
			ShardKey[] shardKeyArr = (ShardKey[]) shardKeyArg;
			for (int i = 0; i < shardKeyArr.length; i++) {
			    if (i != 0) {
				traceConMsg.append(',');
			    }
			    traceConMsg.append(new String(shardKeyArr[i].getKey(), this.getShardkeyCharset()));
			}
		    }
		    traceConMsg.append(')');
		} catch (Exception e) {
		}
	    }
	}
    }

    synchronized public void setTraceShardConnection(int level)
    {
	traceShardConnLevel = level;

	if (traceShardConnLevel == 0) {
	    traceConMsg = null;
	}
	else {
	    if (traceConMsg == null) {
		traceConMsg = new StringBuffer();
	    }
	}
    }

    synchronized public String getTraceShardConnection()
    {
	if (traceConMsg == null) {
	    return null;
	}
	else {
	    String msg = traceConMsg.toString();
	    traceConMsg.setLength(0);
	    return msg;
	}
    }

    public String getCasInfoString()
    {
	return String.format("%s:%d:%s", getFirstConnectionInfo().getHostname(), getFirstConnectionInfo().getPort(),
			getFirstConnectionInfo().getstrPortName());
    }

    synchronized public ShardAdmin getShardAdmin()
    {
	if (shardAdmin == null) {
	    shardAdmin = new ShardAdmin(this.getFirstConnectionInfo(), this.getQueryTimeout() * 1000, shardInfo, this);
	}
	return shardAdmin;
    }

    boolean reuseShardStatement()
    {
	return reuseShardStatement;
    }

    public void setReuseShardStatement(boolean reuseStatement)
    {
	this.reuseShardStatement = reuseStatement;
    }

    public boolean isShardingConnection()
    {
	return true;
    }

    public Object[] checkShardNodes() throws RyeException
    {
	JciShardConnection[] conArr = this.getAllShardConnectionArray();
	JciShardStatement[] stmtArr = new JciShardStatement[conArr.length];

	int[] success = new int[conArr.length];
	int[] fail = new int[conArr.length];

	int successCount = 0;
	int failCount = 0;

	String sql = "select 1";
	for (int i = 0; i < conArr.length; i++) {
	    try {
		stmtArr[i] = conArr[i].prepareShard(null, sql, (byte) 0, true);

		if (conArr[i].getShardNodeid().getNodeId() == conArr[i].getStatusInfoServerNodeid()) {
		    success[successCount++] = conArr[i].getShardNodeid().getNodeId();
		}
		else {
		    fail[failCount++] = conArr[i].getShardNodeid().getNodeId();
		}
	    } catch (Exception e) {
		fail[failCount++] = conArr[i].getShardNodeid().getNodeId();
	    }
	}

	ShardGroupStatement.closeShardStatementArr(stmtArr);

	this.shardAutoCommit();

	success = JciUtil.resizeIntArray(success, successCount);
	fail = JciUtil.resizeIntArray(fail, failCount);

	Object[] res = new Object[2];
	res[0] = success;
	res[1] = fail;

	return res;
    }

    public int getServerHaMode() throws RyeException
    {
	return Protocol.HA_STATE_NA;
    }

    public short getStatusInfoServerNodeid()
    {
	return -1;
    }

    public ShardNodeInstance[] getShardNodeInstance()
    {
	return shardInfo.getShardNodeInstance();
    }

    /*
     * JciShardGroupConnection methods
     */

    synchronized JciShardConnection getAnyShardConnection() throws RyeException
    {
	return getConnection(SHARD_CONNECTION_TYPE.ANY_SHARD, null);
    }

    synchronized JciShardConnection getOneShardConnection(ShardKey shardKey) throws RyeException
    {
	return getConnection(SHARD_CONNECTION_TYPE.ONE_SHARD, shardKey);
    }

    synchronized JciShardConnection[] getNShardConnectionArray(ShardKey[] shardKeyArr) throws RyeException
    {
	if (this.traceShardConnLevel > 0) {
	    appendTraceShardConnectionMsg(conTypeString(SHARD_CONNECTION_TYPE.N_SHARD_SELECT), shardKeyArr);
	}

	return getConnectionArray(SHARD_CONNECTION_TYPE.N_SHARD_SELECT,
			shardInfo.getDistinctNodeIdArray(shardKeyArr, this), this);
    }

    synchronized JciShardConnection[] getAllShardConnectionArray() throws RyeException
    {
	if (this.traceShardConnLevel > 0) {
	    appendTraceShardConnectionMsg(conTypeString(SHARD_CONNECTION_TYPE.ALL_SHARD_SELECT), null);
	}

	return getConnectionArray(SHARD_CONNECTION_TYPE.ALL_SHARD_SELECT, shardInfo.getAllNodeidArray(false, this),
			this);
    }

    synchronized JciShardConnection[] getAllDMLConnectionArray() throws RyeException
    {
	if (this.traceShardConnLevel > 0) {
	    appendTraceShardConnectionMsg(conTypeString(SHARD_CONNECTION_TYPE.ALL_DML), null);
	}

	return getConnectionArray(SHARD_CONNECTION_TYPE.ALL_DML, shardInfo.getAllNodeidArray(true, this), this);
    }

    synchronized JciShardConnection[] getAllDdlConnectionArray() throws RyeException
    {
	if (this.traceShardConnLevel > 0) {
	    appendTraceShardConnectionMsg(conTypeString(SHARD_CONNECTION_TYPE.ALL_DDL), null);
	}

	return getConnectionArray(SHARD_CONNECTION_TYPE.ALL_DDL, shardInfo.getAllNodeidArray(true, this), this);
    }

    private JciShardConnection getConnection(SHARD_CONNECTION_TYPE conType, ShardKey shardKey) throws RyeException
    {
	if (this.traceShardConnLevel > 0) {
	    appendTraceShardConnectionMsg(conTypeString(conType), shardKey);
	}

	ShardNodeId nodeid = null;
	if (conType == SHARD_CONNECTION_TYPE.ONE_SHARD) {
	    if (shardKey == null) {
		throw RyeException.createRyeException(this, RyeErrorCode.ER_SHARD_DML_SHARD_KEY, null);
	    }

	    nodeid = shardKey.getShardNodeId();
	}
	else if (conType == SHARD_CONNECTION_TYPE.METADB) {
	    nodeid = shardInfo.getMetadbNodeid();
	}
	else if (conType == SHARD_CONNECTION_TYPE.ANY_SHARD) {
	    nodeid = shardInfo.getRandomNodeid(this);
	}
	else {
	    throw incompatibleConRequest(conType);
	}

	if (curShardConn == null) {
	    RyeConnectionUrl conUrl = this.getConnectionUrl();
	    JciConnectionInfo conInfo = conUrl.getConInfoList().get(0);

	    JciShardConnection con = shardInfo
			    .getOneConnection(nodeid, conInfo.getstrPortName(), conUrl.getConnProperties(),
					    conUrl.getDbuser(), conUrl.getDbpasswd(), getAutoCommit(), this);

	    con.setShardKey(shardKey);
	    curShardConn = con;
	    curShardConnType = conType;

	    return con;
	}

	if (conType == SHARD_CONNECTION_TYPE.ONE_SHARD) {
	    if (curShardConn instanceof JciShardConnection) {
		if (((JciShardConnection) curShardConn).equalsShardKey(shardKey)) {
		    return (JciShardConnection) curShardConn;
		}
	    }
	    throw RyeException.createRyeException(this, RyeErrorCode.ER_SHARD_MORE_THAN_ONE_SHARD_TRAN, null);
	}
	else if (conType == SHARD_CONNECTION_TYPE.METADB) {
	    if (curShardConn instanceof JciShardConnection) {
		if (((JciShardConnection) curShardConn).equalsNodeId(nodeid)) {
		    return (JciShardConnection) curShardConn;
		}
	    }
	}
	else if (conType == SHARD_CONNECTION_TYPE.ANY_SHARD) {
	    ShardNodeId metaid = shardInfo.getMetadbNodeid();

	    if (curShardConn instanceof JciShardConnection) {
		if (((JciShardConnection) curShardConn).equalsNodeId(metaid) == false) {
		    return (JciShardConnection) curShardConn;
		}
	    }
	    else {
		JciShardConnection[] conArr = (JciShardConnection[]) curShardConn;
		for (int i = 0; i < conArr.length; i++) {
		    if (conArr[i].equalsNodeId(metaid) == false) {
			return conArr[i];
		    }
		}
	    }
	}

	throw incompatibleConRequest(conType);
    }

    private JciShardConnection[] getConnectionArray(SHARD_CONNECTION_TYPE conType, ShardNodeId[] nodeids,
		    JciConnection jciCon) throws RyeException
    {
	if (curShardConn == null) {
	    RyeConnectionUrl conUrl = this.getConnectionUrl();
	    JciConnectionInfo conInfo = conUrl.getConInfoList().get(0);

	    JciShardConnection[] conArr = shardInfo.getNConnectionArray(nodeids, conInfo.getstrPortName(),
			    conUrl.getConnProperties(), conUrl.getDbuser(), conUrl.getDbpasswd(), getAutoCommit(),
			    jciCon);

	    curShardConn = conArr;
	    curShardConnType = conType;

	    return conArr;
	}

	if (curShardConn instanceof JciShardConnection) {
	    throw incompatibleConRequest(conType);
	}

	JciShardConnection[] conArr = (JciShardConnection[]) curShardConn;

	if (conType == SHARD_CONNECTION_TYPE.N_SHARD_SELECT || conType == SHARD_CONNECTION_TYPE.ALL_SHARD_SELECT) {
	    if (curShardConnType == SHARD_CONNECTION_TYPE.ALL_DML) {
		ArrayList<JciShardConnection> newConList = new ArrayList<JciShardConnection>();
		for (int i = 0; i < conArr.length; i++) {
		    int idx = Arrays.binarySearch(nodeids, conArr[i].getShardNodeid());
		    if (idx >= 0) {
			newConList.add(conArr[i]);
		    }
		}
		JciShardConnection[] newConArr = new JciShardConnection[newConList.size()];
		newConList.toArray(newConArr);
		return newConArr;
	    }
	}
	else if (conType == SHARD_CONNECTION_TYPE.ALL_DML) {
	    if (curShardConnType == SHARD_CONNECTION_TYPE.ALL_DML) {
		return conArr;
	    }
	}

	throw incompatibleConRequest(conType);
    }

    private RyeException incompatibleConRequest(SHARD_CONNECTION_TYPE requiredConType)
    {
	/*
	 * if this exception occur, there may be wrong transaction handling. check it
	 */
	String msg = String.format("(%s, %s)", conTypeString(curShardConnType), conTypeString(requiredConType));
	return RyeException.createRyeException(this, RyeErrorCode.ER_SHARD_INCOMPATIBLE_CON_REQUEST, msg, null);
    }

    private String conTypeString(SHARD_CONNECTION_TYPE conType)
    {
	switch (conType)
	{
	case NONE:
	    return "NONE";
	case ONE_SHARD:
	    return "ONE-SHARD";
	case METADB:
	    return "METADB";
	case ALL_DML:
	    return "ALL-DML";
	case ANY_SHARD:
	    return "ANY-SHARD";
	case N_SHARD_SELECT:
	    return "N-SHARD";
	case ALL_SHARD_SELECT:
	    return "ALL-SHARD";
	case ALL_DDL:
	    return "ALL-DDL";
	}
	return "UNKNOWN";
    }

    private RyeException closeShardConnection(JciShardConnection shardCon, boolean commit)
    {
	RyeException retException = null;

	try {
	    shardCon.endTranRequest(commit);
	} catch (RyeException e) {
	    retException = e;
	} finally {
	    try {
		shardCon.close();
	    } catch (Exception ne) {
	    }
	}

	shardInfo.checkServerShardInfoVersion(shardCon.getStatusInfoServerShardInfoVersion());

	return retException;
    }

    long getDriverShardInfoVersion()
    {
	return shardInfo.getShardInfoVersion();
    }

    long getServerShardInfoVersion()
    {
	long serverShardInfoVersion = 0;

	if (curShardConn != null) {
	    if (curShardConn instanceof JciShardConnection) {
		serverShardInfoVersion = ((JciShardConnection) curShardConn).getStatusInfoServerShardInfoVersion();
	    }
	    else {
		JciShardConnection[] conArr = (JciShardConnection[]) curShardConn;
		if (conArr.length > 0) {
		    serverShardInfoVersion = conArr[0].getStatusInfoServerShardInfoVersion();
		}
	    }
	}

	return serverShardInfoVersion;
    }

    void shardAutoCommit() throws RyeException
    {
	switch (curShardConnType)
	{
	case ANY_SHARD:
	case ALL_DDL:
	case N_SHARD_SELECT:
	case ALL_SHARD_SELECT:
	    try {
		endTranRequest(true);
	    } catch (RyeException e) {
		endTranRequest(false);
	    }
	    break;
	case METADB:
	case ALL_DML:
	case NONE:
	case ONE_SHARD:
	default:
	    break;
	}
    }

    synchronized void addOpenCursor(ShardGroupStatement stmt)
    {
	openCursorList.add(stmt);
    }

    synchronized void rmOpenCursor(ShardGroupStatement stmt)
    {
	openCursorList.remove(stmt);
    }

    void refreshShardInfo(int timeout, long serverShardInfoVersion)
    {
	shardInfo.refreshShardInfo(timeout, serverShardInfoVersion);
    }
}
