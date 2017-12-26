/*
 * Copyright (C) 2008 Search Solution Corporation. All rights reserved by Search Solution.
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 *
 * - Redistributions of source code must retain the above copyright notice,
 *   this list of conditions and the following disclaimer.
 *
 * - Redistributions in binary form must reproduce the above copyright notice,
 *   this list of conditions and the following disclaimer in the documentation
 *   and/or other materials provided with the distribution.
 *
 * - Neither the name of the <ORGANIZATION> nor the names of its contributors
 *   may be used to endorse or promote products derived from this software without
 *   specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 * IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA,
 * OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
 * OF SUCH DAMAGE.
 *
 */

package rye.jdbc.jci;

import java.io.IOException;
import java.util.ArrayList;

import rye.jdbc.driver.ConnectionProperties;
import rye.jdbc.driver.RyeConnectionUrl;
import rye.jdbc.driver.RyeDriver;
import rye.jdbc.driver.RyeErrorCode;
import rye.jdbc.driver.RyeException;
import rye.jdbc.sharding.QueryShardKeyInfo;
import rye.jdbc.sharding.ShardAdmin;
import rye.jdbc.sharding.ShardNodeInstance;

public class JciNormalConnection extends JciConnection
{
    private static final int CONNECT_FAIL_RETRY_COUNT = 3;

    private final static int SOCKET_TIMEOUT = 5000;

    private OutputBuffer outBuffer;

    private JciConnectionInfo curConnInfo;

    private boolean needReconnection;

    private JciSocket client;
    private boolean isClosed = false;

    private boolean skip_checkcas = false;
    private ArrayList<Integer> deferredCloseHandle;

    private long lastFailureTime = 0;

    private long connectTimestamp = 1; /*
				        * logical timestamp to check JciStatement's validity. increse at socket close
				        * time
				        */
    private long queryBeginTime;
    private int serverStartTime; /* variable for error_on_server_restart property. not implemented in jdbc. */

    protected JciNormalConnection(RyeConnectionUrl connUrl, ConnectionProperties connProperties, String dbuser,
		    String dbpasswd) throws JciException, RyeException
    {
	super(connUrl, dbuser, dbpasswd);

	if (connProperties != null) {
	    this.setConnectionProperties(connProperties);
	}

	needReconnection = true;
	curConnInfo = getFirstConnectionInfo();
	deferredCloseHandle = new ArrayList<Integer>();

	setBeginTime();
	checkReconnect();
	endTranRequest(true);
    }

    long getConnectTimestamp()
    {
	return this.connectTimestamp;
    }

    public int getServerStartTime()
    {
	return this.serverStartTime;
    }

    protected void changeConnectTimestamp()
    {
	this.connectTimestamp++;
    }

    synchronized public void close()
    {
	if (isClosed == true) {
	    return;
	}

	if (client != null) {
	    disconnectRequest();
	}

	if (client != null) {
	    clientSocketClose();
	}

	isClosed = true;
    }

    synchronized public void endTranRequest(boolean type) throws RyeException
    {
	if (isClosed == true) {
	    throw RyeException.createRyeException(this, RyeErrorCode.ER_IS_CLOSED, null);
	}

	if (needReconnection == true)
	    return;

	JciError uerror = null;

	try {
	    if (client != null && getCASInfoStatus() != Protocol.CAS_INFO_STATUS_INACTIVE) {
		setBeginTime();
		checkReconnect();

		if (getCASInfoStatus() == Protocol.CAS_INFO_STATUS_ACTIVE) {
		    outBuffer.newRequest(client.getOutputStream(), Protocol.CAS_FC_END_TRAN);
		    outBuffer.addByte((type == true) ? Protocol.END_TRAN_COMMIT : Protocol.END_TRAN_ROLLBACK);

		    send_recv_msg();
		}
	    }
	} catch (JciException e) {
	    uerror = new JciError(this, e);
	} catch (IOException e) {
	    uerror = new JciError(this, RyeErrorCode.ER_COMMUNICATION, e);
	} catch (Exception e) {
	    uerror = new JciError(this, RyeErrorCode.ER_JCI_UNKNOWN, e);
	}

	/*
	 * if (transactionList == null || transactionList.size() == 0) errorHandler.clear();
	 */

	boolean keepConnection = true;
	long currentTime = System.currentTimeMillis() / 1000;
	int reconnectTime = getReconnectTime();
	if (curConnInfo != getFirstConnectionInfo() && lastFailureTime != 0 && reconnectTime > 0
			&& currentTime - lastFailureTime > reconnectTime) {
	    if (!RyeDriver.unreachableHosts.contains(getFirstConnectionInfo())) {
		keepConnection = false;
		lastFailureTime = 0;
	    }
	}

	if (uerror != null || keepConnection == false) {
	    if (type == false) {
		uerror = null;
	    }

	    clientSocketClose();
	    needReconnection = true;
	}

	casInfo.setStatusInfoStatus(Protocol.CAS_INFO_STATUS_INACTIVE);

	if (uerror != null) {
	    throw RyeException.createRyeException(uerror, null);
	}
    }

    synchronized public String getVersionRequest() throws RyeException
    {
	if (isClosed == true) {
	    throw RyeException.createRyeException(this, RyeErrorCode.ER_IS_CLOSED, null);
	}
	try {
	    setBeginTime();
	    checkReconnect();

	    outBuffer.newRequest(client.getOutputStream(), Protocol.CAS_FC_GET_DB_VERSION);
	    outBuffer.addByte(getAutoCommit() ? (byte) 1 : (byte) 0);

	    InputBuffer inBuffer;
	    inBuffer = send_recv_msg();

	    int size = inBuffer.readInt();
	    if (size <= 0) {
		return null;
	    }
	    return inBuffer.readString(size, RyeDriver.sysCharset);
	} catch (JciException e) {
	    throw RyeException.createRyeException(this, e);
	} catch (IOException e) {
	    throw RyeException.createRyeException(this, RyeErrorCode.ER_COMMUNICATION, e);
	}
    }

    synchronized public int getServerHaMode() throws RyeException
    {
	if (isClosed == true) {
	    throw RyeException.createRyeException(this, RyeErrorCode.ER_IS_CLOSED, null);
	}
	try {
	    setBeginTime();
	    checkReconnect();

	    outBuffer.newRequest(client.getOutputStream(), Protocol.CAS_FC_SERVER_MODE);

	    InputBuffer inBuffer;
	    inBuffer = send_recv_msg();

	    int serverMode = inBuffer.readInt();
	    // int serverAddr = inBuffer.readInt();

	    return serverMode;
	} catch (JciException e) {
	    throw RyeException.createRyeException(this, e);
	} catch (IOException e) {
	    throw RyeException.createRyeException(this, RyeErrorCode.ER_COMMUNICATION, e);
	}
    }

    synchronized public String qeryplanRequest(String sql) throws RyeException
    {
	if (sql == null || sql.length() == 0)
	    return "";

	if (isClosed == true) {
	    throw RyeException.createRyeException(this, RyeErrorCode.ER_IS_CLOSED, null);
	}

	try {
	    setBeginTime();
	    checkReconnect();
	    outBuffer.newRequest(client.getOutputStream(), Protocol.CAS_FC_GET_QUERY_PLAN);
	    outBuffer.addStringWithNull(sql);

	    InputBuffer inBuffer;
	    inBuffer = send_recv_msg();

	    int infoSize = inBuffer.readInt();
	    String plan = inBuffer.readString(infoSize);
	    if (plan == null) {
		plan = "";
	    }

	    return plan;
	} catch (JciException e) {
	    throw RyeException.createRyeException(this, e);
	} catch (IOException e) {
	    throw RyeException.createRyeException(this, RyeErrorCode.ER_COMMUNICATION, e);
	}
    }

    synchronized public JciNormalStatement schemaInfoRequest(int type, String arg1, String arg2) throws RyeException
    {
	if (isClosed == true) {
	    throw RyeException.createRyeException(this, RyeErrorCode.ER_IS_CLOSED, null);
	}

	try {
	    setBeginTime();
	    checkReconnect();

	    outBuffer.newRequest(client.getOutputStream(), Protocol.CAS_FC_SCHEMA_INFO);
	    outBuffer.addInt(type);

	    if (arg1 == null)
		outBuffer.addNull();
	    else
		outBuffer.addStringWithNull(arg1);

	    if (arg2 == null)
		outBuffer.addNull();
	    else
		outBuffer.addStringWithNull(arg2);

	    outBuffer.addInt(0); /* flag. not used */

	    InputBuffer inBuffer;
	    inBuffer = send_recv_msg();

	    JciStatementQueryInfo queryInfo = makeStatementQueryInfo(null, (byte) 0, inBuffer);
	    int tupleCount = inBuffer.readInt();

	    return new JciNormalStatement(this, queryInfo, tupleCount);
	} catch (JciException e) {
	    throw RyeException.createRyeException(this, e);
	} catch (IOException e) {
	    throw RyeException.createRyeException(this, RyeErrorCode.ER_COMMUNICATION, e);
	}
    }

    boolean isErrorCommunication(int error)
    {
	switch (error)
	{
	case RyeErrorCode.ER_COMMUNICATION:
	case RyeErrorCode.ER_ILLEGAL_DATA_SIZE:
	case RyeErrorCode.CAS_ER_COMMUNICATION:
	    return true;
	default:
	    return false;
	}
    }

    boolean isErrorToReconnect(int error)
    {
	if (isErrorCommunication(error)) {
	    return true;
	}

	switch (error)
	{
	case Protocol.SERVER_ER_TM_SERVER_DOWN_UNILATERALLY_ABORTED:
	case Protocol.SERVER_ER_NET_SERVER_CRASHED:
	case Protocol.SERVER_ER_OBJ_NO_CONNECT:
	case Protocol.SERVER_ER_BO_CONNECT_FAILED:
	    return true;
	default:
	    return false;
	}
    }

    synchronized public JciStatement prepareRequest(String sql, byte flag) throws RyeException
    {
	try {
	    JciStatementQueryInfo queryInfo = prepareInternal(sql, flag);
	    return new JciNormalStatement(this, queryInfo);
	} catch (JciException e) {
	    throw RyeException.createRyeException(this, e);
	}
    }

    synchronized protected JciStatementQueryInfo prepareInternal(String sql, byte flag) throws JciException
    {
	if (isClosed) {
	    throw new JciException(RyeErrorCode.ER_IS_CLOSED);
	}

	boolean isFirstPrepareInTran = !isActive();

	skip_checkcas = true;

	JciError uerror = null;

	// first
	try {
	    checkReconnect();
	    InputBuffer inBuffer = sendRecvPrepareMessage(sql, flag);
	    return makeStatementQueryInfo(sql, flag, inBuffer);
	} catch (JciException e) {
	    uerror = new JciError(this, e);
	} catch (IOException e) {
	    uerror = new JciError(this, RyeErrorCode.ER_COMMUNICATION, e);
	} finally {
	    skip_checkcas = false;
	}

	if (isActive() && !isFirstPrepareInTran) {
	    throw JciException.createJciException(uerror);
	}

	// second loop
	int retryCount = CONNECT_FAIL_RETRY_COUNT;
	while (retryCount > 0 && isErrorToReconnect(uerror.getJdbcErrorCode())) {
	    if (!brokerInfoReconnectWhenServerDown() || isErrorCommunication(uerror.getJdbcErrorCode())) {
		clientSocketClose();
	    }

	    checkReconnect();

	    try {
		InputBuffer inBuffer = sendRecvPrepareMessage(sql, flag);
		return makeStatementQueryInfo(sql, flag, inBuffer);
	    } catch (JciException e) {
		uerror.setUJciException(e);
	    } catch (IOException e) {
		uerror.setErrorCode(RyeErrorCode.ER_COMMUNICATION, e);
	    }

	    retryCount--;
	}

	throw JciException.createJciException(uerror);
    }

    private InputBuffer sendRecvPrepareMessage(String sql, byte flag) throws IOException, JciException
    {
	outBuffer.newRequest(client.getOutputStream(), Protocol.CAS_FC_PREPARE);
	outBuffer.addStringWithNull(sql);
	outBuffer.addByte(flag);
	outBuffer.addByte(getAutoCommit() ? (byte) 1 : (byte) 0);

	outBuffer.addInt(deferredCloseHandle.size());
	for (Integer closeHandle : deferredCloseHandle) {
	    outBuffer.addInt(closeHandle.intValue());
	}
	deferredCloseHandle.clear();

	return send_recv_msg();
    }

    synchronized void closeStatementHandle(int handleId)
    {
	/*
	 * outBuffer.newRequest(client.getOutputStream(), Protocol.CAS_FC_CLOSE_REQ_HANDLE); outBuffer.addInt(handleId);
	 * outBuffer.addByte(getAutoCommit() ? (byte) 1 : (byte) 0); send_recv_msg();
	 */
	deferredCloseHandle.add(Integer.valueOf(handleId));
    }

    private byte getCASInfoStatus()
    {
	return casInfo.getStatusInfoStatus();
    }

    public short getStatusInfoServerNodeid()
    {
	return casInfo.getStatusInfoServerNodeid();
    }

    public long getStatusInfoServerShardInfoVersion()
    {
	return casInfo.getStatusInfoServerShardInfoVersion();
    }

    synchronized private boolean checkCasRequest(String msg)
    {
	if (isClosed == true)
	    return true;
	if (client == null || needReconnection == true)
	    return true;

	if (skip_checkcas) {
	    return true;
	}

	try {
	    outBuffer.newRequest(client.getOutputStream(), Protocol.CAS_FC_CHECK_CAS);
	    if (msg != null) {
		outBuffer.addStringWithNull(msg);
	    }
	    send_recv_msg();
	} catch (IOException e) {
	    logException(e);
	    return false;
	} catch (JciException e) {
	    logException(e);
	    return false;
	}

	return true;
    }

    synchronized void closeCursorRequest(int serverHandle)
    {
	try {
	    outBuffer.newRequest(client.getOutputStream(), Protocol.CAS_FC_CURSOR_CLOSE);
	    outBuffer.addInt(serverHandle);
	    send_recv_msg();
	} catch (JciException e) {
	    /* do nothing */
	} catch (IOException e) {
	    /* do nothing */
	}
    }

    synchronized InputBuffer executeRequest(int serverHandle, byte executeFlag, int maxField, byte autoCommit,
		    int remainingTime, int groupId, BindParameter bindParameter) throws JciException, IOException
    {
	outBuffer.newRequest(client.getOutputStream(), Protocol.CAS_FC_EXECUTE);
	outBuffer.addInt(serverHandle);
	outBuffer.addByte(executeFlag);
	outBuffer.addInt(maxField);
	outBuffer.addInt(0);
	outBuffer.addByte(autoCommit);
	outBuffer.addInt(remainingTime);
	outBuffer.addInt(groupId);

	if (bindParameter == null) {
	    outBuffer.addInt(0);
	}
	else {
	    outBuffer.addInt(bindParameter.getCount());
	    bindParameter.writeParameter(outBuffer);
	}

	return send_recv_msg();
    }

    synchronized InputBuffer executeBatchRequest(int serverHandle, int remainingTime, byte autoCommit, int groupId,
		    ArrayList<BindParameter> batchParameter) throws JciException, IOException
    {
	outBuffer.newRequest(client.getOutputStream(), Protocol.CAS_FC_EXECUTE_BATCH);
	outBuffer.addInt(serverHandle);
	outBuffer.addInt(remainingTime);
	outBuffer.addByte(autoCommit);
	outBuffer.addInt(groupId);

	if (batchParameter == null || batchParameter.size() == 0) {
	    outBuffer.addInt(0);
	    outBuffer.addInt(0);
	    outBuffer.addInt(0);
	}
	else {
	    outBuffer.addInt(batchParameter.size());
	    int numMarkers = ((BindParameter) batchParameter.get(0)).getCount();
	    outBuffer.addInt(numMarkers);
	    outBuffer.addInt(batchParameter.size() * numMarkers);

	    for (int i = 0; i < batchParameter.size(); i++) {
		BindParameter b = (BindParameter) batchParameter.get(i);
		b.writeParameter(outBuffer);
	    }
	}

	return send_recv_msg();
    }

    synchronized protected void changeDbuserRequest(String dbuser, String dbpasswd) throws RyeException
    {
	this.currDBuser = dbuser;
	this.currDBpasswd = dbpasswd;

	/* invalidate all statements */
	changeConnectTimestamp();

	JciError uerror = null;

	try {
	    if (client != null) {
		/*
		 * if connection is already closed, chage user reqesst is not needed. connection's user will be chaned
		 * at next request
		 */
		outBuffer.newRequest(client.getOutputStream(), Protocol.CAS_FC_CHANGE_DBUSER);
		outBuffer.addStringWithNull(dbuser);
		outBuffer.addStringWithNull(dbpasswd);

		send_recv_msg();
		return;
	    }
	} catch (IOException e) {
	    uerror = new JciError(this, RyeErrorCode.ER_COMMUNICATION);
	} catch (JciException e) {
	    uerror = new JciError(this, e);
	}

	if (uerror != null && !isErrorToReconnect(uerror.getJdbcErrorCode())) {
	    throw RyeException.createRyeException(uerror, null);
	}

	clientSocketClose();

	/* reconnect and send msg */
	try {
	    checkReconnect();
	} catch (JciException e) {
	    throw RyeException.createRyeException(new JciError(this, e), null);
	}
	endTranRequest(true);
    }

    synchronized InputBuffer fetchRequest(int serverHandle, int startPos) throws JciException, IOException
    {
	outBuffer.newRequest(client.getOutputStream(), Protocol.CAS_FC_FETCH);
	outBuffer.addInt(serverHandle);
	outBuffer.addInt(startPos);
	outBuffer.addInt(0); /* fetch size. not used */
	outBuffer.addInt(0); /* resultst index. 0: current resultset */

	return send_recv_msg();
    }

    protected void clientSocketClose()
    {
	needReconnection = true;

	if (client != null) {
	    client.close();
	}
	client = null;

	deferredCloseHandle.clear();

	changeConnectTimestamp();
    }

    private InputBuffer send_recv_msg() throws JciException, IOException
    {
	if (client == null) {
	    throw JciException.createJciException(this, RyeErrorCode.ER_COMMUNICATION);
	}

	outBuffer.sendData(casInfo.getStatusInfo());

	InputBuffer inputBuffer = new InputBuffer(client.getInputStream(), this, casInfo, this.getCharset());

	return inputBuffer;
    }

    public void cancelRequest() throws RyeException
    {
	try {
	    BrokerHandler.cancelQuery(curConnInfo, casInfo.getId(), casInfo.getPid(), getConnectTimeout() * 1000);
	} catch (JciException e) {
	    throw RyeException.createRyeException(this, e);
	} catch (IOException e) {
	    throw RyeException.createRyeException(this, RyeErrorCode.ER_COMMUNICATION, e);
	}
    }

    private int getTimeout(long endTimestamp, int timeout) throws JciException
    {
	if (endTimestamp == 0) {
	    return timeout;
	}

	long diff = endTimestamp - System.currentTimeMillis();
	if (diff <= 0) {
	    throw new JciException(RyeErrorCode.ER_TIMEOUT);
	}
	if (diff < timeout) {
	    return (int) diff;
	}

	return timeout;
    }

    private void reconnectWorker(long endTimestamp) throws JciException
    {
	try {
	    int timeout = getConnectTimeout() * 1000;
	    client = BrokerHandler.connectBroker(curConnInfo, getTimeout(endTimestamp, timeout), SOCKET_TIMEOUT);
	    connectDB(getTimeout(endTimestamp, timeout));

	    /* reset timeout not to throw ER_TIMEOUT exception while query execution */
	    client.setTimeout(0);

	    needReconnection = false;
	    isClosed = false;
	} catch (IOException e) {
	    if (client != null) {
		client.close();
		client = null;
	    }
	    throw new JciException(RyeErrorCode.ER_COMMUNICATION, e);
	} catch (JciException je) {
	    if (client != null) {
		client.close();
		client = null;
	    }
	    throw je;
	}
    }

    private void connectDB(int timeout) throws IOException, JciException
    {
	outBuffer.newRequest(client.getOutputStream(), Protocol.CAS_FC_CONNECT_DB);
	outBuffer.addStringWithNull(connUrl.getDbname());
	outBuffer.addStringWithNull(currDBuser);
	outBuffer.addStringWithNull(currDBpasswd);
	outBuffer.addStringWithNull(getUrlForLogging());
	outBuffer.addStringWithNull(RyeDriver.version_string);
	outBuffer.addBytes(casInfo.getDbSessionId());
	outBuffer.sendData(casInfo.getStatusInfo());

	InputBuffer inBuffer = new InputBuffer(client.getInputStream(), this, casInfo, this.getCharset());

	short verMajor = inBuffer.readShort();
	short verMinor = inBuffer.readShort();
	short verPatch = inBuffer.readShort();
	short verBuild = inBuffer.readShort();
	int id = inBuffer.readInt();
	int pid = inBuffer.readInt();
	int sessionIdLen = inBuffer.readInt();
	byte[] dbSessionId = casInfo.getDbSessionId();
	if (dbSessionId == null || dbSessionId.length != sessionIdLen) {
	    dbSessionId = new byte[sessionIdLen];
	}
	inBuffer.readBytes(dbSessionId);
	byte dbms = inBuffer.readByte();
	byte holdableResult = inBuffer.readByte();
	byte statementPooling = inBuffer.readByte();

	casInfo.set(verMajor, verMinor, verPatch, verBuild, id, pid, dbSessionId, dbms, holdableResult,
			statementPooling);

	inBuffer.readByte(); /* cci_default_autocommit. not used in jdbc */
	this.serverStartTime = inBuffer.readInt();
    }

    private long getLoginEndTimestamp(long timestamp)
    {
	int timeout = getConnectTimeout();
	if (timeout <= 0) {
	    return 0;
	}

	return timestamp + (timeout * 1000);
    }

    private void reconnect() throws JciException
    {
	int retry = 0;
	do {
	    for (JciConnectionInfo tmpConInfo : connUrl.getConInfoList()) {

		/*
		 * if all hosts turn out to be unreachable, ignore host reachability and try one more time
		 */
		if (!RyeDriver.isUnreachableHost(tmpConInfo) || retry == 1) {
		    try {
			curConnInfo = tmpConInfo;
			reconnectWorker(getLoginEndTimestamp(System.currentTimeMillis()));

			RyeDriver.unreachableHosts.remove(tmpConInfo);

			return; // success to connect
		    } catch (JciException e) {
			int errno = e.getJciError();

			if (errno == RyeErrorCode.ER_SHARD_MGMT) {
			    throw e;
			}

			logException(e);
			if (RyeErrorCode.isBrokerNotAvailable(errno)) {
			    RyeDriver.addToUnreachableHosts(tmpConInfo);
			}
			else {
			    throw e;
			}
		    }
		}
		lastFailureTime = System.currentTimeMillis() / 1000;
	    }
	    retry++;
	} while (retry < 2);
	// failed to connect to neither hosts
	throw JciException.createJciException(this, RyeErrorCode.ER_CONNECTION);
    }

    private void checkReconnect() throws JciException
    {
	if (outBuffer == null) {
	    outBuffer = new OutputBuffer(this);
	}

	if (getCASInfoStatus() == Protocol.CAS_INFO_STATUS_INACTIVE && checkCasRequest(null) == false) {
	    clientSocketClose();
	}

	if (needReconnection == true) {
	    reconnect();
	}
    }

    private void disconnectRequest()
    {
	try {
	    setBeginTime();
	    /*
	     * checkReconnect(); if (errorHandler.getErrorCode() != RyeErrorCode.ER_NO_ERROR) return;
	     */
	    outBuffer.newRequest(client.getOutputStream(), Protocol.CAS_FC_CON_CLOSE);
	    send_recv_msg();
	} catch (Exception e) {
	}
    }

    public String getCasInfoString()
    {
	return String.format("%s:%d:%s,%d,%d", curConnInfo.getHostname(), curConnInfo.getPort(),
			curConnInfo.getPortName(), casInfo.getId(), casInfo.getPid());
    }

    boolean isActive()
    {
	return getCASInfoStatus() == Protocol.CAS_INFO_STATUS_ACTIVE;
    }

    public void setTraceShardConnection(int level)
    {
    }

    public String getTraceShardConnection()
    {
	return null;
    }

    public ShardAdmin getShardAdmin() throws RyeException
    {
	throw new RyeException("JciNormalConnection cannot make ShardAdmin", null);
    }

    public void setReuseShardStatement(boolean reuseStatement)
    {
    }

    public boolean isShardingConnection()
    {
	return false;
    }

    public Object[] checkShardNodes() throws RyeException
    {
	return null;
    }

    public ShardNodeInstance[] getShardNodeInstance()
    {
	return null;
    }

    private JciStatementQueryInfo makeStatementQueryInfo(String sql, byte prepareFlag, InputBuffer inBuffer)
		    throws JciException
    {
	int serverHandleId = inBuffer.readInt();
	byte sqlCommandType = inBuffer.readByte();
	int numParameters = inBuffer.readInt();
	int numColumns = inBuffer.readInt();

	JciColumnInfo[] colInfo = readColumnInfo(numColumns, inBuffer);
	QueryShardKeyInfo shardKeyInfo = readShardKeyInfo(inBuffer);

	return new JciStatementQueryInfo(sql, prepareFlag, serverHandleId, sqlCommandType, numParameters, numColumns,
			colInfo, shardKeyInfo);

    }

    private JciColumnInfo[] readColumnInfo(int numColumns, InputBuffer inBuffer) throws JciException
    {
	byte type;
	short scale;
	int precision;
	String columnLabel;

	JciColumnInfo[] colInfo = new JciColumnInfo[numColumns];

	for (int i = 0; i < numColumns; i++) {
	    type = inBuffer.readByte();
	    scale = inBuffer.readShort();
	    precision = inBuffer.readInt();
	    int nameSize = inBuffer.readInt();
	    columnLabel = inBuffer.readString(nameSize);

	    String tableName = null;
	    String dbColName = null;
	    boolean isNullable = false;
	    String defValue = null;
	    boolean isUniqueKey = false;
	    boolean isPrimaryKey = false;

	    nameSize = inBuffer.readInt();
	    dbColName = inBuffer.readString(nameSize);
	    nameSize = inBuffer.readInt();
	    tableName = inBuffer.readString(nameSize);
	    isNullable = inBuffer.readBoolean();

	    defValue = inBuffer.readString(inBuffer.readInt());
	    isUniqueKey = inBuffer.readBoolean();
	    isPrimaryKey = inBuffer.readBoolean();

	    colInfo[i] = new JciColumnInfo(type, scale, precision, columnLabel, dbColName, tableName, isNullable,
			    defValue, isUniqueKey, isPrimaryKey);
	}

	return colInfo;
    }

    private QueryShardKeyInfo readShardKeyInfo(InputBuffer inBuffer) throws JciException
    {
	String[] shardKeyValues = null;
	int[] shardKeyPos = null;
	boolean containShardTable;

	containShardTable = inBuffer.readBoolean();

	int numShardKeyValues = inBuffer.readInt();
	if (numShardKeyValues > 0) {
	    shardKeyValues = new String[numShardKeyValues];
	    for (int i = 0; i < numShardKeyValues; i++) {
		int size = inBuffer.readInt();
		shardKeyValues[i] = inBuffer.readString(size);
	    }
	}

	int numShardKeyPos = inBuffer.readInt();
	if (numShardKeyPos > 0) {
	    shardKeyPos = new int[numShardKeyPos];
	    for (int i = 0; i < numShardKeyPos; i++) {
		shardKeyPos[i] = inBuffer.readInt();
	    }
	}

	return new QueryShardKeyInfo(containShardTable, shardKeyValues, shardKeyPos);
    }

    void setBeginTime()
    {
	queryBeginTime = System.currentTimeMillis();
    }

    long getRemainingTime(long timeout)
    {
	if (queryBeginTime == 0 || timeout == 0) {
	    return timeout;
	}

	long now = System.currentTimeMillis();
	return timeout - (now - queryBeginTime);
    }
}
