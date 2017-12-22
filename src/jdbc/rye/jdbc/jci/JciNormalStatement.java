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
import java.math.BigDecimal;
import java.util.ArrayList;

import rye.jdbc.driver.RyeDriver;
import rye.jdbc.driver.RyeErrorCode;
import rye.jdbc.driver.RyeException;
import rye.jdbc.driver.RyeType;
import rye.jdbc.sharding.ShardGroupStatement;

public class JciNormalStatement extends JciStatement
{
    private static final int ER_STMT_POOL_RETRY_COUNT = 3;

    private static final int CURSOR_BEFORE_FIRST = 0;
    private static final int CURSOR_AFTER_LAST = -1;

    private class ExecuteResultInfo
    {
	// variables assigned at execute time
	private byte executeFlag = 0;
	private int maxFetchSize;

	private byte casCursorStatus;
	private int totalTupleNumber;

	private int affectedRows;

	private int fetchedTupleNumber;
	private int currentFirstCursor;
	private int cursorPosition;

	private JciResultTuple tuples[];

	private void setExecuteResult(int result)
	{
	    if (maxFetchSize > 0 && result > maxFetchSize) {
		result = maxFetchSize;
	    }
	    totalTupleNumber = result;
	}

	private boolean hasCurrentPositionTuple()
	{
	    if (currentFirstCursor <= 0) {
		return false;
	    }
	    if (cursorPosition > 0) {
		if (cursorPosition < currentFirstCursor) {
		    return false;
		}
		if (cursorPosition > currentFirstCursor + fetchedTupleNumber - 1) {
		    return false;
		}
	    }

	    return true;
	}

	private Object getTupleValue(int index)
	{
	    if (tuples == null) {
		return null;
	    }

	    JciResultTuple curTuple = tuples[cursorPosition - currentFirstCursor];
	    if (curTuple == null) {
		return null;
	    }
	    else {
		return curTuple.getValue(index);
	    }
	}

	private void clear()
	{
	    tuples = null;
	    currentFirstCursor = CURSOR_BEFORE_FIRST;
	    cursorPosition = CURSOR_BEFORE_FIRST;
	    totalTupleNumber = 0;
	    fetchedTupleNumber = 0;
	}
    }

    private ExecuteResultInfo executeResultInfo;

    protected final JciNormalConnection jciCon;
    private boolean isClosed = false;

    /*
     * logical timestamp to check validity. if stmtTimestamp value is less than jciCon.connectTimestamp, this
     * statement's server handle was already closed.
     */
    private long stmtTimestamp;

    protected JciNormalStatement(JciNormalConnection con, JciStatementQueryInfo queryInfo) throws JciException
    {
	super(queryInfo);

	executeResultInfo = new ExecuteResultInfo();

	jciCon = con;

	clearResult();

	this.stmtTimestamp = jciCon.getConnectTimestamp();
    }

    JciNormalStatement(JciNormalConnection con, JciStatementQueryInfo queryInfo, int tupleCount) throws JciException
    {
	/* JciStatement for schema info request */

	this(con, queryInfo);

	executeResultInfo.casCursorStatus = Protocol.CAS_CURSOR_STATUS_OPEN;
	executeResultInfo.totalTupleNumber = tupleCount;
    }

    public void cancel() throws RyeException
    {
	if (isClosed == true || isServerHandleValid() == false) {
	    return;
	}
	jciCon.cancelRequest();
    }

    public void close()
    {
	synchronized (jciCon) {
	    if (isClosed == true) {
		return;
	    }

	    if (isServerHandleValid()) {
		if (jciCon.getAutoCommit() == false || jciCon.brokerInfoStatementPooling() == true
				|| ((queryInfo.getPrepareFlag() & Protocol.PREPARE_FLAG_HOLDABLE) != 0)) {
		    jciCon.closeStatementHandle(queryInfo.getServerHandleId());
		}
	    }

	    clearResult();

	    isClosed = true;
	}
    }

    public void closeCursor()
    {
	synchronized (jciCon) {
	    if (isServerHandleValid() && isResultsetReturnable()
			    && executeResultInfo.casCursorStatus != Protocol.CAS_CURSOR_STATUS_CLOSED) {
		jciCon.closeCursorRequest(queryInfo.getServerHandleId());
	    }

	    clearResult();
	}
    }

    public void execute(int groupId, int maxRow, int maxField, int queryTimeout, BindParameter bindParameter)
		    throws RyeException
    {
	if (queryInfo.getQuery() == null) {
	    return;
	}

	jciCon.setBeginTime();

	synchronized (jciCon) {
	    /* casCursorStatus will be re-assigned in fetch function */
	    executeResultInfo.casCursorStatus = Protocol.CAS_CURSOR_STATUS_OPEN;

	    if (isServerHandleValid() == false) {
		if (jciCon.brokerInfoStatementPooling()) {
		    try {
			reset();
		    } catch (JciException e) {
			throw RyeException.createRyeException(jciCon, e);
		    }
		}
		else {
		    throw RyeException.createRyeException(jciCon, RyeErrorCode.ER_IS_CLOSED, null);
		}
	    }

	    if (bindParameter != null && !bindParameter.checkAllBinded()) {
		throw RyeException.createRyeException(jciCon, RyeErrorCode.ER_NOT_BIND, null);
	    }

	    setMaxFetchSize(maxRow);
	    executeResultInfo.currentFirstCursor = CURSOR_BEFORE_FIRST;
	    executeResultInfo.fetchedTupleNumber = 0;
	    executeResultInfo.cursorPosition = CURSOR_BEFORE_FIRST;

	    boolean isFirstExecInTran = !jciCon.isActive();

	    JciError uerror = null;

	    try {
		executeInternal(groupId, maxField, queryTimeout, bindParameter);
		return;
	    } catch (JciException e) {
		uerror = new JciError(jciCon, e);
	    } catch (IOException e) {
		uerror = new JciError(jciCon, RyeErrorCode.ER_COMMUNICATION, e);
	    }

	    if (jciCon.isErrorToReconnect(uerror.getJdbcErrorCode())) {
		if (!jciCon.isActive() || isFirstExecInTran) {
		    if (!jciCon.brokerInfoReconnectWhenServerDown()
				    || jciCon.isErrorCommunication(uerror.getJdbcErrorCode())) {
			jciCon.clientSocketClose();
		    }

		    try {
			reset();
			executeInternal(groupId, maxField, queryTimeout, bindParameter);
			return;
		    } catch (JciException e) {
			uerror.setUJciException(e);
		    } catch (IOException e) {
			uerror.setErrorCode(RyeErrorCode.ER_COMMUNICATION, e);
		    }
		}
	    }

	    int retryCount = ER_STMT_POOL_RETRY_COUNT;
	    while (retryCount > 0 && jciCon.brokerInfoStatementPooling()
			    && uerror.getJdbcErrorCode() == RyeErrorCode.CAS_ER_STMT_POOLING) {
		try {
		    reset();
		    executeInternal(groupId, maxField, queryTimeout, bindParameter);
		    return;
		} catch (JciException e) {
		    uerror.setUJciException(e);
		} catch (IOException e) {
		    uerror.setErrorCode(RyeErrorCode.ER_COMMUNICATION, e);
		}
		retryCount--;
	    }

	    throw RyeException.createRyeException(uerror, null);
	}
    }

    public Object getObject(int index) throws RyeException
    {
	synchronized (jciCon) {
	    if (index < 0 || index >= queryInfo.getColumnCount()) {
		throw RyeException.createRyeException(jciCon, RyeErrorCode.ER_COLUMN_INDEX, null);
	    }

	    if (executeResultInfo.cursorPosition <= 0 || executeResultInfo.totalTupleNumber <= 0) {
		/* invalid cursor position. */
		return null;
	    }

	    if (executeResultInfo.hasCurrentPositionTuple() == false) {
		if (isClosed == true || isServerHandleValid() == false) {
		    throw RyeException.createRyeException(jciCon, RyeErrorCode.ER_IS_CLOSED, null);
		}

		/* refetch tuples from current cursor position */
		try {
		    InputBuffer inBuffer = jciCon.fetchRequest(queryInfo.getServerHandleId(),
				    executeResultInfo.cursorPosition);
		    read_fetch_data(inBuffer);
		} catch (IOException e) {
		    throw RyeException.createRyeException(jciCon, RyeErrorCode.ER_COMMUNICATION, null);
		} catch (JciException e) {
		    throw RyeException.createRyeException(jciCon, e);
		}
	    }

	    if (executeResultInfo.fetchedTupleNumber <= 0) {
		throw RyeException.createRyeException(jciCon, RyeErrorCode.ER_NO_MORE_DATA, null);
	    }

	    return executeResultInfo.getTupleValue(index);
	}
    }

    public JciBatchResult executeBatch(int queryTimeout, int groupId, ArrayList<BindParameter> batchParameter)
		    throws RyeException
    {
	synchronized (jciCon) {
	    JciBatchResult batchResult;

	    jciCon.setBeginTime();

	    if (isServerHandleValid() == false) {
		if (jciCon.brokerInfoStatementPooling()) {
		    try {
			reset();
		    } catch (JciException e) {
			throw RyeException.createRyeException(jciCon, e);
		    }
		}
		else {
		    throw RyeException.createRyeException(jciCon, RyeErrorCode.ER_IS_CLOSED, null);
		}
	    }

	    boolean isFirstExecInTran = !jciCon.isActive();
	    JciError uerror = null;

	    try {
		batchResult = executeBatchInternal(queryTimeout, groupId, batchParameter);
		return batchResult;
	    } catch (JciException e) {
		uerror = new JciError(jciCon, e);
	    } catch (IOException e) {
		uerror = new JciError(jciCon, RyeErrorCode.ER_COMMUNICATION);
	    }

	    if (jciCon.isErrorToReconnect(uerror.getJdbcErrorCode())) {
		if (!jciCon.isActive() || isFirstExecInTran) {
		    if (!jciCon.brokerInfoReconnectWhenServerDown()
				    || jciCon.isErrorCommunication(uerror.getJdbcErrorCode())) {
			jciCon.clientSocketClose();
		    }

		    try {
			reset();
			batchResult = executeBatchInternal(queryTimeout, groupId, batchParameter);
			return batchResult;
		    } catch (JciException e) {
			uerror.setUJciException(e);
		    } catch (IOException e) {
			uerror.setErrorCode(RyeErrorCode.ER_COMMUNICATION, e);
		    }
		}
	    }

	    int retryCount = ER_STMT_POOL_RETRY_COUNT;
	    while (retryCount > 0 && jciCon.brokerInfoStatementPooling()
			    && uerror.getJdbcErrorCode() == RyeErrorCode.CAS_ER_STMT_POOLING) {
		try {
		    reset();
		    batchResult = executeBatchInternal(queryTimeout, groupId, batchParameter);
		    return batchResult;
		} catch (JciException e) {
		    uerror.setUJciException(e);
		} catch (IOException e) {
		    uerror.setErrorCode(RyeErrorCode.ER_COMMUNICATION, e);
		}
		retryCount--;
	    }

	    throw RyeException.createRyeException(uerror, null);
	}
    }

    public int getAffectedRows()
    {
	return executeResultInfo.affectedRows;
    }

    public boolean cursorNext() throws RyeException
    {
	if (executeResultInfo.cursorPosition == CURSOR_AFTER_LAST) {
	    return false;
	}

	executeResultInfo.cursorPosition++;

	if (executeResultInfo.cursorPosition > executeResultInfo.totalTupleNumber) {
	    executeResultInfo.cursorPosition = CURSOR_AFTER_LAST;
	    return false;
	}

	return true;
    }

    public void clearResult()
    {
	executeResultInfo.clear();
	executeResultInfo.maxFetchSize = 0;

	executeResultInfo.casCursorStatus = Protocol.CAS_CURSOR_STATUS_CLOSED;
    }

    protected void reset() throws JciException
    {
	clearResult();

	queryInfo = jciCon.prepareInternal(queryInfo.getQuery(), queryInfo.getPrepareFlag());

	this.stmtTimestamp = jciCon.getConnectTimestamp();
    }

    protected ShardGroupStatement getShardGroupStatement()
    {
	return null;
    }

    protected boolean isServerHandleValid()
    {
	return (this.stmtTimestamp == jciCon.getConnectTimestamp());
    }

    private void executeInternal(int groupId, int maxField, int queryTimeout, BindParameter bindParameter)
		    throws JciException, IOException
    {
	long remainingTime = jciCon.getRemainingTime(queryTimeout * 1000);
	if (queryTimeout > 0 && remainingTime <= 0) {
	    throw JciException.createJciException(jciCon, RyeErrorCode.ER_TIMEOUT);
	}

	InputBuffer inBuffer = jciCon.executeRequest(queryInfo.getServerHandleId(), executeResultInfo.executeFlag,
			(maxField < 0 ? 0 : maxField), (jciCon.getAutoCommit() ? (byte) 1 : (byte) 0),
			(int) remainingTime, groupId, bindParameter);

	int executeResult = inBuffer.readInt();
	executeResultInfo.setExecuteResult(executeResult);

	inBuffer.readByte(); // cache_reusable

	byte stmtType = inBuffer.readByte();
	if (stmtType != queryInfo.getSqlCommandType()) {
	    throw new JciException(RyeErrorCode.ER_INTERNAL);
	}

	executeResultInfo.affectedRows = inBuffer.readInt();

	ShardGroupStatement shardGroupStmt = this.getShardGroupStatement();

	int numColumnInfo = inBuffer.readInt();
	for (int i = 0; i < numColumnInfo; i++) {
	    byte type = inBuffer.readByte();
	    short scale = inBuffer.readShort();
	    int precision = inBuffer.readInt();

	    queryInfo.updateColumnInfo(i, type, scale, precision);
	    if (shardGroupStmt != null) {
		shardGroupStmt.queryInfo.updateColumnInfo(i, type, scale, precision);
	    }
	}

	byte containFetchResult = inBuffer.readByte();
	if (containFetchResult == Protocol.EXEC_CONTAIN_FETCH_RESULT) {
	    read_fetch_data(inBuffer);
	}
    }

    private void setMaxFetchSize(int maxRow)
    {
	executeResultInfo.maxFetchSize = maxRow;
    }

    private JciBatchResult executeBatchInternal(int queryTimeout, int groupId, ArrayList<BindParameter> batchParameter)
		    throws IOException, JciException
    {
	long remainingTime = jciCon.getRemainingTime(queryTimeout * 1000);
	if (queryTimeout > 0 && remainingTime <= 0) {
	    throw JciException.createJciException(jciCon, RyeErrorCode.ER_TIMEOUT);
	}

	InputBuffer inBuffer = jciCon.executeBatchRequest(queryInfo.getServerHandleId(), (int) remainingTime,
			(jciCon.getAutoCommit() ? (byte) 1 : (byte) 0), groupId, batchParameter);

	batchParameter = null;
	JciBatchResult batchResult;

	int numQuery = inBuffer.readInt();
	batchResult = new JciBatchResult(numQuery);
	for (int i = 0; i < numQuery; i++) {
	    byte executeStatus = inBuffer.readByte();

	    if (executeStatus == Protocol.EXEC_QUERY_ERROR) {
		inBuffer.readInt(); /* error indicator */
		int srvErrCode = inBuffer.readInt();
		int srvErrMsgSize = inBuffer.readInt();
		String srvErrMsg = inBuffer.readString(srvErrMsgSize, RyeDriver.sysCharset);
		batchResult.setResultError(i, srvErrCode, srvErrMsg);
	    }
	    else {
		int result = inBuffer.readInt();
		batchResult.setResult(i, queryInfo.getSqlCommandType(), result);
	    }
	}

	return batchResult;
    }

    private void read_fetch_data(InputBuffer inBuffer) throws JciException
    {
	executeResultInfo.fetchedTupleNumber = inBuffer.readInt();
	if (executeResultInfo.fetchedTupleNumber < 0) {
	    executeResultInfo.fetchedTupleNumber = 0;
	}

	executeResultInfo.tuples = new JciResultTuple[executeResultInfo.fetchedTupleNumber];

	for (int idx = 0; idx < executeResultInfo.fetchedTupleNumber; idx++) {
	    int cursorPos = inBuffer.readInt();
	    if (idx == 0) {
		executeResultInfo.currentFirstCursor = cursorPos;
	    }

	    int colCount = queryInfo.getColumnCount();
	    Object[] values = new Object[colCount];
	    for (int i = 0; i < colCount; i++) {
		values[i] = readColumnValue(i, inBuffer);
	    }
	    executeResultInfo.tuples[idx] = new JciResultTuple(values);

	}

	executeResultInfo.casCursorStatus = inBuffer.readByte();
    }

    private Object readColumnValue(int index, InputBuffer inBuffer) throws JciException
    {
	int size;
	byte columnType;

	size = inBuffer.readInt();
	if (size < 0)
	    return null;

	JciColumnInfo[] colInfo = queryInfo.getColumnInfo();
	columnType = colInfo[index].getColumnType();
	if (columnType == RyeType.TYPE_NULL) {
	    columnType = inBuffer.readByte();
	    size--;
	}

	switch (columnType)
	{
	case RyeType.TYPE_VARCHAR:
	    return inBuffer.readString(size);
	case RyeType.TYPE_NUMERIC:
	    return new BigDecimal(inBuffer.readString(size, RyeDriver.sysCharset));
	case RyeType.TYPE_BIGINT:
	    return new Long(inBuffer.readLong());
	case RyeType.TYPE_INT:
	    return new Integer(inBuffer.readInt());
	case RyeType.TYPE_DATE:
	    return inBuffer.readDate();
	case RyeType.TYPE_TIME:
	    return inBuffer.readTime();
	case RyeType.TYPE_DATETIME:
	    return inBuffer.readDatetime();
	case RyeType.TYPE_DOUBLE:
	    return new Double(inBuffer.readDouble());
	case RyeType.TYPE_BINARY:
	    return inBuffer.readBytes(size);
	case RyeType.TYPE_NULL:
	    return null;
	default:
	    throw new JciException(RyeErrorCode.ER_TYPE_CONVERSION);
	}
    }

}
