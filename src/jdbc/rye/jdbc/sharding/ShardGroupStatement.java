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

import rye.jdbc.driver.RyeErrorCode;
import rye.jdbc.driver.RyeException;
import rye.jdbc.jci.BindParameter;
import rye.jdbc.jci.JciBatchResult;
import rye.jdbc.jci.JciConnection;
import rye.jdbc.jci.JciNormalStatement;
import rye.jdbc.jci.JciStatement;
import rye.jdbc.jci.JciStatementQueryInfo;
import rye.jdbc.jci.Protocol;
import rye.jdbc.jci.RyeCommandType;

public class ShardGroupStatement extends JciStatement
{
    private static final int EXECUTE_QUERY_MAX_RETRY = 3;

    private JciNormalStatement[] shardResultsetStmt;
    private RyeException selectNodeException;
    private int curShardRsIndex;
    private final ShardGroupConnection jciCon;
    private final boolean reuseShardStatement;

    private int affectedRows = -1;

    ShardGroupStatement(ShardGroupConnection con, JciStatementQueryInfo queryInfo, JciNormalStatement resultset)
    {
	super(queryInfo);

	jciCon = con;
	this.reuseShardStatement = jciCon.reuseShardStatement();

	if (resultset != null) {
	    shardResultsetStmt = new JciNormalStatement[1];
	    shardResultsetStmt[0] = resultset;
	    jciCon.addOpenCursor(this);
	}
    }

    public void close()
    {
	closeCursor();
    }

    public boolean cursorNext() throws RyeException
    {
	synchronized (jciCon) {
	    if (shardResultsetStmt == null) {
		return false;
	    }

	    while (curShardRsIndex < shardResultsetStmt.length) {
		JciNormalStatement curStmt = shardResultsetStmt[curShardRsIndex];
		if (curStmt != null && curStmt.cursorNext() == true) {
		    return true;
		}
		curShardRsIndex++;
	    }

	    if (selectNodeException != null) {
		throw selectNodeException;
	    }

	    return false;
	}
    }

    public void closeCursor()
    {
	synchronized (jciCon) {
	    if (shardResultsetStmt != null) {
		closeShardStatementArr(shardResultsetStmt);
		shardResultsetStmt = null;
		jciCon.rmOpenCursor(this);
	    }
	}
    }

    public void cancel() throws RyeException
    {
	jciCon.cancelRequest();
    }

    public void clearResult()
    {
    }

    public int getAffectedRows()
    {
	return affectedRows;
    }

    public JciBatchResult executeBatch(int queryTimeout, int groupId, ArrayList<BindParameter> batchParameter)
		    throws RyeException
    {
	synchronized (jciCon) {
	    refreshPrepareInfo();

	    for (int retryCount = 0;; retryCount++) {
		try {
		    return executeBatchInternal(queryTimeout, batchParameter);
		} catch (RyeException e) {
		    if (retryCount >= EXECUTE_QUERY_MAX_RETRY) {
			throw e;
		    }
		    checkDifferentShardKeyInfoError(e, true);
		}
	    }
	}
    }

    private void refreshPrepareInfo() throws RyeException
    {
	JciShardStatement shardStmt = null;
	try {
	    JciShardConnection shardCon = jciCon.getAnyShardConnection();
	    shardStmt = shardCon.prepareShard(null, this.getQueryInfo().getQuery(), this.getQueryInfo()
			    .getPrepareFlag(), true);
	    this.setQueryInfo(shardStmt.getQueryInfo());

	    // getQueryInfo().getShardKeyInfo().dump();
	} finally {
	    closeShardStatement(shardStmt);
	    jciCon.shardAutoCommit();
	}
    }

    private boolean checkDifferentShardKeyInfoError(RyeException e, boolean wasForcePrepare) throws RyeException
    {
	if (e.getErrorCode() == RyeErrorCode.ER_SHARD_DIFFERENT_SHARD_KEY_INFO) {
	    return false; /* retry without reprepare */
	}

	if (e.getErrorCode() == RyeErrorCode.ER_SHARD_INCOMPATIBLE_CON_REQUEST
			|| e.getErrorCode() == RyeErrorCode.ER_SHARD_MORE_THAN_ONE_SHARD_TRAN) {
	    if (wasForcePrepare) {
		throw e;
	    }
	    else {
		return true; /* retry after repreapre */
	    }
	}

	throw e;
    }

    private JciBatchResult executeBatchInternal(int queryTimeout, ArrayList<BindParameter> batchParameter)
		    throws RyeException
    {
	if (batchParameter == null || batchParameter.size() == 0) {
	    return new JciBatchResult(0);
	}

	QueryShardKeyInfo queryShardKeyInfo = this.getQueryInfo().getShardKeyInfo();

	RyeCommandType.SQL_TYPE sqlType = this.getSqlType();
	if (sqlType != RyeCommandType.SQL_TYPE.DML) {
	    RyeException.createRyeException(jciCon, RyeErrorCode.ER_INVALID_QUERY_TYPE_FOR_EXECUTEBATCH, null);
	}

	boolean isShardTableQuery = queryShardKeyInfo.isShardTableQuery();

	if (isShardTableQuery) {
	    if (queryShardKeyInfo.getShardKeyCount() != 1) {
		throw RyeException.createRyeException(jciCon, RyeErrorCode.ER_SHARD_DML_SHARD_KEY, null);
	    }

	    /* check if all bind parameters have same shard key value */
	    ShardKey shardKey = null;
	    for (BindParameter param : batchParameter) {
		String[] shardKeyValues = queryShardKeyInfo.getShardKeys(param, jciCon);

		ShardKey tmpShardKey = jciCon.shardInfo.makeShardKey(shardKeyValues[0], jciCon.getShardkeyCharset(),
				jciCon);
		if (shardKey == null) {
		    shardKey = tmpShardKey;
		}
		else {
		    if (shardKey.equals(tmpShardKey) == false) {
			throw RyeException.createRyeException(jciCon, RyeErrorCode.ER_SHARD_MORE_THAN_ONE_SHARD_TRAN,
					null);
		    }
		}
	    }

	    /* execute batch */
	    JciShardStatement shardStmt = null;
	    try {
		JciShardConnection shardCon = jciCon.getOneShardConnection(shardKey);
		shardStmt = shardCon.prepareShard(this, null, (byte) 0, true);
		JciBatchResult batchRes = shardStmt.executeBatch(queryTimeout, shardKey.getGroupid(), batchParameter);
		return batchRes;
	    } finally {
		closeShardStatement(shardStmt);
	    }
	}
	else {
	    JciBatchResult batchResult = new JciBatchResult(batchParameter.size());

	    int resIndex = 0;
	    for (BindParameter param : batchParameter) {
		try {
		    executeShardAllDML(true, 0, 0, 0, param);
		    batchResult.setResult(resIndex, this.getQueryInfo().getSqlCommandType(), this.getAffectedRows());
		} catch (RyeException e) {
		    batchResult.setResultError(resIndex, e.getErrorCode(), e.getMessage());
		    break;
		}
		resIndex++;
	    }
	    return batchResult;
	}
    }

    public Object getObject(int index) throws RyeException
    {
	synchronized (jciCon) {
	    if (shardResultsetStmt == null) {
		return null;
	    }
	    else {
		return shardResultsetStmt[curShardRsIndex].getObject(index);
	    }
	}
    }

    private boolean checkInvalidGroupidError(RyeException e, boolean isInTran, int queryTimeoutSec) throws RyeException
    {
	if (e.getErrorCode() == Protocol.SERVER_ER_SHARD_INVALID_GROUPID) {
	    long driverShardInfoVersion = jciCon.getDriverShardInfoVersion();
	    long serverShardInfoVersion = jciCon.getServerShardInfoVersion();

	    if (driverShardInfoVersion < serverShardInfoVersion && isInTran == false) {
		int refreshTimeout = (queryTimeoutSec <= 0 ? 1000 : queryTimeoutSec * 1000);
		jciCon.refreshShardInfo(refreshTimeout, serverShardInfoVersion);
		driverShardInfoVersion = jciCon.getDriverShardInfoVersion();

		if (driverShardInfoVersion < serverShardInfoVersion) {
		    throw RyeException.createRyeException(jciCon, RyeErrorCode.ER_SHARD_GROUPID_INFO_OBSOLETE, null);
		}
		else {
		    return true; /* retry */
		}
	    }

	    throw e;
	}
	else {
	    return false;
	}
    }

    public void execute(int groupId, int maxRow, int maxField, int queryTimeoutSec, BindParameter bindParameter)
		    throws RyeException
    {
	synchronized (jciCon) {
	    boolean forcePrepare = false;

	    for (int retryCount = 0;; retryCount++) {
		boolean isInTran = jciCon.isInTransaction();

		try {
		    executeInternal(forcePrepare, maxRow, maxField, queryTimeoutSec, bindParameter);
		    return;
		} catch (RyeException e) {
		    if (retryCount >= EXECUTE_QUERY_MAX_RETRY) {
			throw e;
		    }

		    boolean retryInvalidGroupid = checkInvalidGroupidError(e, isInTran, queryTimeoutSec);

		    if (retryInvalidGroupid == false) {
			checkDifferentShardKeyInfoError(e, forcePrepare);

			if (jciCon.traceShardConnLevel > 0) {
			    jciCon.appendTraceShardConnectionMsg("" + e.getErrorCode(), null);
			}

			refreshPrepareInfo();

			forcePrepare = true;
		    }

		    if (isInTran == false) {
			/* before retry, reset transaction status */
			jciCon.endTranRequest(false);
		    }
		} finally {
		    jciCon.shardAutoCommit();
		}
	    }
	}
    }

    private void executeInternal(boolean forcePrepare, int maxRow, int maxField, int queryTimeout,
		    BindParameter bindParameter) throws RyeException
    {
	QueryShardKeyInfo shardKeyInfo = this.getQueryInfo().getShardKeyInfo();

	boolean isShardTableQuery = shardKeyInfo.isShardTableQuery();
	String[] shardKeyValues = shardKeyInfo.getShardKeys(bindParameter, jciCon);

	if (isShardTableQuery == false && shardKeyValues.length > 0) {
	    throw RyeException.createRyeException(jciCon, RyeErrorCode.ER_SHARD_INTERNAL_ERROR,
			    "shard table = false, shardkey values =" + shardKeyValues.length, null);
	}

	ShardKey[] shardKeyArr = jciCon.shardInfo.getDistinctShardKeyArray(shardKeyValues, jciCon.getShardkeyCharset(),
			jciCon);

	RyeCommandType.SQL_TYPE sqlType = this.getSqlType();
	switch (sqlType)
	{
	case DDL:
	    jciCon.endTranRequest(true);

	    ShardAdmin shardAdmin = jciCon.getShardAdmin();
	    shardAdmin.DdlStart();

	    try {
		executeShardDDL(true, maxRow, maxField, queryTimeout, bindParameter);
	    } finally {
		shardAdmin.DdlEnd();
	    }
	    break;

	case DML:
	    if (isShardTableQuery) {
		if (shardKeyArr.length != 1) {
		    throw RyeException.createRyeException(jciCon, RyeErrorCode.ER_SHARD_DML_SHARD_KEY, null);
		}
		else {
		    executeShardDML(shardKeyArr[0], forcePrepare, maxRow, maxField, queryTimeout, bindParameter);
		}
	    }
	    else {
		executeShardAllDML(forcePrepare, maxRow, maxField, queryTimeout, bindParameter);
	    }

	    break;

	case SELECT_UPDATE:
	case SELECT:
	    if (sqlType == RyeCommandType.SQL_TYPE.SELECT_UPDATE && isShardTableQuery && shardKeyArr.length != 1) {
		throw RyeException.createRyeException(jciCon, RyeErrorCode.ER_SHARD_DML_SHARD_KEY, null);
	    }
	    if (isShardTableQuery == true) {
		if (shardKeyArr.length == 1) {
		    executeOneShardSELECT(shardKeyArr[0], forcePrepare, maxRow, maxField, queryTimeout, bindParameter);
		}
		else {
		    executeNShardSELECT(shardKeyArr, forcePrepare, maxRow, maxField, queryTimeout, bindParameter);
		}
	    }
	    else { /* not shard table query */
		executeOneShardSELECT(null, forcePrepare, maxRow, maxField, queryTimeout, bindParameter);
	    }

	    break;

	case COMMIT:
	    jciCon.endTranRequest(true);
	    affectedRows = 0;
	    break;
	case ROLLBACK:
	    jciCon.endTranRequest(false);
	    affectedRows = 0;
	    break;

	case IGNORE_ON_SHARDING:
	    affectedRows = 0;
	    break;

	case UNKNOWN:
	default:
	    throw RyeException.createRyeException(jciCon, RyeErrorCode.ER_SHARD_INTERNAL_ERROR, "unknown sql type: "
			    + sqlType + "(" + this.getQueryInfo().getSqlCommandType() + ")", null);
	}
    }

    /*
     * JciShardGroupStatement methods
     */

    boolean reuseShardStatement()
    {
	return this.reuseShardStatement;
    }

    private void executeShardDDL(boolean forcePrepare, int maxRow, int maxField, int queryTimeout,
		    BindParameter bindParameter) throws RyeException
    {
	boolean prevAutoCommit = jciCon.getAutoCommit();

	if (prevAutoCommit == false) {
	    jciCon.endTranRequest(true);
	    jciCon.setAutoCommit(true);
	}

	RyeException exception = null;

	JciShardConnection[] shardCons = jciCon.getAllDdlConnectionArray();
	int executeRes = 0;

	for (int i = 0; i < shardCons.length; i++) {
	    JciShardStatement shardStmt = null;

	    try {
		shardStmt = shardCons[i].prepareShard(this, null, (byte) 0, forcePrepare);
		shardStmt.execute(JciConnection.SHARD_GROUPID_UNKNOWN, maxRow, maxField, queryTimeout, bindParameter);
		executeRes = shardStmt.getAffectedRows();
	    } catch (RyeException e) {
		exception = RyeException.appendException(exception, e);
	    }

	    closeShardStatement(shardStmt);
	}

	jciCon.endTranRequest(true);
	jciCon.setAutoCommit(prevAutoCommit);

	if (exception == null) {
	    this.setExecuteResult(executeRes);
	}
	else {
	    throw exception;
	}
    }

    private void executeShardAllDML(boolean forcePrepare, int maxRow, int maxField, int queryTimeout,
		    BindParameter bindParameter) throws RyeException
    {
	boolean prevAutoCommit = jciCon.getAutoCommit();

	jciCon.setAutoCommit(false);

	JciShardConnection[] shardConArr = jciCon.getAllDMLConnectionArray();
	JciShardStatement[] shardStmtArr = new JciShardStatement[shardConArr.length];

	RyeException exception = prepareNShard(shardConArr, shardStmtArr, true, forcePrepare);

	exception = executeNShard(shardStmtArr, prevAutoCommit, exception, maxRow, maxField, queryTimeout,
			bindParameter);

	if (exception == null) {
	    int executeRes = -1;
	    for (int i = 0; i < shardStmtArr.length; i++) {
		if (shardStmtArr[i] != null) {
		    executeRes = shardStmtArr[i].getAffectedRows();
		    break;
		}
	    }
	    this.setExecuteResult(executeRes);
	}

	closeShardStatementArr(shardStmtArr);

	if (prevAutoCommit == true) {
	    jciCon.endTranRequest(exception == null ? true : false);
	    jciCon.setAutoCommit(true);
	}

	if (exception != null) {
	    throw exception;
	}
    }

    private void executeShardDML(ShardKey shardKey, boolean forcePrepare, int maxRow, int maxField, int queryTimeout,
		    BindParameter bindParameter) throws RyeException
    {
	JciShardConnection shardCon = jciCon.getOneShardConnection(shardKey);

	JciShardStatement shardStmt = null;
	try {
	    shardStmt = shardCon.prepareShard(this, null, (byte) 0, forcePrepare);
	    shardStmt.execute(shardKey.getGroupid(), maxRow, maxField, queryTimeout, bindParameter);
	    this.setExecuteResult(shardStmt.getAffectedRows());
	} finally {
	    closeShardStatement(shardStmt);
	}
    }

    private void executeOneShardSELECT(ShardKey shardKey, boolean forcePrepare, int maxRow, int maxField,
		    int queryTimeout, BindParameter bindParameter) throws RyeException
    {
	int groupid;
	JciShardConnection shardCon;
	if (shardKey == null) {
	    shardCon = jciCon.getAnyShardConnection();
	    groupid = JciConnection.SHARD_GROUPID_UNKNOWN;
	}
	else {
	    shardCon = jciCon.getOneShardConnection(shardKey);
	    groupid = shardKey.getGroupid();
	}

	JciShardStatement[] shardStmtArr = new JciShardStatement[1];

	try {
	    shardStmtArr[0] = shardCon.prepareShard(this, null, (byte) 0, forcePrepare);
	    shardStmtArr[0].execute(groupid, maxRow, maxField, queryTimeout, bindParameter);

	    this.setExecuteResult(shardStmtArr, null);
	} catch (RyeException e) {
	    closeShardStatement(shardStmtArr[0]);
	    throw e;
	}
    }

    private void executeNShardSELECT(ShardKey[] shardKeyArr, boolean forcePrepare, int maxRow, int maxField,
		    int queryTimeout, BindParameter bindParameter) throws RyeException
    {
	JciShardConnection[] shardConArr;

	if (shardKeyArr == null || shardKeyArr.length == 0) {
	    shardConArr = jciCon.getAllShardConnectionArray();
	}
	else {
	    shardConArr = jciCon.getNShardConnectionArray(shardKeyArr);
	}

	JciShardStatement[] shardStmtArr = new JciShardStatement[shardConArr.length];

	RyeException exception = prepareNShard(shardConArr, shardStmtArr, false, forcePrepare);

	exception = executeNShard(shardStmtArr, false, exception, maxRow, maxField, queryTimeout, bindParameter);

	int successCount = 0;
	for (int i = 0; i < shardStmtArr.length; i++) {
	    if (shardStmtArr[i] != null) {
		successCount++;
	    }
	}

	if (exception == null || successCount > 0) {
	    this.setExecuteResult(shardStmtArr, exception);
	}
	else {
	    throw exception;
	}
    }

    private RyeException prepareNShard(JciShardConnection[] conArr, JciShardStatement[] stmts, boolean stopOnError,
		    boolean forcePrepare)
    {
	QueryShardKeyInfo shardKeyInfo = null;
	RyeException exception = null;

	for (int i = 0; i < conArr.length; i++) {
	    try {
		stmts[i] = conArr[i].prepareShard(this, null, (byte) 0, forcePrepare);

		if (shardKeyInfo == null) {
		    shardKeyInfo = stmts[i].getQueryInfo().getShardKeyInfo();
		}
		else {
		    if (shardKeyInfo.equals(stmts[i].getQueryInfo().getShardKeyInfo()) == false) {
			throw RyeException.createRyeException(jciCon, RyeErrorCode.ER_SHARD_DIFFERENT_SHARD_KEY_INFO,
					null);
		    }
		}
	    } catch (RyeException e) {
		if (e.getErrorCode() == RyeErrorCode.ER_SHARD_DIFFERENT_SHARD_KEY_INFO || stopOnError) {
		    closeShardStatementArr(stmts);
		    return e;
		}
		exception = RyeException.appendException(exception, e);
	    }
	}

	return exception;
    }

    private RyeException executeNShard(JciShardStatement[] shardStmtArr, boolean stopOnError, RyeException exception,
		    int maxRow, int maxField, int queryTimeout, BindParameter bindParameter)
    {
	for (int i = 0; i < shardStmtArr.length; i++) {
	    try {
		if (shardStmtArr[i] != null) {
		    shardStmtArr[i].execute(JciConnection.SHARD_GROUPID_UNKNOWN, maxRow, maxField, queryTimeout,
				    bindParameter);
		}
	    } catch (RyeException e) {
		if (e.getErrorCode() == Protocol.SERVER_ER_SHARD_CANT_GLOBAL_DML_UNDER_MIGRATION) {
		    /* -513 error may return while migration. ignore */
		}
		else {
		    if (e.getErrorCode() == RyeErrorCode.ER_SHARD_DIFFERENT_SHARD_KEY_INFO && i == 0 || stopOnError) {
			closeShardStatementArr(shardStmtArr);
			return e;
		    }

		    closeShardStatement(shardStmtArr[i]);
		    shardStmtArr[i] = null;

		    exception = RyeException.appendException(exception, e);
		}
	    }
	}

	return exception;
    }

    static void closeShardStatement(JciNormalStatement stmt)
    {
	try {
	    if (stmt != null) {
		stmt.close();
	    }
	} catch (Exception e) {
	}
    }

    static void closeShardStatementArr(JciNormalStatement[] stmt)
    {
	for (int i = 0; i < stmt.length; i++) {
	    closeShardStatement(stmt[i]);
	    stmt[i] = null;
	}
    }

    private void setExecuteResult(int affectedRows)
    {
	shardResultsetStmt = null;
	curShardRsIndex = 0;
	selectNodeException = null;

	this.affectedRows = affectedRows;
    }

    private void setExecuteResult(JciShardStatement[] shardResultset, RyeException exception)
    {
	this.affectedRows = -1;

	shardResultsetStmt = shardResultset;
	curShardRsIndex = 0;
	selectNodeException = exception;

	if (shardResultsetStmt != null) {
	    jciCon.addOpenCursor(this);
	}
    }
}
