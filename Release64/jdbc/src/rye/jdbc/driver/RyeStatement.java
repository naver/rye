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

package rye.jdbc.driver;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;

import rye.jdbc.jci.JciBatchResult;
import rye.jdbc.jci.BindParameter;
import rye.jdbc.jci.JciConnection;
import rye.jdbc.jci.JciStatement;
import rye.jdbc.jci.RyeCommandType;

public class RyeStatement implements Statement
{
    protected RyeConnection con;
    protected JciConnection jciCon;
    protected JciStatement jciStmt;
    protected boolean completed;
    protected RyeResultSet result_set;
    protected boolean is_closed;
    protected int update_count;

    private int max_field_size;
    private int max_rows;
    protected int query_timeout;
    private int type;
    private boolean is_holdable;
    private int fetch_direction;
    private int fetch_size;
    private ArrayList<String> batchs;
    private boolean is_from_current_transaction;
    protected BindParameter bindParameter;

    private enum EXECUTE_TYPE {
	EXECUTE, EXECUTE_QUERY, EXECUTE_UPDATE
    };

    protected RyeStatement(RyeConnection c, int t, int hold)
    {
	con = c;
	jciCon = con.jciCon;
	jciStmt = null;
	is_closed = false;
	max_field_size = 0;
	max_rows = 0;
	update_count = -1;
	result_set = null;
	query_timeout = 0;
	type = t;

	is_holdable = false;
	if (hold == ResultSet.HOLD_CURSORS_OVER_COMMIT && jciCon.supportHoldableResult()) {
	    is_holdable = true;
	}

	fetch_direction = ResultSet.FETCH_FORWARD;
	fetch_size = 0;
	batchs = new ArrayList<String>();
	completed = true;
	is_from_current_transaction = true;
	bindParameter = null;
    }

    /*
     * java.sql.Statement interface
     */

    public synchronized void addBatch(String sql) throws SQLException
    {
	checkIsOpen();
	batchs.add(sql);
    }

    public synchronized void clearBatch() throws SQLException
    {
	checkIsOpen();
	batchs.clear();
    }

    public int[] executeBatch() throws SQLException
    {
	try {
	    checkIsOpen();

	    synchronized (con) {
		synchronized (this) {
		    checkIsOpen();
		    if (batchs.size() == 0) {
			return new int[0];
		    }

		    if (!completed) {
			complete();
		    }
		    JciBatchResult batch_results = executeBatchInternal();

		    batchs.clear();

		    con.autoCommit();
		    return batch_results.checkBatchResult();
		}
	    }
	} catch (NullPointerException e) {
	    throw con.createRyeException(RyeErrorCode.ER_STATEMENT_CLOSED, e);
	}
    }

    private JciBatchResult executeBatchInternal()
    {
	JciBatchResult batchResult = new JciBatchResult(batchs.size());
	RyePreparedStatement tmpStmt = null;

	int resIndex = 0;
	for (String query : batchs) {
	    try {
		tmpStmt = con.prepareStatement(query);

		RyeCommandType.SQL_TYPE sqlType = tmpStmt.getSQLType();
		switch (sqlType)
		{
		case DDL:
		case DML:
		    int res = tmpStmt.executeUpdate();
		    batchResult.setResult(resIndex, tmpStmt.getStatementType(), res);
		    break;
		default:
		    int errCode = RyeErrorCode.ER_INVALID_QUERY_TYPE_FOR_EXECUTEBATCH;
		    batchResult.setResultError(resIndex, errCode, RyeErrorCode.getErrorMsg(errCode));
		}
	    } catch (SQLException e) {
		batchResult.setResultError(resIndex, e.getErrorCode(), e.getMessage());
	    }

	    try {
		if (tmpStmt != null) {
		    tmpStmt.close();
		}
	    } catch (SQLException e) {
	    }
	    resIndex++;
	}

	return batchResult;
    }

    public boolean execute(String sql) throws SQLException
    {
	return execute(sql, EXECUTE_TYPE.EXECUTE);
    }

    public synchronized boolean execute(String sql, int autoGeneratedKeys) throws SQLException
    {
	if (autoGeneratedKeys == Statement.RETURN_GENERATED_KEYS) {
	    throw new SQLFeatureNotSupportedException();
	}

	return execute(sql);
    }

    public synchronized boolean execute(String sql, int[] columnIndexes) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public synchronized boolean execute(String sql, String[] columnNames) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public int executeUpdate(String sql) throws SQLException
    {
	execute(sql, EXECUTE_TYPE.EXECUTE_UPDATE);
	return update_count;
    }

    public synchronized int executeUpdate(String sql, int[] columnIndexes) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public synchronized int executeUpdate(String sql, String[] columnNames) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public synchronized int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException
    {
	if (autoGeneratedKeys == Statement.RETURN_GENERATED_KEYS) {
	    throw new SQLFeatureNotSupportedException();
	}

	return executeUpdate(sql);
    }

    public ResultSet executeQuery(String sql) throws SQLException
    {
	execute(sql, EXECUTE_TYPE.EXECUTE_QUERY);
	return result_set;
    }

    public void close() throws SQLException
    {
	try {
	    synchronized (con) {
		synchronized (this) {
		    if (is_closed) {
			return;
		    }
		    complete();
		    is_closed = true;
		    con.removeStatement(this);

		    con = null;
		}
	    }
	} catch (NullPointerException e) {
	}
    }

    public synchronized int getMaxFieldSize() throws SQLException
    {
	checkIsOpen();
	return max_field_size;
    }

    public synchronized void setMaxFieldSize(int max) throws SQLException
    {
	checkIsOpen();
	if (max < 0) {
	    throw RyeException.createRyeException(jciCon, RyeErrorCode.ER_INVALID_VALUE, null);
	}
	max_field_size = max;
    }

    public synchronized int getMaxRows() throws SQLException
    {
	checkIsOpen();
	return max_rows;
    }

    public synchronized void setMaxRows(int max) throws SQLException
    {
	checkIsOpen();
	if (max < 0) {
	    throw RyeException.createRyeException(jciCon, RyeErrorCode.ER_INVALID_VALUE, null);
	}
	max_rows = max;
    }

    public synchronized void setEscapeProcessing(boolean enable) throws SQLException
    {
	checkIsOpen();
    }

    public synchronized int getQueryTimeout() throws SQLException
    {
	checkIsOpen();
	return query_timeout;
    }

    public synchronized void setQueryTimeout(int seconds) throws SQLException
    {
	checkIsOpen();
	if (seconds < 0 || seconds > ConnectionProperties.MAX_QUERY_TIMEOUT) {
	    throw new IllegalArgumentException();
	}
	query_timeout = seconds;
    }

    public void cancel() throws SQLException
    {
	try {
	    checkIsOpen();
	    jciStmt.cancel();
	} catch (NullPointerException e) {
	}
    }

    public synchronized SQLWarning getWarnings() throws SQLException
    {
	checkIsOpen();
	return null;
    }

    public synchronized void clearWarnings() throws SQLException
    {
	checkIsOpen();
    }

    public synchronized void setCursorName(String name) throws SQLException
    {
	checkIsOpen();
    }

    public synchronized ResultSet getResultSet() throws SQLException
    {
	checkIsOpen();

	return result_set;
    }

    public synchronized int getUpdateCount() throws SQLException
    {
	checkIsOpen();

	return update_count;
    }

    public boolean getMoreResults() throws SQLException
    {
	try {
	    checkIsOpen();

	    closePrevResultSet();

	    complete();

	    return false;
	} catch (NullPointerException e) {
	    throw con.createRyeException(RyeErrorCode.ER_STATEMENT_CLOSED, e);
	}
    }

    public synchronized void setFetchDirection(int direction) throws SQLException
    {
	checkIsOpen();

	if (type != ResultSet.TYPE_FORWARD_ONLY)
	    throw con.createRyeException(RyeErrorCode.ER_NON_SCROLLABLE_STATEMENT, null);

	switch (direction)
	{
	case ResultSet.FETCH_FORWARD:
	case ResultSet.FETCH_REVERSE:
	case ResultSet.FETCH_UNKNOWN:
	    fetch_direction = direction;
	    break;
	default:
	    throw new IllegalArgumentException();
	}
    }

    public synchronized int getFetchDirection() throws SQLException
    {
	checkIsOpen();
	return fetch_direction;
    }

    public synchronized void setFetchSize(int rows) throws SQLException
    {
	checkIsOpen();
	if (rows <= 0) {
	    throw new IllegalArgumentException();
	}
	fetch_size = rows;
    }

    public synchronized int getFetchSize() throws SQLException
    {
	checkIsOpen();
	return fetch_size;
    }

    public synchronized int getResultSetConcurrency() throws SQLException
    {
	checkIsOpen();
	return ResultSet.CONCUR_READ_ONLY;
    }

    public synchronized int getResultSetType() throws SQLException
    {
	checkIsOpen();
	return type;
    }

    public synchronized Connection getConnection() throws SQLException
    {
	checkIsOpen();
	return con;
    }

    public synchronized ResultSet getGeneratedKeys() throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public synchronized boolean getMoreResults(int current) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public boolean isClosed() throws SQLException
    {
	return this.is_closed;
    }

    public boolean isPoolable() throws SQLException
    {
	throw new java.lang.UnsupportedOperationException();
    }

    public void setPoolable(boolean poolable) throws SQLException
    {
	throw new java.lang.UnsupportedOperationException();
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException
    {
	throw new java.lang.UnsupportedOperationException();
    }

    public <T> T unwrap(Class<T> iface) throws SQLException
    {
	throw new java.lang.UnsupportedOperationException();
    }

    public void closeOnCompletion() throws SQLException
    {
	throw new java.lang.UnsupportedOperationException();
    }

    public boolean isCloseOnCompletion() throws SQLException
    {
	throw new java.lang.UnsupportedOperationException();
    }

    /*
     * RyeStatement methods
     */

    int getHoldability()
    {
	if (this.is_holdable)
	    return ResultSet.HOLD_CURSORS_OVER_COMMIT;
	else
	    return ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    public synchronized int getResultSetHoldability() throws SQLException
    {
	checkIsOpen();

	return getHoldability();
    }

    public String getQueryplan(String sql) throws SQLException
    {
	checkIsOpen();

	try {
	    return jciCon.qeryplanRequest(sql);
	} finally {
	    con.autoRollback();
	}
    }

    protected boolean executeCore() throws SQLException
    {
	closePrevResultSet();

	completed = false;
	setCurrentTransaction(true);

	boolean is_holdable = false;
	if (getResultSetHoldability() == ResultSet.HOLD_CURSORS_OVER_COMMIT && con.jciCon.supportHoldableResult())
	    is_holdable = true;

	try {
	    jciStmt.execute(JciConnection.SHARD_GROUPID_UNKNOWN, max_rows, max_field_size, query_timeout, bindParameter);
	} catch (SQLException e) {
	    con.autoRollback();
	    throw e;
	}

	boolean result_type = jciStmt.isResultsetReturnable();

	if (result_type) {
	    result_set = new RyeResultSet(con, this, type, is_holdable);
	    update_count = -1;
	}
	else {
	    update_count = jciStmt.getAffectedRows();
	}

	return result_type;

    }

    private void closePrevResultSet() throws SQLException
    {
	if (result_set != null) {
	    result_set.close();
	    result_set = null;
	}
	update_count = -1;
    }

    void complete() throws SQLException
    {
	if (completed) {
	    return;
	}
	completed = true;

	if (result_set != null) {
	    result_set.close();
	    result_set = null;
	}

	if (jciStmt != null) {
	    jciStmt.close();
	    jciStmt = null;
	}

	con.autoCommit();
    }

    protected void checkIsOpen() throws SQLException
    {
	if (is_closed) {
	    if (con != null) {
		throw con.createRyeException(RyeErrorCode.ER_STATEMENT_CLOSED, null);
	    }
	    else {
		throw RyeException.createRyeException(jciCon, RyeErrorCode.ER_STATEMENT_CLOSED, null);
	    }
	}
    }

    public void setCurrentTransaction(boolean is_from_current_transaction)
    {
	this.is_from_current_transaction = is_from_current_transaction;
    }

    public boolean isFromCurrentTransaction()
    {
	return is_from_current_transaction;
    }

    protected boolean execute(String sql, EXECUTE_TYPE execType) throws SQLException
    {
	try {
	    checkIsOpen();

	    synchronized (con) {
		synchronized (this) {
		    long begin = 0;

		    if (jciCon.getLogSlowQuery()) {
			begin = System.currentTimeMillis();
		    }

		    checkIsOpen();
		    if (!completed) {
			complete();
		    }

		    byte prepareFlag = JciConnection.getPrepareFlag(is_holdable);
		    jciStmt = con.prepare(sql, prepareFlag);

		    checkQueryType(execType);

		    boolean isResultSet = executeCore();

		    if (result_set != null) {
			result_set.complete_on_close = true;
		    }

		    if (jciCon.getLogSlowQuery()) {
			long end = System.currentTimeMillis();
			jciCon.logSlowQuery(begin, end, sql, null);
		    }

		    return isResultSet;
		}
	    }
	} catch (NullPointerException e) {
	    throw con.createRyeException(RyeErrorCode.ER_STATEMENT_CLOSED, e);
	}
    }

    private void checkQueryType(EXECUTE_TYPE execType) throws SQLException
    {
	int invalidQueryTypeError = 0;

	switch (execType)
	{
	case EXECUTE_QUERY:
	    if (jciStmt.isResultsetReturnable() == false) {
		invalidQueryTypeError = RyeErrorCode.ER_INVALID_QUERY_TYPE_FOR_EXECUTEQUERY;
	    }
	    break;
	case EXECUTE_UPDATE:
	    if (jciStmt.isResultsetReturnable() == true) {
		invalidQueryTypeError = RyeErrorCode.ER_INVALID_QUERY_TYPE_FOR_EXECUTEUPDATE;
	    }
	    break;
	case EXECUTE:
	    break;
	}

	if (invalidQueryTypeError < 0) {
	    jciStmt.close();
	    jciStmt = null;
	    throw con.createRyeException(invalidQueryTypeError, null);
	}
    }

    void setBindParameter(BindParameter bindParam)
    {
	this.bindParameter = bindParam;
    }
}
