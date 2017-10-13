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

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.sql.SQLFeatureNotSupportedException;

import rye.jdbc.jci.JciBatchResult;
import rye.jdbc.jci.BindParameter;
import rye.jdbc.jci.JciColumnInfo;
import rye.jdbc.jci.JciConnection;
import rye.jdbc.jci.JciStatement;
import rye.jdbc.jci.RyeCommandType;

public class RyePreparedStatement extends RyeStatement implements PreparedStatement
{
    private boolean first_result_type;
    private ArrayList<BindParameter> batchParameter;
    private final int parameterCount;

    protected RyePreparedStatement(RyeConnection c, JciStatement us, int t, int concur, int hold)
    {
	super(c, t, hold);
	jciStmt = us;
	first_result_type = jciStmt.isResultsetReturnable();
	parameterCount = us.getQueryInfo().getParameterCount();
	bindParameter = new BindParameter(parameterCount);
    }

    /*
     * java.sql.PreparedStatement interface
     */

    public synchronized void addBatch() throws SQLException
    {
	checkIsOpen();

	if (bindParameter.checkAllBinded() == false) {
	    throw con.createRyeException(RyeErrorCode.ER_NOT_ALL_BINDED, null);
	}

	if (batchParameter == null) {
	    batchParameter = new ArrayList<BindParameter>();
	}

	batchParameter.add(bindParameter);

	bindParameter = new BindParameter(bindParameter.getCount());
    }

    public synchronized void clearBatch() throws SQLException
    {
	checkIsOpen();

	if (batchParameter != null) {
	    batchParameter.clear();
	}
    }

    public int[] executeBatch() throws SQLException
    {
	try {
	    synchronized (con) {
		synchronized (this) {
		    checkIsOpen();

		    if (batchParameter == null || batchParameter.size() == 0) {
			return new int[0];
		    }

		    if (!completed)
			complete();
		    checkIsOpen();

		    JciBatchResult results;
		    try {
			results = jciStmt.executeBatch(query_timeout, JciConnection.SHARD_GROUPID_UNKNOWN,
					batchParameter);
		    } catch (SQLException e) {
			con.autoRollback();
			throw e;
		    }

		    con.autoCommit();
		    return results.checkBatchResult();
		}
	    }
	} catch (NullPointerException e) {
	    throw RyeException.createRyeException(jciCon, RyeErrorCode.ER_PREPARED_STATEMENT_CLOSED, e);
	}
    }

    public boolean execute() throws SQLException
    {
	try {
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
		    checkIsOpen();

		    executeCore();

		    if (result_set != null) {
			result_set.complete_on_close = true;
		    }

		    if (jciStmt.isResultsetReturnable() == false) {
			complete();
		    }

		    if (jciCon.getLogSlowQuery()) {
			long end = System.currentTimeMillis();
			jciCon.logSlowQuery(begin, end, jciStmt.getQueryInfo().getQuery(), bindParameter);
		    }
		    return first_result_type;
		}
	    }
	} catch (NullPointerException e) {
	    throw RyeException.createRyeException(jciCon, RyeErrorCode.ER_PREPARED_STATEMENT_CLOSED, e);
	}
    }

    public RyeResultSet executeQuery() throws SQLException
    {
	if (first_result_type == false) {
	    throw RyeException.createRyeException(jciCon, RyeErrorCode.ER_INVALID_QUERY_TYPE_FOR_EXECUTEQUERY, null);
	}

	execute();

	return result_set;
    }

    public int executeUpdate() throws SQLException
    {
	if (first_result_type == true) {
	    throw RyeException.createRyeException(jciCon, RyeErrorCode.ER_INVALID_QUERY_TYPE_FOR_EXECUTEUPDATE, null);
	}

	execute();

	return update_count;
    }

    public synchronized ResultSetMetaData getMetaData() throws SQLException
    {
	checkIsOpen();

	JciColumnInfo[] col_info = jciStmt.getQueryInfo().getColumnInfo();

	if (col_info == null || col_info.length == 0)
	    return null;

	return new RyeResultSetMetaData(col_info, jciStmt, jciCon);
    }

    public synchronized void clearParameters() throws SQLException
    {
	checkIsOpen();

	bindParameter.clear();
    }

    public synchronized void setNull(int parameterIndex, int sqlType) throws SQLException
    {
	checkIsOpen();

	bindParameter.setParameter(parameterIndex, null, jciCon);
    }

    public synchronized void setNull(int paramIndex, int sqlType, String typeName) throws SQLException
    {
	setNull(paramIndex, sqlType);
    }

    public synchronized void setBoolean(int parameterIndex, boolean value) throws SQLException
    {
	checkIsOpen();

	Integer data = new Integer((value == true) ? 1 : 0);
	bindParameter.setParameter(parameterIndex, data, jciCon);
    }

    public synchronized void setByte(int parameterIndex, byte value) throws SQLException
    {
	checkIsOpen();

	Integer data = new Integer(value);
	bindParameter.setParameter(parameterIndex, data, jciCon);
    }

    public synchronized void setShort(int parameterIndex, short value) throws SQLException
    {
	checkIsOpen();

	Integer data = new Integer(value);
	bindParameter.setParameter(parameterIndex, data, jciCon);
    }

    public synchronized void setInt(int parameterIndex, int value) throws SQLException
    {
	checkIsOpen();

	Integer data = new Integer(value);
	bindParameter.setParameter(parameterIndex, data, jciCon);
    }

    public synchronized void setLong(int parameterIndex, long value) throws SQLException
    {
	checkIsOpen();

	Long data = new Long(value);
	bindParameter.setParameter(parameterIndex, data, jciCon);
    }

    public synchronized void setFloat(int parameterIndex, float value) throws SQLException
    {
	checkIsOpen();

	Double data = new Double(value);
	bindParameter.setParameter(parameterIndex, data, jciCon);
    }

    public synchronized void setDouble(int parameterIndex, double value) throws SQLException
    {
	checkIsOpen();

	Double data = new Double(value);
	bindParameter.setParameter(parameterIndex, data, jciCon);
    }

    public synchronized void setBigDecimal(int parameterIndex, BigDecimal value) throws SQLException
    {
	checkIsOpen();

	bindParameter.setParameter(parameterIndex, value, jciCon);
    }

    public synchronized void setString(int parameterIndex, String value) throws SQLException
    {
	checkIsOpen();

	bindParameter.setParameter(parameterIndex, value, jciCon);
    }

    public synchronized void setBytes(int parameterIndex, byte[] value) throws SQLException
    {
	checkIsOpen();

	bindParameter.setParameter(parameterIndex, value, jciCon);
    }

    public synchronized void setDate(int parameterIndex, Date value) throws SQLException
    {
	checkIsOpen();

	bindParameter.setParameter(parameterIndex, value, jciCon);
    }

    public synchronized void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException
    {
	setDate(parameterIndex, x);
    }

    public synchronized void setTime(int parameterIndex, Time value) throws SQLException
    {
	checkIsOpen();

	bindParameter.setParameter(parameterIndex, value, jciCon);
    }

    public synchronized void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException
    {
	setTime(parameterIndex, x);
    }

    public synchronized void setTimestamp(int parameterIndex, Timestamp value) throws SQLException
    {
	checkIsOpen();

	bindParameter.setParameter(parameterIndex, value, jciCon);
    }

    public synchronized void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException
    {
	setTimestamp(parameterIndex, x);
    }

    public synchronized void setAsciiStream(int parameterIndex, InputStream in, int length) throws SQLException
    {
	checkIsOpen();

	if (in == null || length < 0) {
	    bindParameter.setParameter(parameterIndex, null, jciCon);
	    return;
	}

	try {
	    byte[] value = new byte[length];
	    int len = 0;

	    len = in.read(value);

	    bindParameter.setParameter(parameterIndex, new String(value, 0, len), jciCon);
	} catch (IOException e) {
	    throw con.createRyeException(RyeErrorCode.ER_IOEXCEPTION_IN_STREAM, e);
	}
    }

    public void setUnicodeStream(int parameterIndex, InputStream in, int length) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public synchronized void setBinaryStream(int parameterIndex, InputStream in, int length) throws SQLException
    {
	checkIsOpen();

	if (in == null || length < 0) {
	    bindParameter.setParameter(parameterIndex, null, jciCon);
	    return;
	}

	try {
	    byte[] value = new byte[length];
	    int len = 0;

	    len = in.read(value);

	    byte[] value2 = new byte[len];
	    System.arraycopy(value, 0, value2, 0, len);

	    bindParameter.setParameter(parameterIndex, value2, jciCon);
	} catch (IOException e) {
	    throw con.createRyeException(RyeErrorCode.ER_IOEXCEPTION_IN_STREAM, e);
	}
    }

    public synchronized void setObject(int parameterIndex, Object value, int targetSqlType, int scale)
		    throws SQLException
    {
	checkIsOpen();

	if (value == null) {
	    bindParameter.setParameter(parameterIndex, null, jciCon);
	    return;
	}

	byte type = RyeType.getObjectDBtype(value);
	if (type == RyeType.TYPE_NULL) {
	    throw con.createRyeException(RyeErrorCode.ER_INVALID_OBJECT_BIND, null);
	}

	if (value instanceof Number
			&& (targetSqlType == java.sql.Types.NUMERIC || targetSqlType == java.sql.Types.DECIMAL)) {
	    Number n = (Number) value;
	    bindParameter.setParameter(parameterIndex, new BigDecimal(n.toString()).setScale(scale), jciCon);
	}
	else {
	    bindParameter.setParameter(parameterIndex, value, jciCon);
	}
    }

    public synchronized void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException
    {
	checkIsOpen();

	setObject(parameterIndex, x);
    }

    public synchronized void setObject(int parameterIndex, Object value) throws SQLException
    {
	checkIsOpen();

	if (value == null) {
	    bindParameter.setParameter(parameterIndex, null, jciCon);
	    return;
	}

	byte type = RyeType.getObjectDBtype(value);
	if (type == RyeType.TYPE_NULL) {
	    throw con.createRyeException(RyeErrorCode.ER_INVALID_OBJECT_BIND, null);
	}

	bindParameter.setParameter(parameterIndex, value, jciCon);
    }

    public synchronized void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException
    {
	checkIsOpen();

	if (reader == null || length < 0) {
	    bindParameter.setParameter(parameterIndex, null, jciCon);
	    return;
	}

	try {
	    char[] value = new char[length];
	    int len = 0;

	    len = reader.read(value);
	    bindParameter.setParameter(parameterIndex, new String(value, 0, len), jciCon);
	} catch (IOException e) {
	    throw con.createRyeException(RyeErrorCode.ER_IOEXCEPTION_IN_STREAM, e);
	}
    }

    public void setRef(int i, Ref x) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public void setBlob(int parameterIndex, Blob value) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public void setClob(int parameterIndex, Clob value) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public void setClob(int parameterIndex, Reader reader) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public void setArray(int i, Array x) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public synchronized void setURL(int index, URL x) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public void setNClob(int parameterIndex, NClob value) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public void setNClob(int parameterIndex, Reader reader) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public void setNString(int parameterIndex, String value) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public void setRowId(int parameterIndex, RowId x) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public void close() throws SQLException
    {
	try {
	    synchronized (con) {
		synchronized (this) {
		    if (is_closed)
			return;

		    complete();
		    is_closed = true;

		    if (jciStmt != null) {
			jciStmt.close();
			jciStmt = null;
		    }

		    con.removeStatement(this);
		    con = null;
		    bindParameter.clear();
		}
	    }
	} catch (NullPointerException e) {
	}
    }

    public synchronized ParameterMetaData getParameterMetaData() throws SQLException
    {
	throw new UnsupportedOperationException();
    }

    /*
     * RyePreparedStatement methods
     */

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

	con.autoCommit();
    }

    protected void checkIsOpen() throws SQLException
    {
	if (is_closed) {
	    if (con != null) {
		throw con.createRyeException(RyeErrorCode.ER_PREPARED_STATEMENT_CLOSED, null);
	    }
	    else {
		throw RyeException.createRyeException(jciCon, RyeErrorCode.ER_PREPARED_STATEMENT_CLOSED, null);
	    }

	}
    }

    public byte getStatementType()
    {
	if (jciStmt != null)
	    return (jciStmt.getQueryInfo().getSqlCommandType());
	else
	    return RyeCommandType.unknownStmt();
    }

    public RyeCommandType.SQL_TYPE getSQLType()
    {
	if (jciStmt == null) {
	    return RyeCommandType.SQL_TYPE.UNKNOWN;
	}
	else {
	    return jciStmt.getSqlType();
	}
    }

    public int getParameterCount()
    {
	return parameterCount;
    }
}
