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

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Calendar;
import java.util.Map;

import rye.jdbc.jci.JciConnection;

abstract class RyeResultSetBase implements ResultSet
{
    protected boolean is_closed;
    protected boolean was_null;
    protected int current_row;
    private final int resultsetType;
    private final int holdability;
    private final int fetch_direction;
    private final int fetch_size;
    protected final JciConnection jciCon;

    protected RyeResultSetBase(int type, int holdability, int fetch_direction, int fetch_size, JciConnection jciCon)
    {
	is_closed = false;
	was_null = false;
	current_row = 0;
	this.resultsetType = type;
	this.holdability = holdability;
	this.fetch_direction = fetch_direction;
	this.fetch_size = fetch_size;
	this.jciCon = jciCon;
    }

    /*
     * RyeResultSetBase methods
     */

    abstract protected Object getObjectInternal(int columnIndex) throws SQLException;

    abstract protected RyeConnection getConnection();

    protected void checkIsOpen() throws SQLException
    {
	if (is_closed) {
	    throw makeException(RyeErrorCode.ER_RESULTSTE_CLOSED);
	}
    }

    private RyeException makeException(int err)
    {
	RyeConnection con = getConnection();
	if (con == null) {
	    return RyeException.createRyeException(jciCon, err, null);
	}
	else {
	    return con.createRyeException(err, null);
	}
    }

    /*
     * java.sql.ResultSet interface
     */

    abstract public boolean next() throws SQLException;

    abstract public void close() throws SQLException;

    abstract public ResultSetMetaData getMetaData() throws SQLException;

    abstract public int findColumn(String columnName) throws SQLException;

    abstract public Statement getStatement() throws SQLException;

    public boolean isClosed() throws SQLException
    {
	return is_closed;
    }

    public boolean wasNull() throws SQLException
    {
	checkIsOpen();
	return was_null;
    }

    public SQLWarning getWarnings() throws SQLException
    {
	return null;
    }

    public void clearWarnings() throws SQLException
    {
    }

    public String getCursorName() throws SQLException
    {
	checkIsOpen();
	return "";
    }

    public Object getObject(int columnIndex) throws SQLException
    {
	Object value = getObjectInternal(columnIndex);
	return RyeType.getObject(value);
    }

    public String getString(int columnIndex) throws SQLException
    {
	Object value = getObjectInternal(columnIndex);
	return RyeType.getString(value, jciCon);
    }

    public boolean getBoolean(int columnIndex) throws SQLException
    {
	Object value = getObjectInternal(columnIndex);
	return RyeType.getBoolean(value, jciCon);
    }

    public byte getByte(int columnIndex) throws SQLException
    {
	Object value = getObjectInternal(columnIndex);
	return RyeType.getByte(value, jciCon);
    }

    public short getShort(int columnIndex) throws SQLException
    {
	Object value = getObjectInternal(columnIndex);
	return RyeType.getShort(value, jciCon);
    }

    public int getInt(int columnIndex) throws SQLException
    {
	Object value = getObjectInternal(columnIndex);
	return RyeType.getInt(value, jciCon);
    }

    public long getLong(int columnIndex) throws SQLException
    {
	Object value = getObjectInternal(columnIndex);
	return RyeType.getLong(value, jciCon);
    }

    public float getFloat(int columnIndex) throws SQLException
    {
	Object value = getObjectInternal(columnIndex);
	return RyeType.getFloat(value, jciCon);
    }

    public double getDouble(int columnIndex) throws SQLException
    {
	Object value = getObjectInternal(columnIndex);
	return RyeType.getDouble(value, jciCon);
    }

    public byte[] getBytes(int columnIndex) throws SQLException
    {
	Object value = getObjectInternal(columnIndex);
	return RyeType.getBytes(value, jciCon);
    }

    public Date getDate(int columnIndex) throws SQLException
    {
	Object value = getObjectInternal(columnIndex);
	return RyeType.getDate(value, jciCon);
    }

    public Time getTime(int columnIndex) throws SQLException
    {
	Object value = getObjectInternal(columnIndex);
	return RyeType.getTime(value, jciCon);
    }

    public Timestamp getTimestamp(int columnIndex) throws SQLException
    {
	Object value = getObjectInternal(columnIndex);
	return RyeType.getTimestamp(value, jciCon);
    }

    public InputStream getAsciiStream(int columnIndex) throws SQLException
    {
	Object value = getObjectInternal(columnIndex);

	if (value == null) {
	    return null;
	}

	if (value instanceof String) {
	    ByteArrayInputStream stream = new ByteArrayInputStream(((String) value).getBytes());
	    return stream;
	}
	else {
	    throw RyeException.createRyeException(jciCon, RyeErrorCode.ER_CONVERSION_ERROR, null);
	}
    }

    public InputStream getBinaryStream(int columnIndex) throws SQLException
    {
	Object value = getObjectInternal(columnIndex);

	if (value == null) {
	    return null;
	}

	if (value instanceof byte[]) {
	    ByteArrayInputStream stream = new ByteArrayInputStream((byte[]) value);
	    return stream;
	}
	else {
	    throw RyeException.createRyeException(jciCon, RyeErrorCode.ER_CONVERSION_ERROR, null);
	}
    }

    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public InputStream getUnicodeStream(int columnIndex) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public String getString(String columnName) throws SQLException
    {
	return getString(findColumn(columnName));
    }

    public boolean getBoolean(String columnName) throws SQLException
    {
	return getBoolean(findColumn(columnName));
    }

    public byte getByte(String columnName) throws SQLException
    {
	return getByte(findColumn(columnName));
    }

    public short getShort(String columnName) throws SQLException
    {
	return getShort(findColumn(columnName));
    }

    public int getInt(String columnName) throws SQLException
    {
	return getInt(findColumn(columnName));
    }

    public long getLong(String columnName) throws SQLException
    {
	return getLong(findColumn(columnName));
    }

    public float getFloat(String columnName) throws SQLException
    {
	return getFloat(findColumn(columnName));
    }

    public double getDouble(String columnName) throws SQLException
    {
	return getDouble(findColumn(columnName));
    }

    public BigDecimal getBigDecimal(String columnName, int scale) throws SQLException
    {
	return getBigDecimal(findColumn(columnName), scale);
    }

    public byte[] getBytes(String columnName) throws SQLException
    {
	return getBytes(findColumn(columnName));
    }

    public Date getDate(String columnName) throws SQLException
    {
	return getDate(findColumn(columnName));
    }

    public Time getTime(String columnName) throws SQLException
    {
	return getTime(findColumn(columnName));
    }

    public Timestamp getTimestamp(String columnName) throws SQLException
    {
	return getTimestamp(findColumn(columnName));
    }

    public InputStream getAsciiStream(String columnName) throws SQLException
    {
	return getAsciiStream(findColumn(columnName));
    }

    public InputStream getUnicodeStream(String columnName) throws SQLException
    {
	return getUnicodeStream(findColumn(columnName));
    }

    public InputStream getBinaryStream(String columnName) throws SQLException
    {
	return getBinaryStream(findColumn(columnName));
    }

    public Object getObject(String columnName) throws SQLException
    {
	return getObject(findColumn(columnName));
    }

    public Reader getCharacterStream(int columnIndex) throws SQLException
    {
	Object value = getObjectInternal(columnIndex);

	if (value == null) {
	    return null;
	}

	if (value instanceof String) {
	    return new StringReader((String) value);
	}

	throw RyeException.createRyeException(jciCon, RyeErrorCode.ER_CONVERSION_ERROR, null);
    }

    public Reader getCharacterStream(String columnName) throws SQLException
    {
	return getCharacterStream(findColumn(columnName));
    }

    public BigDecimal getBigDecimal(int columnIndex) throws SQLException
    {
	Object value = getObjectInternal(columnIndex);
	return RyeType.getBigDecimal(value, jciCon);
    }

    public BigDecimal getBigDecimal(String columnName) throws SQLException
    {
	return getBigDecimal(findColumn(columnName));
    }

    public boolean isBeforeFirst() throws SQLException
    {
	throw makeException(RyeErrorCode.ER_NON_SCROLLABLE);
    }

    public boolean isAfterLast() throws SQLException
    {
	throw makeException(RyeErrorCode.ER_NON_SCROLLABLE);
    }

    public boolean isFirst() throws SQLException
    {
	throw makeException(RyeErrorCode.ER_NON_SCROLLABLE);
    }

    public boolean isLast() throws SQLException
    {
	throw makeException(RyeErrorCode.ER_NON_SCROLLABLE);
    }

    public void beforeFirst() throws SQLException
    {
	throw makeException(RyeErrorCode.ER_NON_SCROLLABLE);
    }

    public void afterLast() throws SQLException
    {
	throw makeException(RyeErrorCode.ER_NON_SCROLLABLE);
    }

    public boolean first() throws SQLException
    {
	throw makeException(RyeErrorCode.ER_NON_SCROLLABLE);
    }

    public boolean last() throws SQLException
    {
	throw makeException(RyeErrorCode.ER_NON_SCROLLABLE);
    }

    public int getRow() throws SQLException
    {
	checkIsOpen();
	return current_row + 1;
    }

    public boolean absolute(int row) throws SQLException
    {
	throw makeException(RyeErrorCode.ER_NON_SCROLLABLE);
    }

    public boolean relative(int rows) throws SQLException
    {
	throw makeException(RyeErrorCode.ER_NON_SCROLLABLE);
    }

    public boolean previous() throws SQLException
    {
	throw makeException(RyeErrorCode.ER_NON_SCROLLABLE);
    }

    public void setFetchDirection(int direction) throws SQLException
    {
	throw makeException(RyeErrorCode.ER_NON_SCROLLABLE);
    }

    public int getFetchDirection() throws SQLException
    {
	checkIsOpen();
	return fetch_direction;
    }

    public void setFetchSize(int rows) throws SQLException
    {
	checkIsOpen();
	// do not support setFetchSize
    }

    public int getFetchSize() throws SQLException
    {
	checkIsOpen();
	return fetch_size;
    }

    public int getType() throws SQLException
    {
	checkIsOpen();
	return resultsetType;
    }

    public int getConcurrency() throws SQLException
    {
	checkIsOpen();
	return CONCUR_READ_ONLY;
    }

    public boolean rowUpdated() throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public boolean rowInserted() throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public boolean rowDeleted() throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public void updateNull(int columnIndex) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public void updateBoolean(int columnIndex, boolean x) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public void updateByte(int columnIndex, byte x) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public void updateShort(int columnIndex, short x) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public void updateInt(int columnIndex, int x) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public void updateLong(int columnIndex, long x) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public void updateFloat(int columnIndex, float x) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public void updateDouble(int columnIndex, double x) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public void updateString(int columnIndex, String x) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public void updateBytes(int columnIndex, byte[] x) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public void updateDate(int columnIndex, Date x) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public void updateTime(int columnIndex, Time x) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public void updateObject(int columnIndex, Object x, int scale) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public void updateObject(int columnIndex, Object x) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public void updateNull(String columnName) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public void updateBoolean(String columnName, boolean x) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public void updateByte(String columnName, byte x) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public void updateShort(String columnName, short x) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public void updateInt(String columnName, int x) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public void updateLong(String columnName, long x) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public void updateFloat(String columnName, float x) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public void updateDouble(String columnName, double x) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public void updateBigDecimal(String columnName, BigDecimal x) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public void updateString(String columnName, String x) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public void updateBytes(String columnName, byte[] x) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public void updateDate(String columnName, Date x) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public void updateTime(String columnName, Time x) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public void updateTimestamp(String columnName, Timestamp x) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public void updateAsciiStream(String columnName, InputStream x, int length) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public void updateBinaryStream(String columnName, InputStream x, int length) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public void updateCharacterStream(String columnName, Reader reader, int length) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public void updateObject(String columnName, Object x, int scale) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public void updateObject(String columnName, Object x) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public void insertRow() throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public void updateRow() throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public void deleteRow() throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public void refreshRow() throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public void cancelRowUpdates() throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public void moveToInsertRow() throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public void moveToCurrentRow() throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public Object getObject(int i, Map<String, Class<?>> map) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public Ref getRef(int i) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public Blob getBlob(int columnIndex) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public Clob getClob(int columnIndex) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public Array getArray(int i) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public Object getObject(String colName, Map<String, Class<?>> map) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public Ref getRef(String colName) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public Blob getBlob(String colName) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public Clob getClob(String colName) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public Array getArray(String colName) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public Date getDate(int columnIndex, Calendar cal) throws SQLException
    {
	return getDate(columnIndex);
    }

    public Date getDate(String columnName, Calendar cal) throws SQLException
    {
	return getDate(columnName);
    }

    public Time getTime(int columnIndex, Calendar cal) throws SQLException
    {
	return getTime(columnIndex);
    }

    public Time getTime(String columnName, Calendar cal) throws SQLException
    {
	return getTime(columnName);
    }

    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException
    {
	return getTimestamp(columnIndex);
    }

    public Timestamp getTimestamp(String columnName, Calendar cal) throws SQLException
    {
	return getTimestamp(columnName);
    }

    public URL getURL(int columnIndex) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public URL getURL(String columnName) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public void updateArray(int columnIndex, Array x) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public void updateArray(String columnName, Array x) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public void updateBlob(int columnIndex, Blob x) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public void updateBlob(String columnName, Blob x) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public void updateClob(int columnIndex, Clob x) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public void updateClob(String columnName, Clob x) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public void updateRef(int columnIndex, Ref x) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public void updateRef(String columnName, Ref x) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public int getHoldability() throws SQLException
    {
	checkIsOpen();

	return holdability;
    }

    public Reader getNCharacterStream(int columnIndex) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public Reader getNCharacterStream(String columnLabel) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public NClob getNClob(int columnIndex) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public NClob getNClob(String columnLabel) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public String getNString(int columnIndex) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public String getNString(String columnLabel) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public RowId getRowId(int columnIndex) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public RowId getRowId(String columnLabel) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public SQLXML getSQLXML(int columnIndex) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public SQLXML getSQLXML(String columnLabel) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public void updateClob(int columnIndex, Reader reader) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public void updateClob(String columnLabel, Reader reader) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public void updateNClob(int columnIndex, NClob clob) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public void updateNClob(String columnLabel, NClob clob) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public void updateNClob(int columnIndex, Reader reader) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public void updateNClob(String columnLabel, Reader reader) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public void updateNString(int columnIndex, String string) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public void updateNString(String columnLabel, String string) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public void updateRowId(int columnIndex, RowId x) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public void updateRowId(String columnLabel, RowId x) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public <T> T unwrap(Class<T> iface) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }

    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException
    {
	throw new SQLFeatureNotSupportedException();
    }
}
