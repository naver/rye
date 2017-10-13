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

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;

import rye.jdbc.jci.JciColumnInfo;

public class RyeResultSet extends RyeResultSetBase
{
    boolean complete_on_close;

    private final RyeConnection con;
    private final RyeStatement stmt;
    private HashMap<String, Integer> col_name_to_index;
    private RyeResultSetMetaData meta_data;

    protected RyeResultSet(RyeConnection c, RyeStatement s, int t, boolean holdable) throws SQLException
    {
	super(t, (holdable ? ResultSet.HOLD_CURSORS_OVER_COMMIT : ResultSet.CLOSE_CURSORS_AT_COMMIT), s
			.getFetchDirection(), s.getFetchSize(), c.getJciConnection());

	this.con = c;
	this.stmt = s;
	col_name_to_index = null;
	complete_on_close = false;
	meta_data = null;
    }

    /*
     * java.sql.ResultSet interface
     */

    public boolean next() throws SQLException
    {
	synchronized (con) {
	    checkIsOpen();

	    if (stmt.jciStmt.cursorNext() == true) {
		current_row++;
		return true;
	    }
	    else {
		current_row = 0;
		if (getType() == ResultSet.TYPE_FORWARD_ONLY) {
		    close();
		}
		return false;
	    }
	}
    }

    public void close() throws SQLException
    {
	synchronized (con) {
	    if (is_closed) {
		return;
	    }
	    is_closed = true;

	    if (stmt.getResultSetHoldability() == ResultSet.HOLD_CURSORS_OVER_COMMIT) {
		stmt.jciStmt.closeCursor();
	    }

	    stmt.jciStmt.clearResult();
	    if (complete_on_close) {
		stmt.complete();
	    }

	    col_name_to_index = null;
	}
    }

    public ResultSetMetaData getMetaData() throws SQLException
    {
	checkIsOpen();

	if (meta_data == null) {
	    meta_data = new RyeResultSetMetaData(stmt.jciStmt.getQueryInfo().getColumnInfo(), stmt.jciStmt,
			    con.getJciConnection());
	}

	return meta_data;
    }

    public int findColumn(String columnName) throws SQLException
    {
	synchronized (con) {
	    checkIsOpen();

	    if (col_name_to_index == null) {
		JciColumnInfo[] colInfo = stmt.jciStmt.getQueryInfo().getColumnInfo();
		col_name_to_index = new HashMap<String, Integer>(colInfo.length);
		for (int i = 0; i < colInfo.length; i++) {
		    col_name_to_index.put(colInfo[i].getColumnLabel().toLowerCase(), i);
		}
	    }

	    Integer index = col_name_to_index.get(columnName.toLowerCase());
	    if (index == null) {
		throw con.createRyeException(RyeErrorCode.ER_INVALID_COLUMN_NAME, null);
	    }

	    return index.intValue() + 1;
	}
    }

    public Statement getStatement() throws SQLException
    {
	checkIsOpen();
	return stmt;
    }

    /*
     * RyeResultSetBase methods
     */

    protected Object getObjectInternal(int columnIndex) throws SQLException
    {
	synchronized (con) {
	    checkIsOpen();

	    was_null = false;

	    Object value = stmt.jciStmt.getObject(columnIndex - 1);

	    if (value == null) {
		was_null = true;
	    }

	    return value;
	}
    }

    protected RyeConnection getConnection()
    {
	return con;
    }
}
