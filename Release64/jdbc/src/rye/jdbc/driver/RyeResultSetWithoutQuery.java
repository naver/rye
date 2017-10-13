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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;

import rye.jdbc.jci.JciConnection;
import rye.jdbc.jci.JciResultTuple;

class RyeResultSetWithoutQuery extends RyeResultSetBase
{
    class TupleComparator implements Comparator<JciResultTuple>
    {
	private final int[] sortKey;

	TupleComparator(int[] sortKey)
	{
	    this.sortKey = sortKey;
	}

	/*
	 * java.util.Comparator interface
	 */

	public int compare(JciResultTuple o1, JciResultTuple o2)
	{
	    for (int i = 0; i < sortKey.length; i++) {
		int res = compareValue(o1.getValue(sortKey[i]), o2.getValue(sortKey[i]));
		if (res != 0) {
		    return res;
		}
	    }
	    return 0;
	}

	private int compareValue(Object o1, Object o2)
	{
	    if (o1 == null && o2 == null) {
		return 0;
	    }
	    if (o1 == null) {
		return -1;
	    }
	    if (o2 == null) {
		return 1;
	    }

	    if (o1 instanceof String) {
		return ((String) o1).compareTo((String) o2);
	    }
	    else if (o1 instanceof Integer) {
		return ((Integer) o1).compareTo((Integer) o2);
	    }
	    else if (o1 instanceof Short) {
		return ((Short) o1).compareTo((Short) o2);
	    }
	    else if (o1 instanceof Boolean) {
		boolean b1 = ((Boolean) o1).booleanValue();
		boolean b2 = ((Boolean) o2).booleanValue();

		if (b1 == b2) {
		    return 0;
		}

		if (b1 == true) {
		    return 1;
		}
		else {
		    return -1;
		}
	    }
	    else {
		return 0;
	    }
	}

    }

    final int[] type;
    final int[] precision;
    final boolean[] nullable;
    final String[] column_name;

    private final int num_of_columns;
    private final int num_of_rows;
    private final JciResultTuple[] rows;
    private RyeResultSetMetaData meta_data;

    protected RyeResultSetWithoutQuery(int[] types, String[] colnames, boolean[] isnull, int[] precision,
		    ArrayList<JciResultTuple> tupleList, int[] sortKey, JciConnection jciCon)
    {
	super(ResultSet.TYPE_FORWARD_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT, ResultSet.FETCH_FORWARD, 0, jciCon);

	num_of_columns = types.length;

	if (precision == null) {
	    precision = new int[num_of_columns];
	    Arrays.fill(precision, (int) 0);
	}

	this.type = types;
	this.precision = precision;
	this.nullable = isnull;
	this.column_name = colnames;

	if (tupleList == null) {
	    this.num_of_rows = 0;
	    this.rows = null;
	}
	else {
	    this.num_of_rows = tupleList.size();

	    this.rows = new JciResultTuple[tupleList.size()];
	    tupleList.toArray(this.rows);

	    if (sortKey != null) {
		Arrays.sort(rows, new TupleComparator(sortKey));
	    }
	}
	this.meta_data = null;
    }

    protected RyeResultSetWithoutQuery(int[] types, String[] colnames, boolean[] isnull, JciConnection jciCon)
    {
	this(types, colnames, isnull, null, null, null, jciCon);
    }

    /*
     * java.sql.ResultSet interface
     */

    public synchronized boolean next() throws SQLException
    {
	checkIsOpen();
	current_row++;

	if (current_row > num_of_rows) {
	    close();
	    current_row = 0;
	    return false;
	}

	return true;
    }

    public synchronized void close() throws SQLException
    {
	if (is_closed)
	    return;
	is_closed = true;
    }

    public synchronized ResultSetMetaData getMetaData() throws SQLException
    {
	checkIsOpen();
	if (meta_data == null) {
	    meta_data = new RyeResultSetMetaData(this, jciCon);
	}
	return meta_data;
    }

    public synchronized int findColumn(String columnName) throws SQLException
    {
	checkIsOpen();
	if (columnName == null) {
	    throw new IllegalArgumentException();
	}
	int i;
	for (i = 0; i < this.column_name.length; i++) {
	    if (column_name[i].equalsIgnoreCase(columnName))
		return i + 1;
	}
	throw RyeException.createRyeException(jciCon, RyeErrorCode.ER_INVALID_COLUMN_NAME, null);
    }

    public synchronized Statement getStatement() throws SQLException
    {
	checkIsOpen();
	return null;
    }

    /*
     * RyeResultSetBase mthods
     */

    protected RyeConnection getConnection()
    {
	return null;
    }

    protected Object getObjectInternal(int columnIndex) throws SQLException
    {
	if (current_row < 1 || current_row > num_of_rows) {
	    throw RyeException.createRyeException(jciCon, RyeErrorCode.ER_INVALID_ROW, null);
	}

	if (columnIndex < 1 || columnIndex > num_of_columns) {
	    throw RyeException.createRyeException(jciCon, RyeErrorCode.ER_INVALID_INDEX, null);
	}

	Object value = rows[current_row - 1].getValue(columnIndex - 1);

	was_null = (value == null);

	return value;
    }
}
