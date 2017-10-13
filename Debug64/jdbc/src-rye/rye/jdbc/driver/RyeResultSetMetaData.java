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

import java.math.BigDecimal;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import rye.jdbc.jci.JciColumnInfo;
import rye.jdbc.jci.JciConnection;
import rye.jdbc.jci.JciStatement;

public class RyeResultSetMetaData implements ResultSetMetaData
{
    private String[] col_name;
    private int[] col_type;
    private String[] col_type_name;
    private int[] col_prec;
    private int[] col_disp_size;
    private int[] col_scale;
    private String[] col_table;
    private int[] col_null;
    private String[] col_class_name;
    private boolean[] is_auto_increment_col;
    private final JciConnection jciCon;

    protected RyeResultSetMetaData(JciColumnInfo[] col_info, JciStatement jcistmt, JciConnection jciCon)
    {
	this.jciCon = jciCon;
	col_name = new String[col_info.length];
	col_type = new int[col_info.length];
	col_type_name = new String[col_info.length];
	col_prec = new int[col_info.length];
	col_disp_size = new int[col_info.length];
	col_scale = new int[col_info.length];
	col_table = new String[col_info.length];
	col_null = new int[col_info.length];
	col_class_name = new String[col_info.length];
	is_auto_increment_col = new boolean[col_info.length];

	for (int i = 0; i < col_info.length; i++) {
	    col_disp_size[i] = getDefaultColumnDisplaySize(col_info[i].getColumnType());
	    col_name[i] = col_info[i].getColumnLabel();
	    col_prec[i] = col_info[i].getColumnPrecision();
	    col_scale[i] = col_info[i].getColumnScale();
	    col_table[i] = col_info[i].getTableName();
	    col_type_name[i] = null;
	    col_class_name[i] = col_info[i].getFQDN();
	    if (col_info[i].isNotNull())
		col_null[i] = columnNoNulls;
	    else
		col_null[i] = columnNullable;

	    is_auto_increment_col[i] = false;

	    int dbType = col_info[i].getColumnType();
	    if (dbType == RyeType.TYPE_NULL) {
		Object o = getValueFromTuple(jcistmt, i);
		if (o != null) {
		    dbType = RyeType.getObjectDBtype(o);
		    if (dbType == RyeType.TYPE_VARCHAR) {
			col_prec[i] = RyeType.VARCHAR_MAX_PRECISION;
		    }
		    else if (dbType == RyeType.TYPE_BINARY) {
			col_prec[i] = RyeType.BINARY_MAX_PRECISION;
		    }
		    else if (dbType == RyeType.TYPE_NUMERIC) {
			col_prec[i] = RyeType.NUMERIC_MAX_PRECISION;
			if (o instanceof BigDecimal) {
			    col_scale[i] = ((BigDecimal) o).scale();
			    if (col_scale[i] < 0) {
				col_scale[i] = 0;
			    }
			}
		    }
		}
	    }

	    switch (dbType)
	    {
	    case RyeType.TYPE_VARCHAR:
		col_type_name[i] = "VARCHAR";
		col_type[i] = java.sql.Types.VARCHAR;
		if (col_prec[i] > col_disp_size[i]) {
		    col_disp_size[i] = col_prec[i];
		}
		break;

	    case RyeType.TYPE_BINARY:
		col_type_name[i] = "BIT VARYING";
		col_type[i] = java.sql.Types.VARBINARY;
		if (col_prec[i] > col_disp_size[i]) {
		    col_disp_size[i] = col_prec[i];
		}
		break;

	    case RyeType.TYPE_INT:
		col_type_name[i] = "INTEGER";
		col_type[i] = java.sql.Types.INTEGER;
		break;

	    case RyeType.TYPE_BIGINT:
		col_type_name[i] = "BIGINT";
		col_type[i] = java.sql.Types.BIGINT;
		break;

	    case RyeType.TYPE_DOUBLE:
		col_type_name[i] = "DOUBLE";
		col_type[i] = java.sql.Types.DOUBLE;
		break;

	    case RyeType.TYPE_NUMERIC:
		col_type_name[i] = "NUMERIC";
		col_type[i] = java.sql.Types.NUMERIC;
		break;

	    case RyeType.TYPE_DATE:
		col_type_name[i] = "DATE";
		col_type[i] = java.sql.Types.DATE;
		break;

	    case RyeType.TYPE_TIME:
		col_type_name[i] = "TIME";
		col_type[i] = java.sql.Types.TIME;
		break;

	    case RyeType.TYPE_DATETIME:
		col_type_name[i] = "DATETIME";
		col_type[i] = java.sql.Types.TIMESTAMP;
		break;

	    case RyeType.TYPE_NULL:
		col_type_name[i] = "";
		col_type[i] = java.sql.Types.NULL;
		//col_type[i] = java.sql.Types.OTHER;
		break;

	    default:
		break;
	    }
	}
    }

    RyeResultSetMetaData(RyeResultSetWithoutQuery r, JciConnection jciCon)
    {
	this.jciCon = jciCon;
	col_name = r.column_name;
	col_type = new int[col_name.length];
	col_type_name = new String[col_name.length];
	col_prec = new int[col_name.length];
	col_disp_size = new int[col_name.length];
	col_scale = new int[col_name.length];
	col_table = new String[col_name.length];
	col_null = new int[col_name.length];
	col_class_name = new String[col_name.length];
	is_auto_increment_col = new boolean[col_name.length];

	for (int i = 0; i < col_name.length; i++) {
	    col_disp_size[i] = getDefaultColumnDisplaySize((byte) r.type[i]);
	    if (r.type[i] == RyeType.TYPE_INT) {
		col_type[i] = java.sql.Types.INTEGER;
		col_type_name[i] = "INTEGER";
		col_prec[i] = 10;
		col_class_name[i] = "java.lang.Integer";
	    }
	    if (r.type[i] == RyeType.TYPE_VARCHAR) {
		col_type[i] = java.sql.Types.VARCHAR;
		col_type_name[i] = "VARCHAR";
		col_prec[i] = r.precision[i];
		if (col_prec[i] > col_disp_size[i]) {
		    col_disp_size[i] = col_prec[i];
		}
		col_class_name[i] = "java.lang.String";
	    }
	    if (r.type[i] == RyeType.TYPE_NULL) {
		col_type[i] = java.sql.Types.NULL;
		col_type_name[i] = "";
		col_prec[i] = 0;
		col_class_name[i] = "";
	    }
	    col_scale[i] = 0;
	    col_table[i] = "";
	    if (r.nullable[i]) {
		col_null[i] = columnNullable;
	    }
	    else {
		col_null[i] = columnNoNulls;
	    }
	    is_auto_increment_col[i] = false;
	}
    }

    private Object getValueFromTuple(JciStatement jcistmt, int index)
    {
	return null;
    }

    private int getDefaultColumnDisplaySize(byte type)
    {
	/* return default column display size based on column type */
	int ret_size = -1;

	switch (type)
	{
	case RyeType.TYPE_VARCHAR:
	    ret_size = 1;
	    break;
	case RyeType.TYPE_BINARY:
	    ret_size = 1;
	    break;
	case RyeType.TYPE_INT:
	    ret_size = 11;
	    break;
	case RyeType.TYPE_BIGINT:
	    ret_size = 20;
	    break;
	case RyeType.TYPE_DOUBLE:
	    ret_size = 23;
	    break;
	case RyeType.TYPE_NUMERIC:
	    ret_size = 40;
	    break;
	case RyeType.TYPE_DATE:
	    ret_size = 10;
	    break;
	case RyeType.TYPE_TIME:
	    ret_size = 8;
	    break;
	case RyeType.TYPE_DATETIME:
	    ret_size = 23;
	    break;
	case RyeType.TYPE_NULL:
	    ret_size = 4;
	    break;
	default:
	    break;
	}
	return ret_size;
    }

    /*
     * java.sql.ResultSetMetaData interface
     */

    public int getColumnCount() throws SQLException
    {
	return col_name.length;
    }

    public boolean isAutoIncrement(int column) throws SQLException
    {
	checkColumnIndex(column);

	return is_auto_increment_col[column - 1];
    }

    public boolean isCaseSensitive(int column) throws SQLException
    {
	checkColumnIndex(column);

	if (col_type[column - 1] == java.sql.Types.CHAR || col_type[column - 1] == java.sql.Types.VARCHAR
			|| col_type[column - 1] == java.sql.Types.LONGVARCHAR) {
	    return true;
	}

	return false;
    }

    public boolean isSearchable(int column) throws SQLException
    {
	checkColumnIndex(column);
	return true;
    }

    public boolean isCurrency(int column) throws SQLException
    {
	checkColumnIndex(column);

	return false;
    }

    public int isNullable(int column) throws SQLException
    {
	checkColumnIndex(column);
	return col_null[column - 1];
    }

    public boolean isSigned(int column) throws SQLException
    {
	checkColumnIndex(column);

	if (col_type[column - 1] == java.sql.Types.SMALLINT || col_type[column - 1] == java.sql.Types.INTEGER
			|| col_type[column - 1] == java.sql.Types.NUMERIC
			|| col_type[column - 1] == java.sql.Types.DECIMAL
			|| col_type[column - 1] == java.sql.Types.REAL || col_type[column - 1] == java.sql.Types.DOUBLE) {
	    return true;
	}

	return false;
    }

    public int getColumnDisplaySize(int column) throws SQLException
    {
	checkColumnIndex(column);
	return col_disp_size[column - 1];
    }

    public String getColumnLabel(int column) throws SQLException
    {
	checkColumnIndex(column);
	return getColumnName(column);
    }

    public String getColumnName(int column) throws SQLException
    {
	checkColumnIndex(column);
	return col_name[column - 1];
    }

    public String getSchemaName(int column) throws SQLException
    {
	checkColumnIndex(column);
	return "";
    }

    public int getPrecision(int column) throws SQLException
    {
	checkColumnIndex(column);
	return col_prec[column - 1];
    }

    public int getScale(int column) throws SQLException
    {
	checkColumnIndex(column);
	return col_scale[column - 1];
    }

    public String getTableName(int column) throws SQLException
    {
	checkColumnIndex(column);
	return col_table[column - 1];
    }

    public String getCatalogName(int column) throws SQLException
    {
	checkColumnIndex(column);
	return "";
    }

    public int getColumnType(int column) throws SQLException
    {
	checkColumnIndex(column);
	return col_type[column - 1];
    }

    public String getColumnTypeName(int column) throws SQLException
    {
	checkColumnIndex(column);
	return col_type_name[column - 1];
    }

    public boolean isReadOnly(int column) throws SQLException
    {
	checkColumnIndex(column);
	return false;
    }

    public boolean isWritable(int column) throws SQLException
    {
	checkColumnIndex(column);
	return true;
    }

    public boolean isDefinitelyWritable(int column) throws SQLException
    {
	checkColumnIndex(column);
	return false;
    }

    public String getColumnClassName(int column) throws SQLException
    {
	checkColumnIndex(column);
	return col_class_name[column - 1];
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException
    {
	throw new java.lang.UnsupportedOperationException();
    }

    public <T> T unwrap(Class<T> iface) throws SQLException
    {
	throw new java.lang.UnsupportedOperationException();
    }

    private void checkColumnIndex(int column) throws SQLException
    {
	if (column < 1 || column > col_name.length) {
	    throw RyeException.createRyeException(jciCon, RyeErrorCode.ER_INVALID_INDEX, null);
	}
    }
}
