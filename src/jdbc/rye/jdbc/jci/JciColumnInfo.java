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

import rye.jdbc.driver.RyeType;

public class JciColumnInfo
{
    private byte type;
    private short scale;
    private int precision;
    private final String columnLabel; /* display name, label */
    private final String tableName; /* column's table name */
    private final String columnName; /* db column name. not support */
    private final boolean isNotNull;

    private final String defaultValue;
    private final boolean isUniqueKey;
    private final boolean isPrimaryKey;

    JciColumnInfo(byte cType, short cScale, int cPrecision, String label, String columnName, String tableName,
		    boolean isNotNull, String defValue, boolean isUnique, boolean isPk) throws JciException
    {
	this.type = cType;
	this.scale = cScale;
	this.precision = cPrecision;
	this.columnLabel = label;

	this.columnName = columnName;
	this.tableName = tableName;
	this.isNotNull = isNotNull;

	this.defaultValue = defValue;
	this.isUniqueKey = isUnique;
	this.isPrimaryKey = isPk;
    }

    /* get functions */
    public String getDefaultValue()
    {
	return defaultValue;
    }

    public boolean isUniqueKey()
    {
	return isUniqueKey;
    }

    public boolean isPrimaryKey()
    {
	return isPrimaryKey;
    }

    public boolean isNotNull()
    {
	return isNotNull;
    }

    public String getTableName()
    {
	return tableName;
    }

    public String getColumnLabel()
    {
	return columnLabel;
    }

    public int getColumnPrecision()
    {
	return precision;
    }

    public int getColumnScale()
    {
	return (int) scale;
    }

    public byte getColumnType()
    {
	return type;
    }

    public String getFQDN()
    {
	return RyeType.getJavaClassName(type);
    }

    public String getRealColumnName()
    {
	return columnName;
    }

    void setType(byte type)
    {
	this.type = type;
    }

    void setScale(short scale)
    {
	this.scale = scale;
    }
    
    void setPrecision(int precision)
    {
	this.precision = precision;
    }
}
