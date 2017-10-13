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
import rye.jdbc.sharding.QueryShardKeyInfo;

public class JciStatementQueryInfo
{
    // variables assigned at prepare time
    private final String sqlText;
    private final byte prepare_flag;
    private final int serverHandleId;
    private final byte sqlCommandType;
    private final int numParameters;
    private final int numColumns;
    private final JciColumnInfo columnInfo[];
    private final QueryShardKeyInfo shardKeyInfo;

    JciStatementQueryInfo(String sqlText, byte prepare_flag, int serverHandleId, byte sqlCommandType,
		    int numParameters, int numColumns, JciColumnInfo columnInfo[], QueryShardKeyInfo shardKeyInfo)
    {
	this.sqlText = sqlText;
	this.prepare_flag = prepare_flag;
	this.serverHandleId = serverHandleId;
	this.sqlCommandType = sqlCommandType;
	this.numParameters = numParameters;
	this.numColumns = numColumns;
	this.columnInfo = columnInfo;
	this.shardKeyInfo = shardKeyInfo;
    }

    public QueryShardKeyInfo getShardKeyInfo()
    {
	return shardKeyInfo;
    }

    public int getParameterCount()
    {
	return numParameters;
    }

    public JciColumnInfo[] getColumnInfo()
    {
	return columnInfo;
    }

    public void updateColumnInfo(int index, byte type, short scale, int precision)
    {
	if (index < columnInfo.length && type != RyeType.TYPE_NULL) {
	    columnInfo[index].setType(type);
	    columnInfo[index].setScale(scale);
	    columnInfo[index].setPrecision(precision);
	}
    }

    public byte getSqlCommandType()
    {
	return sqlCommandType;
    }

    public String getQuery()
    {
	return sqlText;
    }

    public byte getPrepareFlag()
    {
	return prepare_flag;
    }

    int getServerHandleId()
    {
	return serverHandleId;
    }

    public int getColumnCount()
    {
	return numColumns;
    }
}
