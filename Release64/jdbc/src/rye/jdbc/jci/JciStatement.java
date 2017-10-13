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

import java.util.ArrayList;

import rye.jdbc.driver.RyeException;

abstract public class JciStatement
{
    protected JciStatementQueryInfo queryInfo;

    protected JciStatement(JciStatementQueryInfo queryInfo)
    {
	this.queryInfo = queryInfo;
    }

    public JciStatementQueryInfo getQueryInfo()
    {
	return queryInfo;
    }

    public void setQueryInfo(JciStatementQueryInfo newInfo)
    {
	this.queryInfo = newInfo;
    }

    public boolean isResultsetReturnable()
    {
	return RyeCommandType.hasResultset(queryInfo.getSqlCommandType());
    }

    public RyeCommandType.SQL_TYPE getSqlType()
    {
	return RyeCommandType.getSQLType(queryInfo.getSqlCommandType());
    }

    abstract public void cancel() throws RyeException;

    abstract public void close();

    abstract public void closeCursor();

    abstract public void execute(int groupId, int maxRow, int maxField, int queryTimeout, BindParameter bindParameter)
		    throws RyeException;

    abstract public JciBatchResult executeBatch(int queryTimeout, int groupId, ArrayList<BindParameter> batchParameter)
		    throws RyeException;

    abstract public Object getObject(int index) throws RyeException;

    abstract public int getAffectedRows();

    abstract public boolean cursorNext() throws RyeException;

    abstract public void clearResult();
}
