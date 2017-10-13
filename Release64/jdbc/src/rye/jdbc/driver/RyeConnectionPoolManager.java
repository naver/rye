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
import java.sql.SQLException;
import java.util.Hashtable;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import rye.jdbc.jci.JciConnection;

abstract class RyeConnectionPoolManager
{
    private static Hashtable<String, RyeConnectionEventListener> connectionPooltable;
    private static Hashtable<String, RyeConnectionPoolDataSource> poolDataSourceTable;

    static {
	connectionPooltable = new Hashtable<String, RyeConnectionEventListener>();
	poolDataSourceTable = new Hashtable<String, RyeConnectionPoolDataSource>();
    }

    static Connection getConnection(RyeConnectionPoolDataSource pds, String user, String passwd) throws SQLException
    {
	RyeConnectionEventListener cp;

	String key = pds.getDataSourceName();

	synchronized (connectionPooltable) {
	    cp = connectionPooltable.get(key);

	    if (cp == null) {
		cp = addConnectionPool(key, pds);
	    }
	}

	return cp.getConnection(user, passwd);
    }

    static RyeConnectionPoolDataSource getConnectionPoolDataSource(String dsName) throws SQLException
    {
	RyeConnectionPoolDataSource cpds;

	synchronized (poolDataSourceTable) {
	    cpds = poolDataSourceTable.get(dsName);

	    if (cpds == null) {
		try {
		    Context ctx = new InitialContext();
		    cpds = (RyeConnectionPoolDataSource) ctx.lookup(dsName);
		} catch (NamingException e) {
		    throw RyeException.createRyeException((JciConnection) null, RyeErrorCode.ER_UNKNOWN, e.toString(), e);
		}

		if (cpds != null) {
		    poolDataSourceTable.put(dsName, cpds);
		}
	    }
	}

	return cpds;
    }

    static private RyeConnectionEventListener addConnectionPool(String key, RyeConnectionPoolDataSource pds)
    {
	RyeConnectionEventListener cp = new RyeConnectionEventListener(pds);
	connectionPooltable.put(key, cp);
	return cp;
    }

    static void addConnectionPoolDataSource(String key, RyeConnectionPoolDataSource pds)
    {
	synchronized (poolDataSourceTable) {
	    if (poolDataSourceTable.get(key) == null) {
		poolDataSourceTable.put(key, pds);
	    }
	}
    }
}
