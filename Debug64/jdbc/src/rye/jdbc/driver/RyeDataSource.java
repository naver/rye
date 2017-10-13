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
import java.util.logging.Logger;

import javax.naming.NamingException;
import javax.naming.Reference;

public class RyeDataSource extends RyeDataSourceBase implements javax.sql.DataSource, javax.naming.Referenceable,
		java.io.Serializable
{
    private static final long serialVersionUID = -1038542340147556509L;

    public RyeDataSource()
    {
	super();
    }

    protected RyeDataSource(Reference ref)
    {
	super();
	setProperties(ref);
    }

    /*
     * javax.sql.DataSource interface
     */

    public Connection getConnection() throws SQLException
    {
	return getConnection(null, null);
    }

    public Connection getConnection(String username, String passwd) throws SQLException
    {
	String dataSourceName = getDataSourceName();
	Connection con;

	if (dataSourceName == null || dataSourceName.length() == 0) {
	    RyeDriver driver = RyeDriver.ryeDriver;

	    if (username == null) {
		username = getUser();
	    }
	    if (passwd == null) {
		passwd = getPassword();
	    }
	    con = driver.connect(getUrl(), username, passwd);

	    writeLog("getConnection(" + username + ")");
	}
	else {
	    con = getConnection(dataSourceName, username, passwd);
	}

	return con;
    }

    public static Connection getConnection(String connPoolDsName, String user, String passwd) throws SQLException
    {
	RyeConnectionPoolDataSource cpds;

	cpds = RyeConnectionPoolManager.getConnectionPoolDataSource(connPoolDsName);

	if (user == null)
	    user = cpds.getUser();
	if (passwd == null)
	    passwd = cpds.getPassword();

	Connection con = RyeConnectionPoolManager.getConnection(cpds, user, passwd);

	return con;
    }

    /*
     * javax.naming.Referenceable interface
     */

    public synchronized Reference getReference() throws NamingException
    {
	Reference ref = new Reference(this.getClass().getName(), "rye.jdbc.driver.RyeDataSourceObjectFactory", null);

	ref = getProperties(ref);
	writeLog("Bind DataSource");
	return ref;
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException
    {
	throw new java.lang.UnsupportedOperationException();
    }

    public <T> T unwrap(Class<T> iface) throws SQLException
    {
	throw new java.lang.UnsupportedOperationException();
    }

    public Logger getParentLogger()
    {
	throw new java.lang.UnsupportedOperationException();
    }
}
