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

import java.io.Serializable;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Logger;

import javax.naming.NamingException;
import javax.naming.Reference;
import javax.naming.Referenceable;
import javax.sql.ConnectionPoolDataSource;
import javax.sql.PooledConnection;

public class RyeConnectionPoolDataSource extends RyePoolDataSourceBase implements ConnectionPoolDataSource,
		Referenceable, Serializable
{
    private static final long serialVersionUID = 3418532804994248021L;

    public RyeConnectionPoolDataSource()
    {
	super();
    }

    protected RyeConnectionPoolDataSource(Reference ref)
    {
	super();
	setProperties(ref);
    }

    protected RyeConnectionPoolDataSource(Properties prop)
    {
	super();
	setProperties(prop);
    }

    /*
     * javax.sql.ConnectionPoolDataSource interface
     */

    public synchronized PooledConnection getPooledConnection() throws SQLException
    {
	return getPooledConnection(null, null);
    }

    public synchronized PooledConnection getPooledConnection(String username, String passwd) throws SQLException
    {
	PooledConnection poolCon;

	RyeConnection c_con = (RyeConnection) RyeDriver.ryeDriver.connect(getUrl(), username, passwd);

	poolCon = new RyePooledConnection(c_con);

	return poolCon;
    }

    /*
     * javax.naming.Referenceable interface
     */

    public synchronized Reference getReference() throws NamingException
    {
	Reference ref = new Reference(this.getClass().getName(), "rye.jdbc.driver.RyeDataSourceObjectFactory", null);

	ref = getProperties(ref);
	return ref;
    }

    /* JDK 1.7 */
    public Logger getParentLogger()
    {
	throw new java.lang.UnsupportedOperationException();
    }

}
