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
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Logger;

import rye.jdbc.jci.BrokerHealthCheck;
import rye.jdbc.jci.JciConnection;
import rye.jdbc.jci.JciConnectionInfo;
import rye.jdbc.sharding.ShardInfoManager;

public class RyeDriver implements Driver
{
    // version
    public static final String version_string = "@JDBC_DRIVER_VERSION_STRING@";
    public static final RyeVersion driverVersion;

    static {
	StringTokenizer st = new StringTokenizer(version_string, ".");
	if (st.countTokens() != 4) {
	    throw new RuntimeException("Could not parse version_string: " + version_string);
	}
	short verMajor = Short.parseShort(st.nextToken());
	short verMinor = Short.parseShort(st.nextToken());
	short verPatch = Short.parseShort(st.nextToken());
	short verBuild = Short.parseShort(st.nextToken());
	driverVersion = new RyeVersion(verMajor, verMinor, verPatch, verBuild);
    }

    public static CopyOnWriteArrayList<JciConnectionInfo> unreachableHosts;
    public static String sysCharsetName;

    static RyeDriver ryeDriver = null;

    static {
	sysCharsetName = System.getProperty("file.encoding");
	try {
	    ryeDriver = new RyeDriver();
	    DriverManager.registerDriver(ryeDriver);
	} catch (SQLException e) {
	}
    }

    static {
	unreachableHosts = new CopyOnWriteArrayList<JciConnectionInfo>();

	Thread brokerHealthCheck = new Thread(new BrokerHealthCheck());
	brokerHealthCheck.setDaemon(true);
	brokerHealthCheck.setContextClassLoader(null);
	brokerHealthCheck.start();
    }

    public static boolean isUnreachableHost(JciConnectionInfo conInfo)
    {
	return unreachableHosts.contains(conInfo);
    }

    public static void addToUnreachableHosts(JciConnectionInfo conInfo)
    {
	unreachableHosts.addIfAbsent(conInfo);
    }

    public static void clearShardInfo()
    {
	if (ShardInfoManager.defaultShardInfoManager != null) {
	    ShardInfoManager.defaultShardInfoManager.clear();
	}
    }

    /*
     * java.sql.Driver interface
     */

    public Connection connect(String url, Properties info) throws SQLException
    {
	return connect(url, info.getProperty("user"), info.getProperty("password"));
    }

    public Connection connect(String url, String user, String passwd) throws SQLException
    {
	return connect(url, user, passwd, ShardInfoManager.defaultShardInfoManager);
    }

    public Connection connect(String url, String user, String passwd, ShardInfoManager shardInfoManager)
		    throws SQLException
    {
	RyeConnectionUrl conUrl = RyeConnectionUrl.parseUrl(url, user, passwd);

	JciConnection jciCon = JciConnection.makeJciConnection(conUrl, user, passwd, shardInfoManager);

	return (new RyeConnection(jciCon, url, jciCon.getDbuser()));
    }

    public boolean acceptsURL(String url) throws SQLException
    {
	return RyeConnectionUrl.acceptsURL(url);
    }

    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException
    {
	return new DriverPropertyInfo[0];
    }

    public int getMajorVersion()
    {
	return driverVersion.getMajor();
    }

    public int getMinorVersion()
    {
	return driverVersion.getMinor();
    }

    public boolean jdbcCompliant()
    {
	return true;
    }

    public Logger getParentLogger()
    {
	throw new java.lang.UnsupportedOperationException();
    }

}
