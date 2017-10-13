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

import java.io.PrintWriter;
import java.sql.SQLException;
import java.util.Properties;

import javax.naming.Reference;
import javax.naming.StringRefAddr;

public class RyeDataSourceBase
{
    public static final String PROP_SERVER_NAME = "serverName";
    public static final String PROP_DATABASE_NAME = "databaseName";
    public static final String PROP_PORT_NUMBER = "portNumber";
    public static final String PROP_URL = "url";
    public static final String PROP_DATASOURCE_NAME = "dataSourceName";
    public static final String PROP_DESCRIPTION = "description";
    public static final String PROP_NETWORK_PROTOCOL = "networkProtocol";
    public static final String PROP_PASSWORD = "password";
    public static final String PROP_ROLE_NAME = "roleName";
    public static final String PROP_USER = "user";
    public static final String PROP_SERVICE_PORT_NAME = "servicePortName";

    public static final String PROP_MAX_STATEMENTS = "maxStatements";
    public static final String PROP_INITIAL_POOL_SIZE = "initialPoolSize";
    public static final String PROP_MIN_POOL_SIZE = "minPoolSize";
    public static final String PROP_MAX_POOL_SIZE = "maxPoolSize";
    public static final String PROP_MAX_IDLE_TIME = "maxIdleTime";
    public static final String PROP_PROPERTY_CYCLE = "propertyCycle";

    // DataSource Standard Properties
    private String databaseName;
    private String dataSourceName;
    private String description;
    private String networkProtocol;
    private String password;
    private int portNumber;
    private String roleName;
    private String serverName;
    private String user;
    private String url;
    private String servicePortName;

    private int loginTimeout;
    private PrintWriter logWriter;

    protected RyeDataSourceBase()
    {
	databaseName = null;
	dataSourceName = null;
	description = null;
	networkProtocol = null;
	password = null;
	portNumber = 0;
	roleName = null;
	serverName = null;
	user = null;
	url = null;

	loginTimeout = 0;
	logWriter = null;
    }

    /*
     * javax.sql.DataSource, javax.sql.ConnectionPoolDataSource,
     */

    public PrintWriter getLogWriter() throws SQLException
    {
	return logWriter;
    }

    public void setLogWriter(PrintWriter out) throws SQLException
    {
	logWriter = out;
    }

    public void setLoginTimeout(int seconds) throws SQLException
    {
	loginTimeout = seconds;
    }

    public int getLoginTimeout() throws SQLException
    {
	return loginTimeout;
    }

    public String getDatabaseName()
    {
	return databaseName;
    }

    public String getDataSourceName()
    {
	return dataSourceName;
    }

    public String getDescription()
    {
	return description;
    }

    public String getNetworkProtocol()
    {
	return networkProtocol;
    }

    public String getPassword()
    {
	return password;
    }

    public int getPortNumber()
    {
	return portNumber;
    }

    public int getPort()
    {
	return getPortNumber();
    }

    public String getRoleName()
    {
	return roleName;
    }

    public String getServerName()
    {
	return serverName;
    }

    public String getUser()
    {
	return user;
    }

    public String getUrl()
    {
	return url;
    }

    public String getURL()
    {
	return getUrl();
    }

    public void setDatabaseName(String dbName)
    {
	databaseName = dbName;
    }

    public void setDataSourceName(String dsName)
    {
	dataSourceName = dsName;
    }

    public void setDescription(String desc)
    {
	description = desc;
    }

    public void setNetworkProtocol(String netProtocol)
    {
	networkProtocol = netProtocol;
    }

    public void setPassword(String passwd)
    {
	if (passwd == null)
	    password = RyeConnectionUrl.default_password;
	else
	    password = passwd;
    }

    public void setPortNumber(int p)
    {
	portNumber = p;
    }

    public void setPortNumber(String value)
    {
	portNumber = getIntValue(value, -1);
    }

    void setPort(int p)
    {
	setPortNumber(p);
    }

    public void setRoleName(String rName)
    {
	roleName = rName;
    }

    public void setServerName(String svName)
    {
	serverName = svName;
    }

    public void setUser(String uName)
    {
	if (user == null)
	    user = RyeConnectionUrl.default_user;
	else
	    user = uName;
    }

    public void setUrl(String urlString)
    {
	url = urlString;
    }

    public void setURL(String urlString)
    {
	setUrl(urlString);
    }

    protected int getIntValue(String value, int defaultValue)
    {
	if (value == null)
	    return defaultValue;
	else
	    return Integer.parseInt(value);
    }

    public String getServicePortName()
    {
	return servicePortName;
    }

    public void setServicePortName(String servicePortName)
    {
	this.servicePortName = servicePortName;
    }

    protected Reference getProperties(Reference ref)
    {
	ref.add(new StringRefAddr(PROP_SERVER_NAME, getServerName()));
	ref.add(new StringRefAddr(PROP_DATABASE_NAME, getDatabaseName()));
	ref.add(new StringRefAddr(PROP_PORT_NUMBER, Integer.toString(getPortNumber())));
	ref.add(new StringRefAddr(PROP_URL, getUrl()));
	ref.add(new StringRefAddr(PROP_DATASOURCE_NAME, getDataSourceName()));
	ref.add(new StringRefAddr(PROP_DESCRIPTION, getDescription()));
	ref.add(new StringRefAddr(PROP_NETWORK_PROTOCOL, getNetworkProtocol()));
	ref.add(new StringRefAddr(PROP_PASSWORD, getPassword()));
	ref.add(new StringRefAddr(PROP_ROLE_NAME, getRoleName()));
	ref.add(new StringRefAddr(PROP_USER, getUser()));
	ref.add(new StringRefAddr(PROP_SERVICE_PORT_NAME, getServicePortName()));

	return ref;
    }

    protected void setProperties(Reference ref)
    {
	setServerName((String) ref.get(PROP_SERVER_NAME).getContent());
	setDatabaseName((String) ref.get(PROP_DATABASE_NAME).getContent());
	setPortNumber((String) ref.get(PROP_PORT_NUMBER).getContent());
	setUrl((String) ref.get(PROP_URL).getContent());
	setDataSourceName((String) ref.get(PROP_DATASOURCE_NAME).getContent());
	setDescription((String) ref.get(PROP_DESCRIPTION).getContent());
	setNetworkProtocol((String) ref.get(PROP_NETWORK_PROTOCOL).getContent());
	setPassword((String) ref.get(PROP_PASSWORD).getContent());
	setRoleName((String) ref.get(PROP_ROLE_NAME).getContent());
	setUser((String) ref.get(PROP_USER).getContent());
	setServicePortName((String) ref.get(PROP_SERVICE_PORT_NAME).getContent());
    }

    protected void setProperties(Properties prop)
    {
	setServerName(prop.getProperty(PROP_SERVER_NAME));
	setDatabaseName(prop.getProperty(PROP_DATABASE_NAME));
	setPortNumber(prop.getProperty(PROP_PORT_NUMBER));
	setUrl(prop.getProperty(PROP_URL));
	setDataSourceName(prop.getProperty(PROP_DATASOURCE_NAME));
	setDescription(prop.getProperty(PROP_DESCRIPTION));
	setNetworkProtocol(prop.getProperty(PROP_NETWORK_PROTOCOL));
	setPassword(prop.getProperty(PROP_PASSWORD));
	setRoleName(prop.getProperty(PROP_ROLE_NAME));
	setUser(prop.getProperty(PROP_USER));
	setServicePortName(prop.getProperty(PROP_SERVICE_PORT_NAME));
    }

    protected void writeLog(String log)
    {
	if (logWriter != null) {
	    java.util.Date dt = new java.util.Date(System.currentTimeMillis());
	    logWriter.println("[" + dt + "] " + log);
	    logWriter.flush();
	}
    }
}
