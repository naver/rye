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

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import rye.jdbc.jci.JciConnectionInfo;

public class RyeConnectionUrl
{
    private final static String RYE_JDBC_URL_HEADER = "jdbc:rye://";
    private final static String URL_PATTERN = RYE_JDBC_URL_HEADER
		    + "([a-zA-Z_0-9\\.-]+:[0-9]+(?:,[a-zA-Z_0-9\\.-]+:[0-9]+)*)/([^:/]+)(:[^:/]*)?(:[^:/]*)?/([^:/?]+)(\\?(?:[a-zA-Z_0-9]+=[^&=?]+(?:&[a-zA-Z_0-9]+=[^&=?]+)*)?)?";
    private static Pattern urlPattern;

    // default connection informations
    // private static final String default_hostname = "localhost";
    // private static final int default_port = 30000;
    static final String default_user = "public";
    static final String default_password = "";

    private final ArrayList<JciConnectionInfo> conInfoList;
    private final String dbname;
    private final String dbuser;
    private final String dbpasswd;
    private final String propertyStr;
    private final ConnectionProperties connProperties;

    static {
	urlPattern = Pattern.compile(URL_PATTERN, 0);
    }

    public RyeConnectionUrl(ArrayList<JciConnectionInfo> conInfoList, String dbname, String dbuser, String dbpasswd,
		    String propertyStr, ConnectionProperties connProperties)
    {
	this.conInfoList = conInfoList;
	this.dbname = dbname;
	this.dbuser = dbuser;
	this.dbpasswd = dbpasswd;
	this.propertyStr = propertyStr;
	this.connProperties = connProperties;
    }

    static boolean acceptsURL(String url) throws SQLException
    {
	if (url == null) {
	    return false;
	}

	if (url.toLowerCase().startsWith(RYE_JDBC_URL_HEADER)) {
	    return true;
	}

	return false;
    }

    public static RyeConnectionUrl parseUrl(String url, String user, String passwd) throws SQLException
    {
	if (!acceptsURL(url)) {
	    throw RyeException.createRyeException((RyeConnectionUrl) null, RyeErrorCode.ER_INVALID_URL, url, null);
	}

	Matcher matcher = urlPattern.matcher(url);
	if (!matcher.find()) {
	    throw RyeException.createRyeException((RyeConnectionUrl) null, RyeErrorCode.ER_INVALID_URL, url, null);
	}

	String match = matcher.group();
	if (!match.equals(url)) {
	    throw RyeException.createRyeException((RyeConnectionUrl) null, RyeErrorCode.ER_INVALID_URL, url, null);
	}

	String hostsCommaList = matcher.group(1);
	String dbname = matcher.group(2);
	String dbuser = matcher.group(3);
	String dbpasswd = matcher.group(4);
	String portName = matcher.group(5);
	String propertyStr = matcher.group(6);

	ArrayList<JciConnectionInfo> conInfoList = new ArrayList<JciConnectionInfo>();

	String[] hosts = hostsCommaList.split(",");

	for (int i = 0; i < hosts.length; i++) {
	    String[] token = hosts[i].split(":");
	    conInfoList.add(new JciConnectionInfo(token[0], Integer.parseInt(token[1]), portName));
	}

	ConnectionProperties connProperties = new ConnectionProperties(propertyStr);

	if (dbuser == null || dbuser.length() <= 1) {
	    dbuser = default_user;
	}
	else {
	    dbuser = dbuser.substring(1);
	}

	if (dbpasswd == null || dbpasswd.length() <= 1) {
	    dbpasswd = default_password;
	}
	else {
	    dbpasswd = dbpasswd.substring(1);
	}

	if (user != null && user.length() != 0) {
	    dbuser = user;
	}
	if (passwd != null) {
	    dbpasswd = passwd;
	}

	if (connProperties.getConnLoadBal()) {
	    Collections.shuffle(conInfoList);
	}

	return new RyeConnectionUrl(conInfoList, dbname, dbuser, dbpasswd, propertyStr, connProperties);
    }

    public static String makeJdbcUrl(JciConnectionInfo conInfo, String dbname, String user, String passwd,
		    String propStr)
    {
	ArrayList<JciConnectionInfo> conInfoList = new ArrayList<JciConnectionInfo>();
	conInfoList.add(conInfo);
	return makeJdbcUrl(conInfoList, dbname, user, passwd, propStr);
    }

    public static String makeJdbcUrl(ArrayList<JciConnectionInfo> conInfoList, String dbname, String user,
		    String passwd, String propStr)
    {
	return makeJdbcUrl(getServerListForUrl(conInfoList), dbname, user, passwd, conInfoList.get(0).getstrPortName(),
			propStr);
    }

    public static String makeJdbcUrl(String serverList, String dbname, String user, String passwd,
		    String servicePortName, String propStr)
    {
	if (propStr == null) {
	    propStr = "?";
	}

	return String.format("%s%s/%s:%s:%s/%s%s", RYE_JDBC_URL_HEADER, serverList, dbname, user, passwd,
			servicePortName, propStr);
    }

    private static String getServerListForUrl(ArrayList<JciConnectionInfo> conInfoList)
    {
	StringBuffer sb = new StringBuffer();
	boolean isFirst = true;
	for (JciConnectionInfo conInfo : conInfoList) {
	    if (isFirst == false) {
		sb.append(",");
	    }
	    sb.append(conInfo.getHostname());
	    sb.append(":");
	    sb.append("" + conInfo.getPort());

	    isFirst = false;
	}
	return sb.toString();
    }

    public ArrayList<JciConnectionInfo> getConInfoList()
    {
	return conInfoList;
    }

    public JciConnectionInfo[] getConInfoArray()
    {
	JciConnectionInfo[] conInfoArr = new JciConnectionInfo[conInfoList.size()];
	return (conInfoList.toArray(conInfoArr));
    }

    public String getDbname()
    {
	return dbname;
    }

    public String getDbuser()
    {
	return dbuser;
    }

    public String getDbpasswd()
    {
	return dbpasswd;
    }

    public String getPropertyStr()
    {
	return propertyStr;
    }

    public ConnectionProperties getConnProperties()
    {
	return connProperties;
    }

    public String getUrlForLogging(String dbuser)
    {
	return RyeConnectionUrl.makeJdbcUrl(getConInfoList(), getDbname(), dbuser, "********", getPropertyStr());
    }

    public String getUrlForLogging()
    {
	return this.getUrlForLogging(getDbuser());
    }
}
