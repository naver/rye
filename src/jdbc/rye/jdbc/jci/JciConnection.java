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

import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.Date;

import rye.jdbc.driver.RyeConnection;
import rye.jdbc.driver.RyeConnectionUrl;
import rye.jdbc.driver.RyeErrorCode;
import rye.jdbc.driver.RyeException;
import rye.jdbc.driver.ConnectionProperties;
import rye.jdbc.log.Log;
import rye.jdbc.sharding.ShardAdmin;
import rye.jdbc.sharding.ShardGroupConnection;
import rye.jdbc.sharding.ShardInfo;
import rye.jdbc.sharding.ShardInfoManager;
import rye.jdbc.sharding.ShardMgmtConnectionInfo;
import rye.jdbc.sharding.ShardNodeInstance;

abstract public class JciConnection
{
    public static final int SHARD_GROUPID_UNKNOWN = 0;

    RyeConnection ryeCon;

    protected RyeConnectionUrl connUrl;

    protected boolean autoCommit = true;
    protected String currDBuser = "";
    protected String currDBpasswd = "";

    protected ConnectionProperties connProperties;

    protected CasInfo casInfo = new CasInfo();

    protected JciConnection(RyeConnectionUrl connUrl, String dbuser, String dbpasswd)
    {
	this.connUrl = connUrl;

	if (dbuser == null) {
	    this.currDBuser = connUrl.getDbuser();
	}
	else {
	    this.currDBuser = dbuser;
	}

	if (dbpasswd == null) {
	    this.currDBpasswd = connUrl.getDbpasswd();
	}
	else {
	    this.currDBpasswd = dbpasswd;
	}

	this.connProperties = connUrl.getConnProperties();
    }

    public static JciConnection makeJciConnection(RyeConnectionUrl connUrl, String user, String passwd,
		    ShardInfoManager shardInfoManager) throws RyeException
    {
	try {
	    return new JciNormalConnection(connUrl, null, user, passwd);
	} catch (JciException e) {
	    if (e.getJciError() == RyeErrorCode.ER_SHARD_MGMT) {
		ShardMgmtConnectionInfo shardMgmtConInfo = new ShardMgmtConnectionInfo(connUrl.getConInfoArray(),
				connUrl.getDbname());

		String shardLogfile = null;
		if (connUrl.getConnProperties().getLogShardInfo() == true) {
		    shardLogfile = connUrl.getConnProperties().getLogFileShardInfo();
		}

		ShardInfo tmpShardInfo = shardInfoManager.getShardInfo(shardMgmtConInfo, shardLogfile);
		if (tmpShardInfo == null) {
		    throw RyeException.createRyeException(connUrl, RyeErrorCode.ER_SHARD_INFO_INVALID, e);
		}
		return new ShardGroupConnection(connUrl, tmpShardInfo, user, passwd);
	    }
	    else {
		throw RyeException.createRyeException(connUrl, e);
	    }
	}
    }

    public void dumpProperty(PrintStream out)
    {
	this.connProperties.dump(out);
    }
    
    public String getDatabaseName()
    {
	return connUrl.getDbname();
    }

    public String getDbuser()
    {
	return currDBuser;
    }

    public String getDbpasswd()
    {
	return currDBpasswd;
    }

    public String getHostname()
    {
	return getFirstConnectionInfo().getHostname();
    }

    protected JciConnectionInfo getFirstConnectionInfo()
    {
	return connUrl.getConInfoList().get(0);
    }

    protected RyeConnectionUrl getConnectionUrl()
    {
	return connUrl;
    }

    public void setConnectionProperties(ConnectionProperties connProperties)
    {
	this.connProperties = connProperties;
    }

    protected int getSlowQueryThresholdMillis()
    {
	return connProperties.getSlowQueryThresholdMillis();
    }

    protected boolean getLogSlowQueris()
    {
	return connProperties.getLogSlowQueris();
    }

    protected boolean getLogOnException()
    {
	return connProperties.getLogOnException();
    }

    protected int getReconnectTime()
    {
	return connProperties.getReconnectTime();
    }

    protected int getConnectTimeout()
    {
	return connProperties.getConnectTimeout();
    }

    public int getQueryTimeout()
    {
	return connProperties.getQueryTimeout();
    }

    public String getCharset()
    {
	return connProperties.getCharset();
    }

    public String getShardkeyCharset()
    {
	return connProperties.getCharset();
    }

    public ConnectionProperties.ZERO_DATE_BEHAVIOR getZeroDateTimeBehavior()
    {
	return connProperties.getZeroDateTimeBehavior();
    }

    public boolean getLogSlowQuery()
    {
	return connProperties.getLogSlowQueris();
    }

    public boolean getUseLazyConnection()
    {
	return connProperties.getUseLazyConnection();
    }

    protected Log getLogger()
    {
	return connProperties.getLogger();
    }

    public String getUrlForLogging()
    {
	return connUrl.getUrlForLogging(currDBuser);
    }

    public boolean brokerInfoStatementPooling()
    {
	return (casInfo.getStatementPooling() == Protocol.CAS_STATEMENT_POOLING_ON);
    }

    boolean brokerInfoReconnectWhenServerDown()
    {
	return false;
    }

    public boolean supportHoldableResult()
    {
	return (casInfo.getHoldableResult() == Protocol.CAS_HOLDABLE_RESULT_SUPPORT);
    }

    public void setRyeConnection(RyeConnection con)
    {
	ryeCon = con;
    }

    public void setAutoCommit(boolean autoCommit)
    {
	this.autoCommit = autoCommit;
    }

    public boolean getAutoCommit()
    {
	return autoCommit;
    }

    public void logException(Throwable t)
    {
	if (!getLogOnException()) {
	    return;
	}

	StringBuffer b = new StringBuffer();
	b.append("DUMP EXCEPTION\n");
	b.append("[" + t.getClass().getName() + "]");

	Log logger = getLogger();
	if (logger != null) {
	    synchronized (logger) {
		logger.logInfo(b.toString(), t);
	    }
	}
    }

    private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    public void logSlowQuery(long begin, long end, String sql, BindParameter p)
    {
	if (getLogSlowQueris() != true) {
	    return;
	}

	long elapsed = end - begin;
	if (getSlowQueryThresholdMillis() > elapsed) {
	    return;
	}

	StringBuffer b = new StringBuffer();
	b.append("SLOW QUERY\n");
	b.append(String.format("[CAS INFO]\n%s\n", getCasInfoString()));
	b.append(String.format("[TIME]\nSTART: %s, ELAPSED: %d\n", dateFormat.format(new Date(begin)), elapsed));
	b.append("[SQL]\n").append(sql).append('\n');
	if (p != null && p.getCount() > 0) {
	    b.append("[BIND]\n");
	    Object[] values = p.getValues();
	    for (int i = 0; i < values.length; i++) {
		if (i != 0)
		    b.append(", ");
		b.append(values[i].toString());
	    }
	    b.append('\n');
	}

	Log logger = getLogger();
	if (logger != null) {
	    synchronized (logger) {
		logger.logInfo(b.toString());
	    }
	}
    }

    byte[] getDbSessionId()
    {
	return casInfo.getDbSessionId();
    }

    public static byte getPrepareFlag(boolean holdable)
    {
	byte flag = 0;

	if (holdable)
	    flag |= Protocol.PREPARE_FLAG_HOLDABLE;

	return flag;
    }

    abstract public String getCasInfoString();

    abstract public void close();

    abstract public void endTranRequest(boolean type) throws RyeException;

    abstract public String getVersionRequest() throws RyeException;

    abstract public String qeryplanRequest(String sql) throws RyeException;

    abstract public JciStatement schemaInfoRequest(int type, String arg1, String arg2) throws RyeException;

    abstract public JciStatement prepareRequest(String sql, byte flag) throws RyeException;

    abstract public void cancelRequest() throws RyeException;

    abstract public void setTraceShardConnection(int level);

    abstract public String getTraceShardConnection();

    abstract public ShardAdmin getShardAdmin();

    abstract public void setReuseShardStatement(boolean reuseStatement);

    abstract public boolean isShardingConnection();

    abstract public Object[] checkShardNodes() throws RyeException;

    abstract public int getServerHaMode() throws RyeException;

    abstract public short getStatusInfoServerNodeid();

    abstract public ShardNodeInstance[] getShardNodeInstance();

    abstract public int getServerStartTime();
}
