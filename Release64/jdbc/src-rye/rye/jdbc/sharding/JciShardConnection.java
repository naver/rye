/*
 * Copyright 2017 NAVER Corp.
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted
 * provided that the following conditions are met:
 *
 *  - Redistributions of source code must retain the above copyright notice, this list of conditions and the
 * following disclaimer.
 *  - Redistributions in binary form must reproduce the above copyright notice, this list of conditions and
 * the following disclaimer in the documentation and/or other materials provided with the distribution.
 *  - Neither the name of Search Solution Coporation nor the names of its contributors may be used to
 * endorse or promote products derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL
 * THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF
 * THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package rye.jdbc.sharding;

import rye.jdbc.driver.ConnectionProperties;
import rye.jdbc.driver.RyeConnectionUrl;
import rye.jdbc.driver.RyeException;
import rye.jdbc.jci.JciException;
import rye.jdbc.jci.JciNormalConnection;
import rye.jdbc.jci.JciStatementQueryInfo;

class JciShardConnection extends JciNormalConnection
{
    private static final int STATEMENT_POOL_SIZE = 100;
    
    private final StatementPool statementPool;
    private final ShardDataSource dataSource;
    private final ShardNodeId shardNodeid;
    private ShardKey shardKey;

    JciShardConnection(RyeConnectionUrl connUrl, ConnectionProperties conProperties, ShardDataSource dataSource,
		    ShardNodeId nodeid, String dbuser, String dbpasswd) throws RyeException, JciException
    {
	super(connUrl, conProperties, dbuser, dbpasswd);
	
	this.dataSource = dataSource;
	this.shardNodeid = nodeid;
	statementPool = new StatementPool(STATEMENT_POOL_SIZE);
    }

    /*
     * override JciNormalConnection methods
     */
    synchronized public void close()
    {
	try {
	    endTranRequest(false);
	    dataSource.returnToPool(this);
	} catch (RyeException e) {
	    /* if something is wrong, do not resue this connection */
	    remove();
	}
    }

    protected void clientSocketClose()
    {
	super.clientSocketClose();
	statementPool.clear();
    }

    synchronized public JciShardStatement prepareRequest(String sql, byte flag, ShardGroupStatement stmtShardGroup) 
		    throws RyeException
    {
	try {
	    JciStatementQueryInfo queryInfo = prepareInternal(sql, flag);
	    return new JciShardStatement(this, queryInfo, stmtShardGroup);
	} catch (JciException e) {
	    throw RyeException.createRyeException(this, e);
	}
    }

    /*
     * JciShardConnection methods 
     */
    
    synchronized void changeUser(String dbuser, String dbpasswd) throws RyeException
    {
	if (this.currDBuser.equals(dbuser)) {
	    return;
	}

	statementPool.clear();

	changeDbuserRequest(dbuser, dbpasswd);
    }
    
    synchronized void remove()
    {
	// called by JciShardDataSource. close physically */
	statementPool.clear();
	super.close();
    }
    
    synchronized JciShardStatement prepareShard(ShardGroupStatement stmtShardGroup, String sql, byte prepareFlag,
	    boolean forcePrepare) throws RyeException
    {
	/*
	 * forcePrepare flag : if this flag enabled, remove cached statement and re-prepare statement.
	 *   following cases force prepare.
	 *   - execute batch : if cached plan is used, server's plan cache error may occur.
	 *   - shard DDL : if pooled statement is reused, the query may work incorrectly.
	 *      ex) "alter table ..." -> table recreate -> "alter table ..." reused
	 */
	
	if (stmtShardGroup != null) {
	    sql = stmtShardGroup.getQueryInfo().getQuery();
	    prepareFlag = stmtShardGroup.getQueryInfo().getPrepareFlag();
	}

	JciShardStatement stmt = statementPool.remove(sql);

	if (stmt != null) {
	    if (stmtShardGroup != null && stmtShardGroup.reuseShardStatement() == false) {
		forcePrepare = true;
	    }
	    
	    if (forcePrepare == true) {
		stmt.remove();
		stmt = null;
	    }
	}

	if (stmt == null) {
	    stmt = (JciShardStatement) prepareRequest(sql, prepareFlag, stmtShardGroup);
	}
	else {
	    stmt.setShardGroupStatement(stmtShardGroup);
	}

	return stmt;
    }

    synchronized void poolingShardStatement(String sql, JciShardStatement stmt)
    {
	statementPool.put(sql, stmt);
    }

    ShardNodeId getShardNodeid()
    {
	return shardNodeid;
    }

    boolean equalsNodeId(ShardNodeId compareKey)
    {
	return (this.shardNodeid.getNodeId() == compareKey.getNodeId());
    }

    boolean equalsShardKey(ShardKey compareKey)
    {
	if (this.shardKey != null && compareKey != null) {
	    return this.shardKey.equals(compareKey);
	}

	return false;
    }

    public void setShardKey(ShardKey shardKey)
    {
	this.shardKey = shardKey;
    }
}
