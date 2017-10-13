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

import rye.jdbc.driver.RyeErrorCode;
import rye.jdbc.jci.JciException;
import rye.jdbc.jci.JciNormalStatement;
import rye.jdbc.jci.JciStatementQueryInfo;
import rye.jdbc.jci.RyeCommandType;

public class JciShardStatement extends JciNormalStatement
{
    private ShardGroupStatement stmtShardGroup;

    /*
     * override JciNormalStatement methods
     */

    JciShardStatement(JciShardConnection relatedC, JciStatementQueryInfo queryInfo, ShardGroupStatement stmtShardGroup)
		    throws JciException
    {
	super(relatedC, queryInfo);

	this.stmtShardGroup = stmtShardGroup;
	checkShardInfoChanged();
    }

    public void close()
    {
	String sql = this.getQueryInfo().getQuery();

	if (isServerHandleValid() && sql != null && isPoolable(getSqlType())) {
	    closeCursor();
	    ((JciShardConnection) jciCon).poolingShardStatement(sql, this);
	}
	else {
	    remove();
	}
    }

    private boolean isPoolable(RyeCommandType.SQL_TYPE sqlType)
    {
	return (sqlType != RyeCommandType.SQL_TYPE.DDL);
    }

    protected void reset() throws JciException
    {
	super.reset();
	checkShardInfoChanged();
    }

    protected ShardGroupStatement getShardGroupStatement()
    {
	return this.stmtShardGroup;
    }

    /*
     * JciShardStatement methods
     */

    void remove()
    {
	super.close();
    }

    void setShardGroupStatement(ShardGroupStatement stmt)
    {
	this.stmtShardGroup = stmt;
    }

    private void checkShardInfoChanged() throws JciException
    {
	if (stmtShardGroup != null) {
	    JciStatementQueryInfo prevQueryInfo = stmtShardGroup.getQueryInfo();

	    /*
	     * replace shard group statement's query info with last created shard node's query info. when statement pool
	     * error occurs, query info may be changed
	     */
	    stmtShardGroup.setQueryInfo(this.getQueryInfo());

	    if (prevQueryInfo.getShardKeyInfo().equals(this.getQueryInfo().getShardKeyInfo()) == false) {
		/*
		 * if two ShardKeyInfos are different, stored shard key info is obsolete. retry execute logic
		 */
		throw JciException.createJciException(jciCon, RyeErrorCode.ER_SHARD_DIFFERENT_SHARD_KEY_INFO);
	    }
	}
    }
}
