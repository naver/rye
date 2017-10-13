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
import rye.jdbc.driver.RyeException;
import rye.jdbc.jci.BrokerHandler;
import rye.jdbc.jci.JciConnection;
import rye.jdbc.jci.JciConnectionInfo;
import rye.jdbc.jci.JciException;
import rye.jdbc.jci.JciSocket;
import rye.jdbc.jci.Protocol;

public class ShardAdmin
{
    private final int[] EMPTY_INT_ARRAY = {};

    private int timeout;

    private final JciConnectionInfo conInfo;
    private final ShardInfo shardInfo;
    private JciConnection jciCon;

    private JciSocket socketDdlStart = null;
    private JciSocket socketMigStart = null;
    private JciSocket socketGcStart = null;

    ShardAdmin(JciConnectionInfo conInfo, int timeout, ShardInfo shardInfo, JciConnection jciCon)
    {
	this.conInfo = conInfo;
	this.shardInfo = shardInfo;
	this.jciCon = jciCon;

	if (timeout <= 0) {
	    this.timeout = 600 * 1000;
	}
	else {
	    this.timeout = timeout;
	}
    }

    void checkJciConClosed() throws RyeException
    {
	if (jciCon == null) {
	    throw RyeException.createRyeException(jciCon, RyeErrorCode.ER_CONNECTION_CLOSED, null);
	}
    }

    void checkArgs(Object... args) throws RyeException
    {
	for (int i = 0; i < args.length; i++) {
	    if (args[i] == null) {
		throw RyeException.createRyeException(jciCon, RyeErrorCode.ER_INVALID_ARGUMENT, null);
	    }
	}
    }

    public void init(String dbname, int groupidCount, String[] initNodes) throws RyeException
    {
	checkJciConClosed();
	checkArgs(dbname);

	if (jciCon.getDbuser().equalsIgnoreCase("dba") == false) {
	    return;
	}

	try {
	    byte[] sendMsg = Protocol.mgmtRequestMsg(Protocol.BRREQ_OP_CODE_INIT, jciCon.getDbpasswd(), dbname,
			    new Integer(groupidCount), initNodes);

	    BrokerHandler.shardMgmtAdminRequest(null, conInfo, sendMsg, timeout, true);
	} catch (JciException e) {
	    throw RyeException.createRyeException(jciCon, e);
	}
    }

    public void addNode(String nodeArg) throws RyeException
    {
	checkJciConClosed();
	checkArgs(shardInfo.getDbname(), nodeArg);

	if (jciCon.getDbuser().equalsIgnoreCase("dba") == false) {
	    return;
	}

	try {
	    byte[] sendMsg = Protocol.mgmtRequestMsg(Protocol.BRREQ_OP_CODE_ADD_NODE, shardInfo.getDbname(),
			    jciCon.getDbpasswd(), nodeArg);

	    BrokerHandler.shardMgmtAdminRequest(null, conInfo, sendMsg, timeout, true);
	} catch (JciException e) {
	    throw RyeException.createRyeException(jciCon, e);
	}
    }

    public void dropNode(String nodeArg) throws RyeException
    {
	nodeDropInternal(0, nodeArg);
    }

    public void dropNodeidAll(int nodeid) throws RyeException
    {
	if (nodeid <= 0) {
	    throw RyeException.createRyeException(jciCon, RyeErrorCode.ER_INVALID_ARGUMENT, null);
	}

	nodeDropInternal(nodeid, "");
    }

    private void nodeDropInternal(int dropAllNodeid, String nodeArg) throws RyeException
    {
	checkJciConClosed();
	checkArgs(shardInfo.getDbname(), nodeArg);

	if (jciCon.getDbuser().equalsIgnoreCase("dba") == false) {
	    return;
	}

	try {
	    byte[] sendMsg = Protocol.mgmtRequestMsg(Protocol.BRREQ_OP_CODE_DROP_NODE, shardInfo.getDbname(),
			    jciCon.getDbpasswd(), new Integer(dropAllNodeid), nodeArg);

	    BrokerHandler.shardMgmtAdminRequest(null, conInfo, sendMsg, timeout, true);
	} catch (JciException e) {
	    throw RyeException.createRyeException(jciCon, e);
	}
    }

    public void migrationStart(int groupid, int destNodeid) throws RyeException
    {
	checkJciConClosed();
	checkArgs(shardInfo.getDbname());

	if (jciCon.getDbuser().equalsIgnoreCase("dba") == false) {
	    return;
	}

	if (socketMigStart != null) {
	    throw RyeException.createRyeException(jciCon, RyeErrorCode.ER_INVALID_VALUE, null);
	}

	try {
	    int reqTimeout = timeout;

	    byte[] sendMsg = Protocol.mgmtRequestMsg(Protocol.BRREQ_OP_CODE_MIGRATION_START, shardInfo.getDbname(),
			    new Integer(groupid), new Integer(destNodeid), new Integer(0), new Integer(
					    reqTimeout / 1000));

	    if (reqTimeout >= 0) {
		/* socket timeout should be greater than protocol's timeout sec */
		reqTimeout = reqTimeout + 5000;
	    }

	    socketMigStart = new JciSocket(conInfo, timeout, timeout);

	    BrokerHandler.shardMgmtAdminRequest(socketMigStart, conInfo, sendMsg, reqTimeout, true);

	} catch (JciException e) {
	    if (socketMigStart != null) {
		socketMigStart.close();
		socketMigStart = null;
	    }
	    throw RyeException.createRyeException(jciCon, e);
	}
    }

    public void migrationEnd(int groupid, int destNodeid, int numShardKeys) throws RyeException
    {
	checkJciConClosed();
	checkArgs(shardInfo.getDbname());

	if (jciCon.getDbuser().equalsIgnoreCase("dba") == false) {
	    return;
	}

	if (socketMigStart == null) {
	    throw RyeException.createRyeException(jciCon, RyeErrorCode.ER_INVALID_VALUE, null);
	}

	try {
	    int reqTimeout = timeout;

	    byte[] sendMsg = Protocol.mgmtRequestMsg(Protocol.BRREQ_OP_CODE_MIGRATION_END, shardInfo.getDbname(),
			    new Integer(groupid), new Integer(destNodeid), new Integer(numShardKeys), new Integer(
					    reqTimeout / 1000));

	    if (reqTimeout >= 0) {
		/* socket timeout should be greater than protocol's timeout sec */
		reqTimeout = reqTimeout + 5000;
	    }

	    BrokerHandler.shardMgmtAdminRequest(socketMigStart, conInfo, sendMsg, reqTimeout, true);
	} catch (JciException e) {
	    throw RyeException.createRyeException(jciCon, e);
	} finally {
	    socketMigStart.close();
	    socketMigStart = null;
	}
    }

    public void DdlStart() throws RyeException
    {
	checkJciConClosed();
	checkArgs(shardInfo.getDbname());

	if (socketDdlStart != null) {
	    throw RyeException.createRyeException(jciCon, RyeErrorCode.ER_INVALID_VALUE, null);
	}

	try {
	    int reqTimeout = timeout;

	    byte[] sendMsg = Protocol.mgmtRequestMsg(Protocol.BRREQ_OP_CODE_DDL_START, shardInfo.getDbname(),
			    new Integer(reqTimeout / 1000));

	    if (reqTimeout >= 0) {
		/* socket timeout should be greater than protocol's timeout sec */
		reqTimeout = reqTimeout + 5000;
	    }

	    socketDdlStart = new JciSocket(conInfo, timeout, timeout);

	    BrokerHandler.shardMgmtAdminRequest(socketDdlStart, conInfo, sendMsg, reqTimeout, true);

	} catch (JciException e) {
	    if (socketDdlStart != null) {
		socketDdlStart.close();
		socketDdlStart = null;
	    }
	    throw RyeException.createRyeException(jciCon, e);
	}
    }

    public void DdlEnd()
    {
	if (socketDdlStart == null) {
	    return;
	}

	try {
	    checkJciConClosed();
	    checkArgs(shardInfo.getDbname());

	    int reqTimeout = timeout;

	    byte[] sendMsg = Protocol.mgmtRequestMsg(Protocol.BRREQ_OP_CODE_DDL_END, shardInfo.getDbname(),
			    new Integer(reqTimeout / 1000));

	    if (reqTimeout >= 0) {
		/* socket timeout should be greater than protocol's timeout sec */
		reqTimeout = reqTimeout + 5000;
	    }

	    BrokerHandler.shardMgmtAdminRequest(socketDdlStart, conInfo, sendMsg, reqTimeout, false);

	} catch (Exception e) {
	    // throw RyeException.createRyeException(jciCon, e);
	}

	if (socketDdlStart != null) {
	    socketDdlStart.close();
	    socketDdlStart = null;
	}
    }

    public void GcStart() throws RyeException
    {
	checkJciConClosed();
	checkArgs(shardInfo.getDbname());

	if (socketGcStart != null) {
	    throw RyeException.createRyeException(jciCon, RyeErrorCode.ER_INVALID_VALUE, null);
	}

	try {
	    int reqTimeout = timeout;

	    byte[] sendMsg = Protocol.mgmtRequestMsg(Protocol.BRREQ_OP_CODE_GC_START, shardInfo.getDbname(),
			    new Integer(reqTimeout / 1000));

	    if (reqTimeout >= 0) {
		/* socket timeout should be greater than protocol's timeout sec */
		reqTimeout = reqTimeout + 5000;
	    }

	    socketGcStart = new JciSocket(conInfo, timeout, timeout);

	    BrokerHandler.shardMgmtAdminRequest(socketGcStart, conInfo, sendMsg, reqTimeout, true);

	} catch (JciException e) {
	    if (socketGcStart != null) {
		socketGcStart.close();
		socketGcStart = null;
	    }
	    throw RyeException.createRyeException(jciCon, e);
	}
    }

    public void GcEnd()
    {
	if (socketGcStart == null) {
	    return;
	}

	try {
	    checkJciConClosed();
	    checkArgs(shardInfo.getDbname());

	    int reqTimeout = timeout;

	    byte[] sendMsg = Protocol.mgmtRequestMsg(Protocol.BRREQ_OP_CODE_GC_END, shardInfo.getDbname(), new Integer(
			    reqTimeout / 1000));

	    if (reqTimeout >= 0) {
		/* socket timeout should be greater than protocol's timeout sec */
		reqTimeout = reqTimeout + 5000;
	    }

	    BrokerHandler.shardMgmtAdminRequest(socketGcStart, conInfo, sendMsg, reqTimeout, false);

	} catch (Exception e) {
	    // throw RyeException.createRyeException(jciCon, e);
	}

	if (socketGcStart != null) {
	    socketGcStart.close();
	    socketGcStart = null;
	}
    }

    public int rebalance(boolean ignorePrevFail, int[] srcNodeid, int[] destNodeid) throws RyeException
    {
	return rebalanceInternal(Protocol.BRREQ_REBALANCE_TYPE_REBALANCE, ignorePrevFail, srcNodeid, destNodeid);
    }

    public int emptyNode(boolean ignorePrevFail, int[] srcNodeid, int[] destNodeid) throws RyeException
    {
	return rebalanceInternal(Protocol.BRREQ_REBALANCE_TYPE_EMPTY_NODE, ignorePrevFail, srcNodeid, destNodeid);
    }

    private int rebalanceInternal(int rebalanceType, boolean ignorePrevFail, int[] srcNodeid, int[] destNodeid)
		    throws RyeException
    {
	checkJciConClosed();
	checkArgs(shardInfo.getDbname());

	if (srcNodeid == null) {
	    srcNodeid = EMPTY_INT_ARRAY;
	}
	if (destNodeid == null) {
	    destNodeid = EMPTY_INT_ARRAY;
	}

	if (srcNodeid.length == 0 && destNodeid.length == 0) {
	    throw RyeException.createRyeException(jciCon, RyeErrorCode.ER_INVALID_ARGUMENT, null);
	}

	if (jciCon.getDbuser().equalsIgnoreCase("dba") == false) {
	    return 0;
	}

	try {
	    String dbaPasswdArg = jciCon.getDbpasswd();
	    Integer rebalTypeArg = new Integer(rebalanceType);
	    Integer prevFailArg = new Integer(ignorePrevFail ? 1 : 0);
	    byte[] sendMsg = Protocol.mgmtRequestMsg(Protocol.BRREQ_OP_CODE_REBALANCE_REQ, shardInfo.getDbname(),
			    rebalTypeArg, prevFailArg, dbaPasswdArg, srcNodeid, destNodeid);

	    return BrokerHandler.shardMgmtAdminRequest(null, conInfo, sendMsg, timeout, true);
	} catch (JciException e) {
	    throw RyeException.createRyeException(jciCon, e);
	}
    }

    public int rebalanceJobCount() throws RyeException
    {
	return rebalanceJobCountInternal(Protocol.MGMT_REBALANCE_JOB_COUNT_TYPE_REMAIN);
    }

    public int rebalanceCompleteCount() throws RyeException
    {
	return rebalanceJobCountInternal(Protocol.MGMT_REBALANCE_JOB_COUNT_TYPE_COMPLETE);
    }

    public int rebalanceFailCount() throws RyeException
    {
	return rebalanceJobCountInternal(Protocol.MGMT_REBALANCE_JOB_COUNT_TYPE_FAILED);
    }

    public int rebalanceJobCountInternal(int jobType) throws RyeException
    {
	checkJciConClosed();
	checkArgs(shardInfo.getDbname());

	try {
	    byte[] sendMsg = Protocol.mgmtRequestMsg(Protocol.BRREQ_OP_CODE_REBALANCE_JOB_COUNT, shardInfo.getDbname(),
			    new Integer(jobType));

	    return BrokerHandler.shardMgmtAdminRequest(null, conInfo, sendMsg, timeout, true);
	} catch (JciException e) {
	    throw RyeException.createRyeException(jciCon, e);
	}
    }

    public boolean hasNodeid(int nodeid)
    {
	return shardInfo.hasNodeid(nodeid);
    }

    public ShardNodeInstance[] getShardNodeInstance()
    {
	return shardInfo.getShardNodeInstance();
    }

    public ShardNodeInstance[] getShardNodeInstance(int nodeid)
    {
	return shardInfo.getShardNodeInstance(nodeid);
    }

    public String dumpNodeInfo(int level)
    {
	return shardInfo.dumpNodeInfo(level);
    }

    public String dumpGroupidInfo(int level)
    {
	return shardInfo.dumpGroupidInfo(level);
    }

    public void refreshShardInfo()
    {
	shardInfo.refreshShardInfo(timeout, 0);
    }

    void close()
    {
	if (socketDdlStart != null) {
	    socketDdlStart.close();
	}

	if (socketMigStart != null) {
	    socketMigStart.close();
	}

	jciCon = null;
    }
}
