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

package rye.jdbc.admin;

import java.io.ByteArrayInputStream;
import java.sql.SQLException;
import java.util.ArrayList;

import rye.jdbc.driver.RyeConnectionUrl;
import rye.jdbc.driver.RyeDriver;
import rye.jdbc.driver.RyeErrorCode;
import rye.jdbc.driver.RyeException;
import rye.jdbc.jci.BrokerHandler;
import rye.jdbc.jci.BrokerResponse;
import rye.jdbc.jci.JciConnectionInfo;
import rye.jdbc.jci.JciException;
import rye.jdbc.jci.JciUtil;
import rye.jdbc.jci.Protocol;

public class LocalMgmt
{
    private final static int DEFAULT_TIMEOUT = 600 * 1000;
    private final static int RES_INT_SIZE = 4;

    private static final int timeout = DEFAULT_TIMEOUT;
    private final JciConnectionInfo conInfo;
    private final ArrayList<JciConnectionInfo> conInfoList;

    public LocalMgmt(String localMgmtHost, int localMgmtPort) throws SQLException
    {
	this(new JciConnectionInfo(localMgmtHost, localMgmtPort, "rw"));
    }

    public LocalMgmt(JciConnectionInfo conInfo) throws SQLException
    {
	this.conInfo = conInfo;

	conInfoList = new ArrayList<JciConnectionInfo>();
	conInfoList.add(conInfo);
    }

    ShardMgmtInfo[] getShardMgmtInfo() throws SQLException
    {
	try {
	    byte[] sendMsg = Protocol.mgmtRequestMsg(Protocol.BRREQ_OP_CODE_GET_SHARD_MGMT_INFO);

	    BrokerResponse brRes = BrokerHandler.mgmtRequest(null, conInfo, sendMsg, timeout, true);
	    int result = brRes.getResultCode();
	    if (result < 0) {
		throw new JciException(result);
	    }

	    byte[] resultmsg = brRes.getAdditionalMsg(0);

	    if (resultmsg.length < RES_INT_SIZE) {
		throw JciException.createJciException(null, RyeErrorCode.ER_ILLEGAL_DATA_SIZE);
	    }
	    int offset = 0;

	    int numShardMgmt = JciUtil.bytes2int(resultmsg, offset);
	    offset += RES_INT_SIZE;

	    ShardMgmtInfo[] shardMgmtInfo = new ShardMgmtInfo[numShardMgmt];

	    for (int i = 0; i < numShardMgmt; i++) {
		if (resultmsg.length - offset < RES_INT_SIZE) {
		    throw JciException.createJciException(null, RyeErrorCode.ER_ILLEGAL_DATA_SIZE);
		}

		int mgmtInfoSize = JciUtil.bytes2int(resultmsg, offset);
		offset += RES_INT_SIZE;

		ByteArrayInputStream instream = new ByteArrayInputStream(resultmsg, offset, mgmtInfoSize);
		offset += mgmtInfoSize;

		String dbname = unpackString(instream);
		int port = unpackInt(instream);
		shardMgmtInfo[i] = new ShardMgmtInfo(dbname, new NodeAddress(conInfo.getHostname(), port));
	    }

	    return shardMgmtInfo;

	} catch (JciException e) {
	    throw RyeException.createRyeException(makeRyeConnectionUrlForException(), e);
	}

    }

    int numShardVersionInfo() throws SQLException
    {
	try {
	    byte[] sendMsg = Protocol.mgmtRequestMsg(Protocol.BRREQ_OP_CODE_NUM_SHARD_VERSION_INFO);

	    BrokerResponse brRes = BrokerHandler.mgmtRequest(null, conInfo, sendMsg, timeout, true);
	    int result = brRes.getResultCode();
	    if (result < 0) {
		throw new JciException(result);
	    }

	    byte[] resultmsg = brRes.getAdditionalMsg(0);

	    if (resultmsg.length < RES_INT_SIZE) {
		throw JciException.createJciException(null, RyeErrorCode.ER_ILLEGAL_DATA_SIZE);
	    }

	    return JciUtil.bytes2int(resultmsg, 0);

	} catch (JciException e) {
	    throw RyeException.createRyeException(makeRyeConnectionUrlForException(), e);
	}
    }

    byte[] readRyeConf() throws SQLException
    {
	return readRyeFile(Protocol.READ_RYE_FILE_RYE_CONF);
    }

    byte[] readBrokerAclConf() throws SQLException
    {
	return readRyeFile(Protocol.READ_RYE_FILE_BR_ACL);
    }

    void brokerAclReload(byte[] acl) throws SQLException
    {
	try {
	    byte[] sendMsg = Protocol
			    .mgmtRequestMsg(Protocol.BRREQ_OP_CODE_BR_ACL_RELOAD, new Integer(acl.length), acl);

	    BrokerResponse brRes = BrokerHandler.mgmtRequest(null, conInfo, sendMsg, timeout, true);
	    int result = brRes.getResultCode();
	    if (result < 0) {
		throw new JciException(result);
	    }
	} catch (JciException e) {
	    throw RyeException.createRyeException(makeRyeConnectionUrlForException(), e);
	}
    }

    private byte[] readRyeFile(int which) throws SQLException
    {
	try {
	    byte[] sendMsg = Protocol.mgmtRequestMsg(Protocol.BRREQ_OP_CODE_READ_RYE_FILE, new Integer(which));

	    BrokerResponse brRes = BrokerHandler.mgmtRequest(null, conInfo, sendMsg, timeout, true);
	    int result = brRes.getResultCode();
	    if (result < 0) {
		throw new JciException(result);
	    }

	    return brRes.getAdditionalMsg(0);

	} catch (JciException e) {
	    throw RyeException.createRyeException(makeRyeConnectionUrlForException(), e);
	}
    }

    void writeRyeConf(byte[] contents) throws SQLException
    {
	try {
	    byte[] sendMsg = Protocol.mgmtRequestMsg(Protocol.BRREQ_OP_CODE_WRITE_RYE_CONF,
			    new Integer(contents.length), contents);

	    BrokerResponse brRes = BrokerHandler.mgmtRequest(null, conInfo, sendMsg, timeout, true);
	    int result = brRes.getResultCode();
	    if (result < 0) {
		throw new JciException(result);
	    }
	} catch (JciException e) {
	    throw RyeException.createRyeException(makeRyeConnectionUrlForException(), e);
	}
    }

    public void updateConf(RyeConfValue ryeConf) throws SQLException
    {
	try {
	    byte[] sendMsg = Protocol.mgmtRequestMsg(Protocol.BRREQ_OP_CODE_UPDATE_CONF, ryeConf.getProcName(),
			    ryeConf.getSectName(), ryeConf.getKeyName(), ryeConf.getValue());

	    BrokerResponse brRes = BrokerHandler.mgmtRequest(null, conInfo, sendMsg, timeout, true);
	    int result = brRes.getResultCode();
	    if (result < 0) {
		throw new JciException(result);
	    }
	} catch (JciException e) {
	    throw RyeException.createRyeException(makeRyeConnectionUrlForException(), e);
	}
    }

    void deleteConf(String procName, String sectName, String key) throws SQLException
    {
	int numPart;
	if (procName == null || procName.length() == 0) {
	    throw RyeException.createRyeException(makeRyeConnectionUrlForException(), RyeErrorCode.ER_INVALID_ARGUMENT,
			    null);
	}
	if (sectName == null) {
	    sectName = "";
	    key = "";
	    numPart = 1;
	}
	else if (key == null) {
	    key = "";
	    numPart = 2;
	}
	else {
	    numPart = 3;
	}

	try {
	    byte[] sendMsg = Protocol.mgmtRequestMsg(Protocol.BRREQ_OP_CODE_DELETE_CONF, new Integer(numPart),
			    procName, sectName, key);

	    BrokerResponse brRes = BrokerHandler.mgmtRequest(null, conInfo, sendMsg, timeout, true);
	    int result = brRes.getResultCode();
	    if (result < 0) {
		throw new JciException(result);
	    }
	} catch (JciException e) {
	    throw RyeException.createRyeException(makeRyeConnectionUrlForException(), e);
	}
    }

    public String getConf(String procName, String sectName, String key) throws SQLException
    {
	try {
	    if (procName == null)
		procName = "";
	    if (sectName == null)
		sectName = "";
	    if (key == null)
		key = "";

	    byte[] sendMsg = Protocol.mgmtRequestMsg(Protocol.BRREQ_OP_CODE_GET_CONF, new Integer(3), procName,
			    sectName, key);

	    BrokerResponse brRes = BrokerHandler.mgmtRequest(null, conInfo, sendMsg, timeout, true);
	    int result = brRes.getResultCode();
	    if (result < 0) {
		throw new JciException(result);
	    }

	    byte[] confValue = brRes.getAdditionalMsg(0);
	    if (confValue == null || confValue.length <= 1) {
		return null;
	    }
	    else {
		return new String(confValue, 0, confValue.length - 1, RyeDriver.sysCharset);
	    }
	} catch (JciException e) {
	    throw RyeException.createRyeException(makeRyeConnectionUrlForException(), e);
	}
    }

    void deleteTmpFile(String file) throws SQLException
    {
	try {
	    byte[] sendMsg = Protocol.mgmtRequestMsg(Protocol.BRREQ_OP_CODE_RM_TMP_FILE, file);

	    BrokerResponse brRes = BrokerHandler.mgmtRequest(null, conInfo, sendMsg, timeout, true);
	    int result = brRes.getResultCode();
	    if (result < 0) {
		throw new JciException(result);
	    }
	} catch (JciException e) {
	    throw RyeException.createRyeException(makeRyeConnectionUrlForException(), e);
	}
    }

    private int unpackInt(ByteArrayInputStream instream) throws JciException
    {
	if (instream.available() < RES_INT_SIZE) {
	    throw JciException.createJciException(null, RyeErrorCode.ER_ILLEGAL_DATA_SIZE);
	}

	byte[] res = new byte[RES_INT_SIZE];
	if (instream.read(res, 0, res.length) < res.length) {
	    throw JciException.createJciException(null, RyeErrorCode.ER_ILLEGAL_DATA_SIZE);
	}
	return JciUtil.bytes2int(res, 0);
    }

    private String unpackString(ByteArrayInputStream instream) throws JciException
    {
	int strsize = unpackInt(instream);
	if (instream.available() < strsize) {
	    throw JciException.createJciException(null, RyeErrorCode.ER_ILLEGAL_DATA_SIZE);
	}

	byte[] res = new byte[strsize - 1];
	instream.read(res, 0, res.length);
	instream.read();

	return new String(res, RyeDriver.sysCharset);
    }

    private RyeConnectionUrl makeRyeConnectionUrlForException()
    {
	return new RyeConnectionUrl(conInfoList, "", "", "", "", null);
    }
}
