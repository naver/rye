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

import java.io.IOException;
import java.net.SocketTimeoutException;

import rye.jdbc.driver.RyeErrorCode;

public class BrokerHandler
{
    static JciSocket connectBroker(JciConnectionInfo conInfo, int timeout, int sockTimeout) throws JciException
    {
	JciSocket socket = new JciSocket(conInfo, timeout, sockTimeout);

	try {
	    socket.getOutputStream().write(Protocol.getConnectMsg(conInfo.getbytePortName()));
	    socket.getOutputStream().flush();

	    BrokerResponse brRes = readBrokerResponse(socket.getInputStream());
	    int code = brRes.getResultCode();
	    if (code < 0) {
		if (code == RyeErrorCode.BR_ER_NOT_SHARD_MGMT_OPCODE) {
		    throw new JciException(RyeErrorCode.ER_SHARD_MGMT);
		}
		else {
		    throw new JciException(code);
		}
	    }
	    else if (code == 0) {
		return socket;
	    }

	    // if (code > 0) { only windows }
	    socket.close();
	    throw new JciException(RyeErrorCode.ER_NO_SUPPORTED);

	} catch (SocketTimeoutException e) {
	    socket.close();
	    throw new JciException(RyeErrorCode.ER_TIMEOUT, e);
	} catch (IOException e) {
	    socket.close();
	    throw new JciException(RyeErrorCode.ER_CONNECTION, e);
	} catch (JciException e) {
	    socket.close();
	    throw e;
	}
    }

    static void pingBroker(JciConnectionInfo conInfo, int timeout) throws IOException, JciException
    {
	if (conInfo == null || conInfo.getbytePortName() == null) {
	    return;
	}

	JciSocket socket = new JciSocket(conInfo, timeout, timeout);

	try {
	    socket.getOutputStream().write(Protocol.getPingCheckMsg(conInfo.getbytePortName()));
	    socket.getOutputStream().flush();

	    BrokerResponse brRes = readBrokerResponse(socket.getInputStream());
	    int error = brRes.getResultCode();
	    if (error < 0) {
		throw new JciException(error);
	    }
	} finally {
	    socket.close();
	}
    }

    private static void cancelQueryInternal(JciConnectionInfo conInfo, byte[] data, int timeout) throws IOException,
		    JciException
    {
	JciSocket socket = new JciSocket(conInfo, timeout, timeout);

	try {
	    socket.getOutputStream().write(data);
	    socket.getOutputStream().flush();

	    BrokerResponse brRes = readBrokerResponse(socket.getInputStream());
	    int error = brRes.getResultCode();
	    if (error < 0) {
		throw new JciException(error);
	    }
	} catch (SocketTimeoutException e) {
	    throw new JciException(RyeErrorCode.ER_TIMEOUT);
	} finally {
	    socket.close();
	}
    }

    static void cancelQuery(JciConnectionInfo conInfo, int casId, int casPid, int timeout) throws IOException,
		    JciException
    {
	cancelQueryInternal(conInfo, Protocol.getQueryCancelMsg(casId, casPid, conInfo.getbytePortName()), timeout);
    }

    public static BrokerResponse syncShardInfo(JciConnectionInfo conInfo, String dbname, long nodeVersion,
		    long gidVersion, long created_at, int timeout) throws JciException
    {
	byte[] sendMsg = Protocol.mgmtRequestMsg(Protocol.BRREQ_OP_CODE_GET_SHARD_INFO, dbname, new Long(nodeVersion),
			new Long(gidVersion), new Long(created_at));
	BrokerResponse brRes = mgmtRequest(null, conInfo, sendMsg, timeout, true);
	return brRes;
    }

    public static int shardMgmtAdminRequest(JciSocket socketArg, JciConnectionInfo conInfo, byte[] sendMsg,
		    int timeout, boolean waitResponse) throws JciException
    {
	int result = 0;

	BrokerResponse brRes = mgmtRequest(socketArg, conInfo, sendMsg, timeout, waitResponse);

	if (waitResponse) {
	    result = brRes.getResultCode();
	    if (result < 0) {
		throw new JciException(result);
	    }
	}

	return result;
    }

    public static BrokerResponse mgmtRequest(JciSocket socketArg, JciConnectionInfo conInfo, byte[] sendMsg,
		    int timeout, boolean readResponse) throws JciException
    {
	JciSocket socket;

	if (socketArg == null) {
	    socket = new JciSocket(conInfo, timeout, timeout);
	}
	else {
	    socket = socketArg;
	}

	try {
	    socket.getOutputStream().write(sendMsg);
	    socket.getOutputStream().flush();

	    BrokerResponse brRes = null;
	    if (readResponse) {
		brRes = readBrokerResponse(socket.getInputStream());
	    }
	    return brRes;

	} catch (SocketTimeoutException e) {
	    throw new JciException(RyeErrorCode.ER_TIMEOUT, e);
	} catch (IOException e) {
	    throw new JciException(RyeErrorCode.ER_CONNECTION, e);
	} finally {
	    if (socketArg == null && socket != null) {
		socket.close();
	    }
	}
    }

    private static BrokerResponse readBrokerResponse(TimedDataInputStream in) throws IOException, JciException
    {
	int msgSize = in.readInt();
	byte[] msg = new byte[msgSize];
	in.readFully(msg);
	BrokerResponse brRes = Protocol.unpackBrokerResponse(msg);
	if (brRes == null) {
	    throw new JciException(RyeErrorCode.ER_COMMUNICATION);
	}

	int additionalMsgCount = brRes.getAdditionalMsgCount();
	if (additionalMsgCount > 0) {
	    byte[][] additionalMsg = new byte[additionalMsgCount][];
	    for (int i = 0; i < additionalMsgCount; i++) {
		if (brRes.getAdditionalMsgSize(i) > 0) {
		    additionalMsg[i] = new byte[brRes.getAdditionalMsgSize(i)];
		    in.readFully(additionalMsg[i]);
		}
	    }
	    brRes.setAdditionalMsg(additionalMsg);
	}

	return brRes;
    }
}
