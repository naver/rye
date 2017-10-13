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

import rye.jdbc.jci.BrokerHandler;
import rye.jdbc.jci.BrokerResponse;
import rye.jdbc.jci.JciConnectionInfo;
import rye.jdbc.jci.JciException;
import rye.jdbc.jci.JciUtil;
import rye.jdbc.jci.Protocol;

public class RyeCommand
{
    private static final String[] emptyStringArray = {};
    private final int DEFAULT_TIMEOUT = 600 * 1000;

    private final JciConnectionInfo conInfo;
    private int timeout = DEFAULT_TIMEOUT;
    private int commandExitStatus = -1;
    private byte[] commandStdout = null;
    private byte[] commandStderr = null;
    private final Integer protocolCommand;

    public RyeCommand(String localMgmtHost, int localMgmtPort) throws RyeException
    {
	conInfo = new JciConnectionInfo(localMgmtHost, localMgmtPort, null);
	protocolCommand = new Integer(Protocol.MGMT_LAUNCH_PROCESS_RYE_COMMAND);
    }

    public int exec(String[] args) throws RyeException
    {
	if (args == null || args.length == 0) {
	    throw RyeException.createRyeException((RyeConnectionUrl) null, RyeErrorCode.ER_INVALID_ARGUMENT, null);
	}

	try {
	    String[] env = emptyStringArray;

	    byte[] sendMsg = Protocol.mgmtRequestMsg(Protocol.BRREQ_OP_CODE_LAUNCH_PROCESS, protocolCommand, args, env);

	    BrokerResponse brRes = BrokerHandler.mgmtRequest(null, conInfo, sendMsg, timeout, true);
	    int result = brRes.getResultCode();
	    if (result < 0) {
		throw new JciException(result);
	    }

	    byte[] netStream;

	    netStream = brRes.getAdditionalMsg(0);
	    if (netStream != null && netStream.length >= 4) {
		commandExitStatus = JciUtil.bytes2int(netStream, 0);
	    }

	    commandStdout = brRes.getAdditionalMsg(1);
	    commandStderr = brRes.getAdditionalMsg(2);

	    return result;
	} catch (JciException e) {
	    throw RyeException.createRyeException((RyeConnectionUrl) null, e);
	}
    }

    public int getCommandExitStatus()
    {
	return commandExitStatus;
    }

    public byte[] getCommandStdout()
    {
	return commandStdout;
    }

    public byte[] getCommandStderr()
    {
	return commandStderr;
    }

    public void setTimeout(int timeout)
    {
	this.timeout = timeout;
    }
}
