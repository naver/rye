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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;

import rye.jdbc.driver.RyeErrorCode;
import rye.jdbc.driver.RyeException;

public class JciConnectionInfo
{
    private final InetAddress hostAddress;
    private final String hostname;
    private final int port;
    private final byte[] portNameMsgProtocol;
    private final String portName;

    public JciConnectionInfo(String host, int port, String portName) throws RyeException
    {
	try {
	    this.hostAddress = InetAddress.getByName(host);
	} catch (UnknownHostException e) {
	    throw RyeException.createRyeException((JciConnection) null, RyeErrorCode.ER_INVALID_URL, "UnknownHost: "
			    + host, null);
	}
	this.hostname = host;
	this.port = port;
	this.portName = portName;
	this.portNameMsgProtocol = Protocol.packPortName(portName);
    }

    private JciConnectionInfo(JciConnectionInfo src, String portName)
    {
	this.hostAddress = src.hostAddress;
	this.hostname = src.hostname;
	this.port = src.port;
	this.portName = portName;
	this.portNameMsgProtocol = Protocol.packPortName(portName);
    }

    public static ArrayList<JciConnectionInfo> copyList(ArrayList<JciConnectionInfo> srcList, String portName)
    {
	ArrayList<JciConnectionInfo> newList = new ArrayList<JciConnectionInfo>();

	for (JciConnectionInfo conInfo : srcList) {
	    newList.add(new JciConnectionInfo(conInfo, portName));
	}
	return newList;
    }

    public int hashCode()
    {
	return (hostAddress.hashCode() + port);
    }

    public boolean equals(Object obj)
    {
	if (obj instanceof JciConnectionInfo) {
	    JciConnectionInfo otherJciConInfo = (JciConnectionInfo) obj;
	    if (hostAddress.equals(otherJciConInfo.hostAddress) == false) {
		return false;
	    }

	    if (port != otherJciConInfo.port)
		return false;

	    return Arrays.equals(portNameMsgProtocol, otherJciConInfo.portNameMsgProtocol);
	}
	else {
	    return false;
	}
    }

    public boolean hasSameHost(JciConnectionInfo[] conInfoArr)
    {
	for (int i = 0; i < conInfoArr.length; i++) {
	    if (equals(conInfoArr[i])) {
		return true;
	    }
	}

	return false;
    }

    public InetAddress getHostAddress()
    {
	return hostAddress;
    }

    public String getHostname()
    {
	return hostname;
    }

    public int getPort()
    {
	return port;
    }

    public byte[] portNameMsgProtocol()
    {
	return portNameMsgProtocol;
    }

    public String getPortName()
    {
	return portName;
    }
}
