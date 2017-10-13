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

public class BrokerResponse
{
    private int resultCode;
    private short protocolVersion;
    private int[] additionalMsgSize;
    private byte[][] additionalMsg;

    BrokerResponse(short protocolVersion, int resultCode, int[] additionalMsgSize)
    {
	this.protocolVersion = protocolVersion;
	this.resultCode = resultCode;
	this.additionalMsgSize = additionalMsgSize;
	additionalMsg = null;
    }

    public int getResultCode()
    {
	return resultCode;
    }

    short getProtocolVersion()
    {
	return protocolVersion;
    }

    int getAdditionalMsgSize(int index)
    {
	if (index < additionalMsgSize.length) {
	    return additionalMsgSize[index];
	}
	else {
	    return 0;
	}
    }

    int getAdditionalMsgCount()
    {
	for (int i = 0; i < additionalMsgSize.length; i++) {
	    if (additionalMsgSize[i] > 0) {
		return additionalMsgSize.length;
	    }
	}
	return 0;
    }

    public byte[] getAdditionalMsg(int index)
    {
	if (getAdditionalMsgSize(index) > 0) {
	    return additionalMsg[index];
	}
	else {
	    return null;
	}
    }

    void setAdditionalMsg(byte[][] additionalMsg)
    {
	this.additionalMsg = additionalMsg;
    }
}
