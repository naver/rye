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

class CasInfo
{
    private int id;
    private int pid;
    private short protocolVersion;
    private byte[] dbSessionId;
    private byte dbms;
    private byte holdableResult;
    private byte statementPooling;
    private byte[] statusInfo;
    private short serverNodeid;
    private long serverShardVersion;

    CasInfo()
    {
	statusInfo = new byte[Protocol.CAS_STATUS_INFO_SIZE];
	statusInfo[Protocol.CAS_STATUS_INFO_IDX_STATUS] = Protocol.CAS_INFO_STATUS_INACTIVE;

	set((short) 0, 0, 0, Protocol.createNullSessionId(), Protocol.CAS_DBMS_RYE,
		Protocol.CAS_HOLDABLE_RESULT_SUPPORT, Protocol.CAS_STATEMENT_POOLING_ON);
    }

    void set(short version, int id, int pid, byte[] session, byte dbms, byte holdable, byte pooling)
    {
	this.protocolVersion = version;
	this.id = id;
	this.pid = pid;
	this.dbSessionId = session;
	this.dbms = dbms;
	this.holdableResult = holdable;
	this.statementPooling = pooling;
    }

    int getId()
    {
	return id;
    }

    int getPid()
    {
	return pid;
    }

    short getProtocolVersion()
    {
	return protocolVersion;
    }

    byte[] getDbSessionId()
    {
	return dbSessionId;
    }

    byte getDbms()
    {
	return dbms;
    }

    byte getHoldableResult()
    {
	return holdableResult;
    }

    byte getStatementPooling()
    {
	return statementPooling;
    }

    byte[] getStatusInfo()
    {
	return statusInfo;
    }

    void setStatusInfo(byte[] src, int off, int len)
    {
	System.arraycopy(src, off, statusInfo, 0, len);
	serverNodeid = JciUtil.bytes2short(statusInfo, Protocol.CAS_STATUS_INFO_IDX_SERVER_NODEID);
	serverShardVersion = JciUtil.bytes2long(statusInfo, Protocol.CAS_STATUS_INFO_IDX_SHARD_INFO_VER);
    }

    void setStatusInfoStatus(byte status)
    {
	statusInfo[Protocol.CAS_STATUS_INFO_IDX_STATUS] = status;
    }
    
    byte getStatusInfoStatus()
    {
	return statusInfo[Protocol.CAS_STATUS_INFO_IDX_STATUS];
    }
    
    short getStatusInfoServerNodeid()
    {
	return serverNodeid;
    }
    
    long getStatusInfoServerShardInfoVersion()
    {
	return serverShardVersion;
    }
}
