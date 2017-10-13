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
import rye.jdbc.jci.Protocol;

public class ShardNodeInstance
{
    /* raw data received from server */

    private final short nodeid;
    private final String dbname;
    private final String hostname;
    private final int port;
    private int ha_state;

    ShardNodeInstance(short nodeid, String dbname, String hostname, int port) throws JciException
    {
	if (dbname == null || hostname == null || nodeid <= 0 || port <= 0) {
	    throw new JciException(RyeErrorCode.ER_SHARD_INFO_RECV_FAIL);
	}

	this.nodeid = nodeid;
	this.dbname = dbname;
	this.hostname = hostname;
	this.port = port;
	this.ha_state = Protocol.HA_STATE_FOR_DRIVER_UNKNOWN;
    }

    public short getNodeid()
    {
	return nodeid;
    }

    public String getDbname()
    {
	return dbname;
    }

    public String getHost()
    {
	return hostname;
    }

    public int getPort()
    {
	return port;
    }

    public String getHaState()
    {
	switch (ha_state)
	{
	case Protocol.HA_STATE_FOR_DRIVER_MASTER:
	    return "master";
	case Protocol.HA_STATE_FOR_DRIVER_TO_BE_MASTER:
	    return "to-be-master";
	case Protocol.HA_STATE_FOR_DRIVER_SLAVE:
	    return "slave";
	case Protocol.HA_STATE_FOR_DRIVER_TO_BE_SLAVE:
	    return "to-be-slave";
	case Protocol.HA_STATE_FOR_DRIVER_REPLICA:
	    return "replica";
	case Protocol.HA_STATE_FOR_DRIVER_UNKNOWN:
	default:
	    return "unknown";
	}

    }

    void setStatus(int ha_state)
    {
	this.ha_state = ha_state;
    }
}
