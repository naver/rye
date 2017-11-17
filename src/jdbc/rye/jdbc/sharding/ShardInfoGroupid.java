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

import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;
import java.util.zip.CRC32;

import rye.jdbc.driver.RyeErrorCode;
import rye.jdbc.driver.RyeException;
import rye.jdbc.jci.InputBuffer;
import rye.jdbc.jci.JciConnection;
import rye.jdbc.jci.JciException;
import rye.jdbc.jci.Protocol;
import rye.jdbc.log.BasicLogger;

class ShardInfoGroupid
{
    private final short[] nodeid;
    private final long version;
    private final Random rand;
    private final ShardInfo shardInfo;

    ShardInfoGroupid(byte[] netStream, short[] oldNodeid, ShardInfo shardInfo) throws JciException
    {
	InputBuffer in = new InputBuffer(netStream);

	short[] nodeidArr;
	long version = in.readLong();
	int groupidCount = in.readInt();
	byte type = in.readByte();

	if (groupidCount <= 0) {
	    throw new JciException(RyeErrorCode.ER_SHARD_INFO_RECV_FAIL);
	}

	if (type == Protocol.BR_RES_SHARD_INFO_ALL) {
	    nodeidArr = new short[groupidCount];

	    for (int i = 0; i < groupidCount; i++) {
		nodeidArr[i] = in.readShort();
	    }
	}
	else {
	    if (oldNodeid == null || oldNodeid.length != groupidCount) {
		throw new JciException(RyeErrorCode.ER_SHARD_INFO_RECV_FAIL);
	    }

	    nodeidArr = oldNodeid.clone();

	    int changedCount = in.readInt();
	    for (int i = 0; i < changedCount; i++) {
		int groupid = in.readInt();
		short nodeid = in.readShort();

		nodeidArr[groupid - 1] = nodeid;
	    }
	}

	this.nodeid = nodeidArr;
	this.version = version;
	this.rand = new Random(System.currentTimeMillis());
	this.shardInfo = shardInfo;

	dump();
    }

    short[] getNodeidArray()
    {
	return nodeid;
    }

    ShardNodeId getRandomNodeid()
    {
	return ShardNodeId.valueOf(nodeid[rand.nextInt(nodeid.length)]);
    }

    long getVersion()
    {
	return version;
    }

    ShardNodeId[] getDistinctNodeIdArray(ShardKey[] shardKey) throws RyeException
    {
	HashSet<ShardNodeId> idSet = new HashSet<ShardNodeId>();

	for (int i = 0; i < shardKey.length; i++) {
	    idSet.add(shardKey[i].getShardNodeId());
	}

	ShardNodeId[] idArr = new ShardNodeId[idSet.size()];
	idSet.toArray(idArr);
	Arrays.sort(idArr);

	return idArr;
    }

    ShardKey[] getDistinctShardKeyArray(String[] shardKeyArr, String charset, JciConnection jciCon) throws RyeException
    {
	HashSet<ShardKey> keySet = new HashSet<ShardKey>();

	for (int i = 0; i < shardKeyArr.length; i++) {
	    keySet.add(makeShardKey(shardKeyArr[i], charset, jciCon));
	}

	ShardKey[] idArr = new ShardKey[keySet.size()];
	keySet.toArray(idArr);

	return idArr;
    }

    ShardKey makeShardKey(String shardKey, String charset, JciConnection jciCon) throws RyeException
    {
	if (shardKey == null) {
	    throw RyeException.createRyeException(jciCon, RyeErrorCode.ER_SHARD_NODE_CONNECTION_INVALID, null);
	}

	byte[] byteKey;
	try {
	    byteKey = shardKey.trim().toLowerCase().getBytes(charset);
	} catch (UnsupportedEncodingException e) {
	    throw RyeException.createRyeException(jciCon, RyeErrorCode.ER_UNKNOWN, e.getMessage(), e);
	}

	CRC32 crc = new CRC32();
	crc.update(byteKey);
	int groupid = (int) (crc.getValue() % nodeid.length + 1);

	return new ShardKey(getNodeid(groupid, jciCon), byteKey, groupid);
    }

    private short getNodeid(int groupid, JciConnection jciCon) throws RyeException
    {
	if (groupid < 1 || groupid > nodeid.length) {
	    throw RyeException.createRyeException(jciCon, RyeErrorCode.ER_SHARD_GROUPID_INVALID, null);
	}
	return nodeid[groupid - 1];
    }

    private void dump()
    {
	BasicLogger logger = shardInfo.getShardInfoManager().logger;

	if (logger != null) {
	    synchronized (logger) {
		logger.logInfo(dumpString(2));
	    }
	}
    }

    String dumpString(int level)
    {
	StringBuffer sb = new StringBuffer();
	if (level > 1) {
	    sb.append(String.format("GROUPID version = %d%n", version));
	}
	for (int i = 0; i < nodeid.length; i++) {
	    sb.append(String.format("%7d %3d%n", i + 1, nodeid[i]));
	}
	return sb.toString();
    }
}
