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

class ShardNodeId implements Comparable<ShardNodeId>
{
    private static final int CACHE_SIZE = 128;

    private static class NodeidCache
    {
	private NodeidCache()
	{
	}

	static final ShardNodeId[] cache = new ShardNodeId[CACHE_SIZE];

	static {
	    for (int i = 0; i < CACHE_SIZE; i++) {
		cache[i] = new ShardNodeId((short) i);
	    }
	}
    }

    private final short nodeid;

    private ShardNodeId(short nodeid)
    {
	this.nodeid = nodeid;
    }

    static ShardNodeId valueOf(short s)
    {
	if (s >= (short) 0 && s < (short) CACHE_SIZE) {
	    return NodeidCache.cache[s];
	}
	return new ShardNodeId(s);
    }

    short getNodeId()
    {
	return nodeid;
    }

    /*
     * override java.lang.Object
     */

    public int hashCode()
    {
	return (int) nodeid;
    }

    public boolean equals(Object obj)
    {
	if (obj instanceof ShardNodeId) {
	    return this.nodeid == ((ShardNodeId) obj).nodeid;
	}
	return false;
    }

    /*
     * java.lang.Comparable interface
     */
    public int compareTo(ShardNodeId id)
    {
	return (this.nodeid - id.nodeid);
    }
}
