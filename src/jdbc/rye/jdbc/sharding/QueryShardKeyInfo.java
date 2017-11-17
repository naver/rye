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

import java.io.PrintStream;
import java.util.Arrays;

import rye.jdbc.driver.RyeErrorCode;
import rye.jdbc.driver.RyeException;
import rye.jdbc.jci.BindParameter;
import rye.jdbc.jci.JciConnection;

public class QueryShardKeyInfo
{
    private static final String[] emptyStringArr = new String[0];
    private static final int[] emptyIntArr = new int[0];

    private final String[] shardKeyValues;
    private final int[] shardKeyParameters;
    private final boolean containsShardTable;

    public QueryShardKeyInfo(boolean shardTable, String[] keyValues, int[] keyPos)
    {
	this.containsShardTable = shardTable;

	if (keyValues == null) {
	    this.shardKeyValues = emptyStringArr;
	}
	else {
	    Arrays.sort(keyValues);
	    this.shardKeyValues = keyValues;
	}

	if (keyPos == null) {
	    this.shardKeyParameters = emptyIntArr;
	}
	else {
	    Arrays.sort(keyPos);
	    this.shardKeyParameters = keyPos;
	}
    }

    boolean equals(QueryShardKeyInfo otherKey)
    {
	if (this.containsShardTable != otherKey.containsShardTable) {
	    return false;
	}
	if (Arrays.equals(this.shardKeyValues, otherKey.shardKeyValues) == false) {
	    return false;
	}
	if (Arrays.equals(this.shardKeyParameters, otherKey.shardKeyParameters) == false) {
	    return false;
	}

	return true;
    }

    boolean isShardTableQuery()
    {
	return containsShardTable;
    }

    int getShardKeyCount()
    {
	return shardKeyValues.length + shardKeyParameters.length;
    }

    String[] getShardKeys(BindParameter bindParam, JciConnection jciCon) throws RyeException
    {
	if (bindParam == null) {
	    return shardKeyValues;
	}

	String[] values = new String[shardKeyValues.length + shardKeyParameters.length];

	int index = 0;
	for (int i = 0; i < shardKeyValues.length; i++) {
	    values[index++] = shardKeyValues[i];
	}

	Object[] bindValues = bindParam.getValues();

	for (int i = 0; i < shardKeyParameters.length; i++) {
	    int bindIndex = shardKeyParameters[i];
	    if (bindValues[bindIndex] instanceof String) {
		values[index++] = (String) bindValues[bindIndex];
	    }
	    else {
		throw RyeException.createRyeException(jciCon, RyeErrorCode.ER_SHARD_INVALID_SHARDKEY_TYPE,
				"- bind type:" + bindValues[i].getClass().getName(), null);
	    }
	}

	return values;
    }

    public void dump(PrintStream out)
    {
	out.printf("shardTable: %s%n", containsShardTable);
	out.printf("sharKeyValues%n");
	for (int i = 0; i < shardKeyValues.length; i++) {
	    out.println(shardKeyValues[i]);
	}
	out.printf("dshardKeyparameters%n");
	for (int i = 0; i < shardKeyParameters.length; i++) {
	    out.println(shardKeyParameters[i]);
	}
	out.println("-------------");
    }
}
