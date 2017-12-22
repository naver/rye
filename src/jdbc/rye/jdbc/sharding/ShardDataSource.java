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

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Random;

import rye.jdbc.driver.ConnectionProperties;
import rye.jdbc.driver.RyeConnectionUrl;
import rye.jdbc.driver.RyeErrorCode;
import rye.jdbc.driver.RyeException;
import rye.jdbc.jci.JciConnectionInfo;
import rye.jdbc.jci.JciException;

class ShardDataSource
{
    private final RyeConnectionUrl[] connUrlArray;
    private int connUrlIndex = 0;
    private final String servicePortName;
    private final Random rand;
    private final ArrayList<JciConnectionInfo> conInfoList;
    private final String dbname;
    private LinkedList<JciShardConnection> availableConns;
    private int totalConns;
    private int purgeRetryCount;

    ShardDataSource(ArrayList<JciConnectionInfo> conInfoList, String dbname, String servicePortName)
    {
	this.connUrlArray = initUrl(conInfoList, dbname);
	this.conInfoList = conInfoList;
	this.rand = new Random();
	this.dbname = dbname;

	this.servicePortName = servicePortName;
	availableConns = new LinkedList<JciShardConnection>();
	totalConns = 0;
	purgeRetryCount = -1;
    }

    private RyeConnectionUrl[] initUrl(ArrayList<JciConnectionInfo> conInfoList, String dbname)
    {
	RyeConnectionUrl[] urlArray = null;

	if (conInfoList.size() <= 3) {
	    JciConnectionInfo[] conInfoArr = new JciConnectionInfo[conInfoList.size()];
	    conInfoList.toArray(conInfoArr);

	    ArrayList<RyeConnectionUrl> allUrlList = new ArrayList<RyeConnectionUrl>();
	    makeUrlPermutation(allUrlList, dbname, conInfoArr, 0, conInfoArr.length - 1);

	    urlArray = new RyeConnectionUrl[allUrlList.size()];
	    allUrlList.toArray(urlArray);
	}

	return urlArray;
    }

    private void makeUrlPermutation(ArrayList<RyeConnectionUrl> allUrlList, String dbname, JciConnectionInfo[] arr,
		    int start, int end)
    {
	if (start >= end) {
	    ArrayList<JciConnectionInfo> conInfoList = new ArrayList<JciConnectionInfo>();
	    for (int i = 0; i < arr.length; i++) {
		conInfoList.add(arr[i]);
	    }
	    allUrlList.add(new RyeConnectionUrl(conInfoList, dbname, "", "", null, null));
	    return;
	}
	for (int i = start; i <= end; i++) {
	    swap(arr, start, i);
	    makeUrlPermutation(allUrlList, dbname, arr, start + 1, end);
	    swap(arr, i, start);
	}

    }

    synchronized private RyeConnectionUrl getConnUrl()
    {
	if (connUrlArray == null) {
	    Collections.shuffle(this.conInfoList, rand);
	    ArrayList<JciConnectionInfo> conList = new ArrayList<JciConnectionInfo>();
	    for (JciConnectionInfo info : this.conInfoList) {
		conList.add(info);
	    }
	    return new RyeConnectionUrl(conList, dbname, "", "", null, null);
	}
	else {
	    if (connUrlIndex >= connUrlArray.length) {
		connUrlIndex = 0;
	    }
	    return connUrlArray[connUrlIndex++];
	}
    }

    private void swap(JciConnectionInfo[] arr, int x, int y)
    {
	JciConnectionInfo tmp = arr[x];
	arr[x] = arr[y];
	arr[y] = tmp;
    }

    void returnToPool(JciShardConnection con) throws RyeException
    {
	if (con != null) {
	    con.endTranRequest(false);

	    synchronized (this) {
		availableConns.addFirst(con);
	    }
	}
    }

    JciShardConnection getConnection(String user, String passwd, ConnectionProperties conProperties,
		    ShardNodeId nodeid, boolean autoCommit) throws RyeException
    {
	JciShardConnection con;

	synchronized (this) {
	    if (availableConns.size() > 0) {
		con = availableConns.removeFirst();
	    }
	    else {
		con = null;
		totalConns++;
	    }
	}

	if (con == null) {
	    RyeConnectionUrl connUrl = getConnUrl();
	    try {
		con = new JciShardConnection(connUrl, conProperties, this, nodeid, user, passwd);
	    } catch (JciException e) {
		throw RyeException.createRyeException(connUrl, e);
	    }
	}
	else {
	    try {
		con.setConnectionProperties(conProperties);
		con.changeUser(user, passwd);
	    } catch (RyeException e) {
		con.remove();
		throw e;
	    }
	}

	con.setAutoCommit(autoCommit);

	if (nodeid.equals(con.getShardNodeid()) == false) {
	    String msg = String.format("connection pool error. nodeid:%d,%d", nodeid.getNodeId(), con.getShardNodeid()
			    .getNodeId());
	    throw RyeException.createRyeException(con, RyeErrorCode.ER_SHARD_INTERNAL_ERROR, msg, null);
	}

	return con;
    }

    int purge()
    {
	while (true) {
	    JciShardConnection con = null;

	    synchronized (this) {
		if (availableConns.size() > 0) {
		    con = availableConns.removeFirst();
		}
		else {
		    break;
		}
	    }

	    closePooledConnection(con);
	}

	purgeRetryCount++;
	if (purgeRetryCount > 10) {
	    // Maybe there are lost connections. give up purge
	    return -1;
	}
	else {
	    return getTotalConns();
	}
    }

    synchronized int getTotalConns()
    {
	return this.totalConns;
    }

    synchronized int getAvailableConns()
    {
	return this.availableConns.size();
    }

    private void closePooledConnection(JciShardConnection con)
    {
	if (con == null)
	    return;

	try {
	    con.remove();
	} catch (Exception e) {
	}

	synchronized (this) {
	    totalConns--;
	}
    }

    String getServicePortName()
    {
	return servicePortName;
    }

}
