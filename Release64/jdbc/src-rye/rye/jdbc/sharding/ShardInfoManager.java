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
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;

import rye.jdbc.log.BasicLogger;

public class ShardInfoManager
{
    private class ShardInfoManagerThread extends Thread
    {
	public void run()
	{

	    while (true) {
		ShardInfo[] shardInfoArr = null;

		synchronized (shardInfoMap) {
		    try {
			if (shardInfoSyncRequest == false) {
			    shardInfoMap.wait(SYNC_INTERVAL_MILLISEC);
			}
		    } catch (InterruptedException e) {
		    }

		    shardInfoArr = new ShardInfo[shardInfoMap.size()];
		    shardInfoMap.values().toArray(shardInfoArr);

		    shardInfoSyncRequest = false;
		}

		/* sort with ready flag value. false, true order */
		Arrays.sort(shardInfoArr, shardInfoComparator);

		syncAll(shardInfoArr);
	    }
	}
    }

    private class ShardInfoFlagComparator implements Comparator<ShardInfo>
    {
	public int compare(ShardInfo s1, ShardInfo s2)
	{
	    return ((s1.isReady() ? 1 : 0) - (s2.isReady() ? 1 : 0));
	}
    }

    public static ShardInfoManager defaultShardInfoManager;

    static {
	defaultShardInfoManager = new ShardInfoManager(true);
    }

    static final int MAX_SERVICE_BROKER = 10;
    static final int SYNC_WAIT_MILLISEC = 10000;
    public static final int SYNC_INTERVAL_MILLISEC = 10000;

    private final HashMap<String, ShardInfo> shardInfoMap;
    private boolean shardInfoSyncRequest = false;
    public BasicLogger logger;
    private final ArrayList<ShardDataSource> obsoleteDsList;
    private ShardInfoManagerThread managerThread;
    private final ShardInfoFlagComparator shardInfoComparator;

    public ShardInfoManager()
    {
	shardInfoMap = new HashMap<String, ShardInfo>(10);
	obsoleteDsList = new ArrayList<ShardDataSource>();
	shardInfoComparator = new ShardInfoFlagComparator();
    }

    private ShardInfoManager(boolean runManagerThread)
    {
	this();

	if (runManagerThread) {
	    managerThread = new ShardInfoManagerThread();
	    managerThread.setDaemon(true);
	    managerThread.setContextClassLoader(null);
	    managerThread.start();
	}
    }

    public void clear()
    {
	ShardInfo[] shardInfoArr = null;

	synchronized (shardInfoMap) {
	    shardInfoArr = new ShardInfo[shardInfoMap.size()];
	    shardInfoMap.values().toArray(shardInfoArr);

	    for (int i = 0; i < shardInfoArr.length; i++) {
		shardInfoArr[i].clear(obsoleteDsList);
	    }

	    shardInfoMap.clear();
	}

	purgeObsoleteDs();

	obsoleteDsList.clear();
    }

    private void purgeObsoleteDs()
    {
	synchronized (obsoleteDsList) {
	    for (int i = obsoleteDsList.size() - 1; i >= 0; i--) {
		ShardDataSource ds = obsoleteDsList.get(i);
		int remainConns = ds.purge();
		if (remainConns <= 0) {
		    obsoleteDsList.remove(i);
		}
	    }
	}
    }

    private void syncAll(ShardInfo[] shardInfoArr)
    {
	for (int i = 0; i < shardInfoArr.length; i++) {
	    shardInfoArr[i].sync(obsoleteDsList);
	}

	purgeObsoleteDs();

	for (int i = 0; i < shardInfoArr.length; i++) {
	    shardInfoArr[i].dump();
	}
    }

    public boolean syncAllRequest()
    {
	synchronized (shardInfoMap) {
	    if (managerThread == null) {
		ShardInfo[] shardInfoArr = new ShardInfo[shardInfoMap.size()];
		shardInfoMap.values().toArray(shardInfoArr);

		syncAll(shardInfoArr);
		return false;
	    }
	    else {
		shardInfoSyncRequest = true;
		shardInfoMap.notify();
		return true;
	    }
	}
    }

    public ShardInfo getShardInfo(ShardMgmtConnectionInfo conInfo, String shardLogfile)
    {
	ShardInfo shardInfo = null;

	String dbname = conInfo.getDbname();

	synchronized (shardInfoMap) {
	    if (logger == null && shardLogfile != null) {
		logger = new BasicLogger(shardLogfile);
	    }

	    shardInfo = shardInfoMap.get(dbname);
	    if (shardInfo == null) {
		shardInfo = new ShardInfo(conInfo, this);
		shardInfoMap.put(dbname, shardInfo);
	    }
	}

	if (shardInfo.needSync(conInfo)) {
	    syncAllRequest();
	}

	if (managerThread != null) {
	    synchronized (shardInfo) {
		while (shardInfo.isReady() == false) {
		    try {
			shardInfo.incrWaiterCount();
			shardInfo.wait(SYNC_WAIT_MILLISEC);
			shardInfo.decrWaiterCount();
			break;
		    } catch (InterruptedException e) {
		    }
		}
	    }
	}

	if (shardInfo.isReady() == false) {
	    return null;
	}

	return shardInfo;
    }
}
