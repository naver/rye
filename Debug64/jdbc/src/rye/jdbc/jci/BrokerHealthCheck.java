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

import java.io.IOException;

import rye.jdbc.driver.RyeConnectionUrl;
import rye.jdbc.driver.RyeDriver;
import rye.jdbc.driver.RyeErrorCode;
import rye.jdbc.jci.BrokerHandler;

public class BrokerHealthCheck extends Thread
{
    private static final int BROKER_HEALTH_CHECK_TIMEOUT = 5000;
    public static final int MONITORING_INTERVAL = 60000;
    private OutputBuffer outBuffer = new OutputBuffer(null);
    private static byte[] sessionId = Protocol.createNullSessionId();
    private static CasInfo casInfo = new CasInfo();

    public void run()
    {
	long startTime, elapseTime;

	while (true) {
	    startTime = System.currentTimeMillis();

	    if (RyeDriver.unreachableHosts == null) {
		return;
	    }
	    if (RyeDriver.unreachableHosts.size() > 0) {
		for (JciConnectionInfo conInfo : RyeDriver.unreachableHosts) {
		    try {
			checkBrokerAlive(conInfo, BROKER_HEALTH_CHECK_TIMEOUT);
			RyeDriver.unreachableHosts.remove(conInfo);
		    } catch (JciException e) {
			// do nothing
		    } catch (IOException e) {
			// do nothing
		    }
		}
	    }
	    elapseTime = System.currentTimeMillis() - startTime;

	    if (elapseTime < MONITORING_INTERVAL) {
		try {
		    Thread.sleep(MONITORING_INTERVAL - elapseTime);
		} catch (InterruptedException e) {
		    // do nothing
		}
	    }
	}
    }

    private void checkBrokerAlive(JciConnectionInfo conInfo, int timeout) throws IOException, JciException
    {
	JciSocket toBroker = null;
	String url = RyeConnectionUrl.makeJdbcUrl(conInfo, Protocol.HEALTH_CHECK_DUMMY_DB, "", "********", null);

	long startTime = System.currentTimeMillis();

	try {
	    toBroker = BrokerHandler.connectBroker(conInfo, timeout, timeout);
	    if (timeout > 0) {
		timeout -= (System.currentTimeMillis() - startTime);
		if (timeout <= 0) {
		    throw new JciException(RyeErrorCode.ER_TIMEOUT);
		}
	    }

	    outBuffer.newRequest(toBroker.getOutputStream(), Protocol.CAS_FC_CONNECT_DB);
	    outBuffer.addString(Protocol.HEALTH_CHECK_DUMMY_DB, false);
	    outBuffer.addString("", false);
	    outBuffer.addString("", false);
	    outBuffer.addString(url, false);
	    outBuffer.addString(RyeDriver.version_string, false);
	    outBuffer.addBytes(sessionId);
	    outBuffer.sendData(casInfo.getStatusInfo());

	    InputBuffer inBuffer = new InputBuffer(toBroker.getInputStream(), null, casInfo);

	    inBuffer.readShort(); // protocol version

	    inBuffer = null;
	    outBuffer.clear();
	} finally {
	    if (toBroker != null)
		toBroker.close();
	}
    }
}
