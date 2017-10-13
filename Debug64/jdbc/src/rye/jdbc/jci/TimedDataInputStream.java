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

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.SocketTimeoutException;

import rye.jdbc.driver.RyeErrorCode;

class TimedDataInputStream
{
    public final static int PING_TIMEOUT = 5000;
    private DataInputStream stream = null;
    private JciConnectionInfo conInfo = null;
    private int timeout = 0;

    TimedDataInputStream(InputStream stream, JciConnectionInfo conInfo)
    {
	this(stream, conInfo, 0);
    }

    TimedDataInputStream(InputStream stream, JciConnectionInfo conInfo, int timeout)
    {
	this.stream = new DataInputStream(stream);
	this.conInfo = conInfo;
	this.timeout = timeout;
    }

    void setTimeout(int timeout)
    {
	this.timeout = timeout;
    }

    private int readInt(int timeout) throws IOException, JciException
    {
	long begin = System.currentTimeMillis();

	while (true) {
	    try {
		return stream.readInt();
	    } catch (SocketTimeoutException e) {
		if (timeout > 0 && timeout - (System.currentTimeMillis() - begin) <= 0) {
		    throw new JciException(RyeErrorCode.ER_TIMEOUT, e);
		}
		BrokerHandler.pingBroker(conInfo, PING_TIMEOUT);
	    }
	}
    }

    int readInt() throws IOException, JciException
    {
	return readInt(timeout);
    }

    void readFully(byte[] b) throws IOException, JciException
    {
	readFully(b, timeout);
    }

    private void readFully(byte[] b, int timeout) throws IOException, JciException
    {
	long begin = System.currentTimeMillis();

	while (true) {
	    try {
		stream.readFully(b);
		return;
	    } catch (SocketTimeoutException e) {
		if (timeout > 0 && timeout - (System.currentTimeMillis() - begin) <= 0) {
		    throw new JciException(RyeErrorCode.ER_TIMEOUT, e);
		}
		BrokerHandler.pingBroker(conInfo, PING_TIMEOUT);
	    }
	}
    }

    private int read(byte[] b, int off, int len, int timeout) throws IOException, JciException
    {
	long begin = System.currentTimeMillis();

	while (true) {
	    try {
		return stream.read(b, off, len);
	    } catch (SocketTimeoutException e) {
		if (timeout > 0 && timeout - (System.currentTimeMillis() - begin) <= 0) {
		    throw new JciException(RyeErrorCode.ER_TIMEOUT, e);
		}
		BrokerHandler.pingBroker(conInfo, PING_TIMEOUT);
	    }
	}
    }

    int read(byte[] b, int off, int len) throws IOException, JciException
    {
	return read(b, off, len, timeout);
    }

    void close() throws IOException
    {
	stream.close();
    }

}
