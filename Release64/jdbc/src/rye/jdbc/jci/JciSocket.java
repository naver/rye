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

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;

import rye.jdbc.driver.RyeErrorCode;

public class JciSocket
{
    private static int DEFAULT_SO_TIMEOUT = 5000;

    private Socket socket;
    private final TimedDataInputStream input;
    private final DataOutputStream output;

    public JciSocket(JciConnectionInfo conInfo, int timeout, int soTimeout) throws JciException
    {
	int connectTimeout = 5000;
	if (timeout > 0 && timeout < connectTimeout) {
	    connectTimeout = timeout;
	}

	try {
	    socket = new Socket();
	    InetSocketAddress brokerAddress = new InetSocketAddress(conInfo.getHostAddress(), conInfo.getPort());

	    try {
		socket.connect(brokerAddress, connectTimeout);
	    } catch (SocketTimeoutException e) {
		if (socket.isClosed() == false) {
		    socket.close();
		    socket = null;
		}

		if (timeout <= 0) {
		    connectTimeout = 30000;
		}
		else {
		    connectTimeout = timeout - connectTimeout;
		    if (connectTimeout <= 0) {
			throw new JciException(RyeErrorCode.ER_TIMEOUT);
		    }
		}

		socket = new Socket();
		socket.connect(brokerAddress, connectTimeout);
	    }

	    if (soTimeout <= 0) {
		soTimeout = DEFAULT_SO_TIMEOUT;
	    }

	    socket.setTcpNoDelay(true);
	    socket.setSoTimeout(soTimeout);
	    socket.setKeepAlive(true);
	    input = new TimedDataInputStream(socket.getInputStream(), conInfo, timeout);
	    output = new DataOutputStream(socket.getOutputStream());

	} catch (SocketTimeoutException e) {
	    close();
	    throw new JciException(RyeErrorCode.ER_TIMEOUT, e);
	} catch (IOException e) {
	    close();
	    throw new JciException(RyeErrorCode.ER_CONNECTION, e);
	} catch (JciException e) {
	    close();
	    throw e;
	} finally {
	}
    }

    void setTimeout(int timeout)
    {
	input.setTimeout(timeout);
    }

    public TimedDataInputStream getInputStream()
    {
	return input;
    }

    public DataOutputStream getOutputStream()
    {
	return output;
    }

    public void close()
    {
	try {
	    if (input != null) {
		input.close();
	    }
	} catch (IOException e) {
	}

	try {
	    if (output != null) {
		output.close();
	    }
	} catch (IOException e) {
	}

	if (socket != null) {
	    try {
		socket.setSoLinger(true, 0);
	    } catch (IOException e) {
	    }

	    try {
		socket.close();
	    } catch (IOException e) {
	    }
	}
    }
}
