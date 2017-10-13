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

package rye.jdbc.driver;

import java.sql.SQLException;

import rye.jdbc.jci.JciConnection;
import rye.jdbc.jci.JciError;
import rye.jdbc.jci.JciException;
import rye.jdbc.jci.JciNormalConnection;

public class RyeException extends SQLException
{
    private static final long serialVersionUID = -1902040094322313271L;

    public RyeException(String msg, Throwable cause)
    {
	super(msg, cause);
    }

    private RyeException(String msg, int errCode, Throwable t, JciConnection jciCon)
    {
	super(msg, null, errCode, t);

	if (jciCon != null) {
	    jciCon.logException(this);
	}
    }

    public static RyeException createRyeException(JciConnection jciCon, int errCode, Throwable t)
    {
	String errMsg = makeErrormsg(RyeErrorCode.getErrorMsg(errCode), null, jciCon, null);

	return new RyeException(errMsg, errCode, t, jciCon);
    }

    public static RyeException createRyeException(RyeConnectionUrl connUrl, int errCode, Throwable t)
    {
	String errMsg = makeErrormsg(RyeErrorCode.getErrorMsg(errCode), null, null, connUrl);

	return new RyeException(errMsg, errCode, t, null);
    }

    public static RyeException createRyeException(JciError error, RyeConnectionUrl connUrl)
    {
	JciConnection jciCon = error.getJciconnection();
	String errMsg = makeErrormsg(error.getErrorMsg(), null, jciCon, connUrl);

	return new RyeException(errMsg, error.getJdbcErrorCode(), error.getCause(), jciCon);
    }

    public static RyeException createRyeException(JciConnection jciCon, JciException e)
    {
	return createRyeException(new JciError(jciCon, e), null);
    }

    public static RyeException createRyeException(RyeConnectionUrl connUrl, JciException e)
    {
	return createRyeException(new JciError(null, e), connUrl);
    }

    public static RyeException createRyeException(JciConnection jciCon, int errCode, String msg, Throwable t)
    {
	String errMsg = makeErrormsg(RyeErrorCode.getErrorMsg(errCode), msg, jciCon, null);

	return new RyeException(errMsg, errCode, t, jciCon);
    }

    public static RyeException createRyeException(RyeConnectionUrl connUrl, int errCode, String msg, Throwable t)
    {
	String errMsg = makeErrormsg(RyeErrorCode.getErrorMsg(errCode), msg, null, connUrl);

	return new RyeException(errMsg, errCode, t, null);
    }

    public static RyeException appendException(RyeException retException, RyeException e)
    {
	if (retException == null) {
	    retException = e;
	}
	else {
	    retException.setNextException(e);
	}

	return retException;
    }

    private static String makeErrormsg(String msg1, String msg2, JciConnection jciCon, RyeConnectionUrl connUrl)
    {
	String errorMessage = (msg2 == null ? msg1 : msg1 + msg2);

	if (jciCon != null) {
	    if (jciCon instanceof JciNormalConnection) {
		return String.format("%s[LOCAL CONN-%s][CAS INFO-%s]", errorMessage, jciCon.getUrlForLogging(),
				jciCon.getCasInfoString());
	    }
	    else {
		return String.format("%s[GLOBAL CONN-%s]", errorMessage, jciCon.getUrlForLogging());
	    }
	}
	else if (connUrl != null) {
	    return String.format("%s[URL-%s]", errorMessage, connUrl.getUrlForLogging());
	}
	else {
	    return String.format("%s", errorMessage);
	}
    }
}
