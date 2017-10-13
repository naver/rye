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

import rye.jdbc.driver.RyeErrorCode;

public class JciError
{
    private JciConnection connection = null;
    private int jciErrorCode;
    private int serverErrorCode;
    private String errorMessage;
    private Throwable cause;
    private JciException jciException;

    public JciError(JciConnection c, int errCode)
    {
	connection = c;
	setErrorCode(errCode, null);
    }

    JciError()
    {
	this(null, RyeErrorCode.ER_NO_ERROR);
    }

    JciError(JciConnection c)
    {
	this(c, RyeErrorCode.ER_NO_ERROR);
    }

    public JciError(JciConnection c, int errCode, Throwable t)
    {
	this(c, errCode);
	this.cause = t;
    }

    public JciError(JciConnection c, JciException e)
    {
	connection = c;
	this.setUJciException(e);
	this.cause = e;
    }

    int getErrorCode()
    {
	return jciErrorCode;
    }

    /*
    private int getSessionNumber(byte[] session)
    {
	int ch1 = session[8];
	int ch2 = session[9];
	int ch3 = session[10];
	int ch4 = session[11];

	return ((ch1 << 24) + (ch2 << 16) + (ch3 << 8) + (ch4 << 0));
    }
*/
    public String getErrorMsg()
    {
	return errorMessage;
    }

    public int getJdbcErrorCode()
    {
	if (jciErrorCode == RyeErrorCode.ER_NO_ERROR)
	    return RyeErrorCode.ER_NO_ERROR;
	else if (jciErrorCode == RyeErrorCode.ER_DBMS)
	    return serverErrorCode;
	else
	    return (jciErrorCode);
    }

    void setUJciException(JciException jciException)
    {
	this.cause = jciException;

	if (jciException.jciErrCode == RyeErrorCode.ER_DBMS) {
	    String msg;
	    if (jciException.serverErrIndicator == RyeErrorCode.DBMS_ERROR_INDICATOR) {
		msg = jciException.getMessage();
	    }
	    else {
		msg = RyeErrorCode.getCasErrorMsg(jciException.serverErrCode);
	    }

	    this.jciErrorCode = RyeErrorCode.ER_DBMS;
	    this.serverErrorCode = jciException.serverErrCode;
	    this.errorMessage = msg;
	}
	else if (jciException.jciErrCode == RyeErrorCode.ER_JCI_UNKNOWN) {
	    this.setErrorMessage(jciException.jciErrCode, jciException.getMessage());
	}
	else {
	    this.setErrorCode(jciException.jciErrCode, null);
	}

	this.jciException = jciException;
    }

    void setErrorCode(int code, Throwable t)
    {
	jciErrorCode = code;
	if (code != RyeErrorCode.ER_NO_ERROR) {
	    errorMessage = RyeErrorCode.getErrorMsg(code);
	}

	this.cause = t;

	this.jciException = null;
    }

    private void setErrorMessage(int code, String addMessage)
    {
	setErrorCode(code, null);
	errorMessage += ":" + addMessage;
    }

    void clear()
    {
	jciErrorCode = RyeErrorCode.ER_NO_ERROR;
    }

    public Throwable getCause()
    {
	return cause;
    }
    
    public JciConnection getJciconnection()
    {
	return connection;
    }

    JciException getUJciException()
    {
	return this.jciException;
    }
}
