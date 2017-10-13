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

public class JciException extends Exception
{
    /*
     * jci internal exception jci methods called by rye.jdbc.driver classes should throw RyeExeption
     */

    private static final long serialVersionUID = 4464106407657785825L;

    int jciErrCode;
    int serverErrCode;
    int serverErrIndicator;

    public JciException(int err)
    {
	super();
	jciErrCode = err;
    }

    JciException(int err, Throwable t)
    {
	super(t == null ? null : t.getMessage());
	jciErrCode = err;

	if (t != null) {
	    setStackTrace(t.getStackTrace());
	}
    }

    JciException(int err, int indicator, int srv_err, String msg)
    {
	super(msg);
	jciErrCode = err;
	serverErrCode = srv_err;
	serverErrIndicator = indicator;
    }

    static JciException createJciException(JciError uerror)
    {
	JciException ex = uerror.getUJciException();
	if (ex == null) {
	    return new JciException(uerror.getErrorCode(), uerror.getCause());
	}
	else {
	    return ex;
	}
    }

    public static JciException createJciException(JciConnection con, int err)
    {
	JciException e = new JciException(err);
	if (con != null) {
	    con.logException(e);
	}
	return e;
    }

    public int getJciError()
    {
	return this.jciErrCode;
    }

    public String toString()
    {
	String msg, indicator;
	int errorCode;
	if (jciErrCode == RyeErrorCode.ER_DBMS) {
	    if (serverErrIndicator == RyeErrorCode.DBMS_ERROR_INDICATOR) {
		msg = getMessage();
		indicator = "ER_DBMS";
		errorCode = serverErrCode;
	    }
	    else {
		msg = RyeErrorCode.getCasErrorMsg(serverErrCode);
		indicator = "ER_BROKER";
		errorCode = jciErrCode;
	    }
	}
	else {
	    msg = getMessage();
	    indicator = "ER_DRIVER";
	    errorCode = jciErrCode;
	}
	return String.format("%s[%d,%s]", indicator, errorCode, msg);
    }
}
