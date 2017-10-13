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

import java.sql.BatchUpdateException;
import java.sql.SQLException;

public class JciBatchResult
{
    private boolean errorFlag;
    private int resultCount;
    private int result[];
    private int statementType[];
    private int errorCode[];
    private String errorMessage[];

    public JciBatchResult(int maxCount)
    {
	result = new int[maxCount];
	statementType = new int[maxCount];
	errorCode = new int[maxCount];
	errorMessage = new String[maxCount];
	resultCount = 0;
	errorFlag = false;
    }

    public void setResult(int index, byte stmt, int res)
    {
	if (index < 0 || index >= result.length)
	    return;

	result[index] = res;
	errorCode[index] = 0;
	errorMessage[index] = null;
	statementType[index] = stmt;

	if (resultCount <= index) {
	    resultCount = index + 1;
	}
    }

    public void setResultError(int index, int code, String message)
    {
	if (index < 0 || index >= result.length)
	    return;
	result[index] = -3;
	errorCode[index] = code;
	errorMessage[index] = message;
	errorFlag = true;

	if (resultCount <= index) {
	    resultCount = index + 1;
	}
    }

    public int[] checkBatchResult() throws SQLException
    {
	int[] res = new int[resultCount];
	System.arraycopy(result, 0, res, 0, resultCount);

	if (errorFlag == false) {
	    return res;
	}

	BatchUpdateException bex = null;
	for (int i = 0; i < resultCount; i++) {
	    if (result[i] < 0) {
		if (bex == null) {
		    bex = new BatchUpdateException(errorMessage[i], null, errorCode[i], res);
		}
		else {
		    bex.setNextException(new SQLException(errorMessage[i], null, errorCode[i]));
		}
	    }
	}
	throw bex;
    }
}
