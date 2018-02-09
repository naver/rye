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

package rye.jdbc.admin;

import rye.jdbc.driver.RyeConnectionUrl;
import rye.jdbc.driver.RyeErrorCode;
import rye.jdbc.driver.RyeException;

public class RyeConfValue
{
    static final byte CHANGE_CONF_DISABLE_AUTO_GEN_PARAM = 0x01;
    static final byte CHANGE_CONF_DISABLE_SHARD_MGMT_PARAM = 0x02;
    static final byte CHANGE_CONF_ENABLE_ALL = 0;
    static final byte CHANGE_CONF_ENABLE_NORMAL = CHANGE_CONF_DISABLE_AUTO_GEN_PARAM
		    | CHANGE_CONF_DISABLE_SHARD_MGMT_PARAM;

    static final String PROC_BROKER = "broker";
    static final String SECT_SHARD_MGMT = "shard_mgmt";
    static final String PROC_SERVER = "server";
    static final String SECT_COMMON = "common";
    static final String KEY_HA_NODE_LIST = "ha_node_list";
    static final String KEY_HA_DB_LIST = "ha_db_list";
    static final String KEY_HA_NODE_MYSELF = "ha_node_myself";

    static final String KEY_SHARD_MGMT_METADB = "shard_mgmt_metadb";
    static final String KEY_SHARD_MGMT_NUM_MIGRATOR = "shard_mgmt_num_migrator";
    static final String DEFAULT_VALUE_SHARD_MGMT_NUM_MIGRATOR = "10";

    private static final String[] AUTO_GEN_PARAM = { KEY_HA_NODE_LIST, KEY_HA_DB_LIST, KEY_HA_NODE_MYSELF,
		    KEY_SHARD_MGMT_METADB, "rye_port_id", "rye_shm_key" };

    protected static final String[] builtinBrokers = { "broker", SECT_SHARD_MGMT, "rw", "ro", "so", "repl" };

    private final String procName;
    private final String sectName;
    private final String keyName;
    private final String value;
    private final boolean isShardMgmtParam;
    private final boolean isAutoGenParam;

    RyeConfValue(String procName, String sectName, String keyName, String value) throws RyeException
    {
	if (procName == null || procName.length() == 0 || sectName == null || sectName.length() == 0 || keyName == null
			|| keyName.length() == 0 || value == null || value.length() == 0) {
	    throw RyeException.createRyeException((RyeConnectionUrl) null, RyeErrorCode.ER_INVALID_ARGUMENT, null);
	}
	this.procName = procName.toLowerCase();
	this.sectName = sectName.toLowerCase();
	this.keyName = keyName.toLowerCase();
	this.value = value;

	isShardMgmtParam = (this.procName.equals(PROC_BROKER) && this.sectName.equals(SECT_SHARD_MGMT));

	boolean isAutoGen = false;
	for (int i = 0; i < AUTO_GEN_PARAM.length; i++) {
	    if (this.keyName.equals(AUTO_GEN_PARAM[i])) {
		isAutoGen = true;
	    }
	}
	this.isAutoGenParam = isAutoGen;
    }

    String getProcName()
    {
	return procName;
    }

    String getSectName()
    {
	return sectName;
    }

    String getKeyName()
    {
	return keyName;
    }

    String getValue()
    {
	return value;
    }

    boolean isChangeable(byte changeMode)
    {
	if ((changeMode & CHANGE_CONF_DISABLE_SHARD_MGMT_PARAM) != 0 && this.isShardMgmtParam) {
	    return false;
	}
	if ((changeMode & CHANGE_CONF_DISABLE_AUTO_GEN_PARAM) != 0 && this.isAutoGenParam) {
	    return false;
	}
	return true;
    }

    protected static String[] splitParam(String param) throws RyeException
    {
	String[] keyValue = { null, null, null, null };

	int idx = param.indexOf('=');
	if (idx <= 0) {
	    throw ShardCommand.makeAdminRyeException(null, "invalid rye-auto.conf parameter: %s", param);
	}
	String paramName = param.substring(0, idx).toLowerCase();
	keyValue[3] = param.substring(idx + 1);

	String[] token = paramName.split("\\.");
	if (token.length > 3) {
	    throw ShardCommand.makeAdminRyeException(null, "invalid rye-auto.conf parameter: %s", param);
	}

	for (int i = 0; i < token.length; i++) {
	    keyValue[3 - token.length + i] = token[i].trim();
	}

	return keyValue;
    }

    public static RyeConfValue makeServerConfValue(String key, String value) throws RyeException
    {
	return new RyeServerConfValue(key, value);
    }

    public static RyeConfValue makeBrokerConfValue(String sectName, String key, String value) throws RyeException
    {
	return new RyeBrokerConfValue(sectName, key, value);
    }
}

class RyeServerConfValue extends RyeConfValue
{
    RyeServerConfValue(String key, String value) throws RyeException
    {
	super(PROC_SERVER, SECT_COMMON, key, value);
    }

    static RyeServerConfValue valueOf(String param) throws RyeException
    {
	String[] keyValue = splitParam(param);
	return new RyeServerConfValue(keyValue[2], keyValue[3]);
    }
}

class RyeBrokerConfValue extends RyeConfValue
{
    RyeBrokerConfValue(String sectName, String key, String value) throws RyeException
    {
	super(PROC_BROKER, sectName, key, value);
    }

    static RyeBrokerConfValue valueOf(String param) throws RyeException
    {
	String[] keyValue = splitParam(param);
	for (int i = 0; i < builtinBrokers.length; i++) {
	    if (builtinBrokers[i].equals(keyValue[1])) {
		return new RyeBrokerConfValue(keyValue[1], keyValue[2], keyValue[3]);
	    }
	}
	throw ShardCommand.makeAdminRyeException(null, "invalid rye-auto.conf parameter: %s", param);
    }
}

class RyeBrokerShardmgmtConfValue extends RyeBrokerConfValue
{
    RyeBrokerShardmgmtConfValue(String key, String value) throws RyeException
    {
	super(SECT_SHARD_MGMT, key, value);
    }
}
