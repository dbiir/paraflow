/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cn.edu.ruc.iir.paraflow.commons.exceptions;

import cn.edu.ruc.iir.paraflow.commons.proto.StatusProto;

public class DatabaseNotFoundException extends ParaFlowException
{
    private static final long serialVersionUID = 5600165987123353248L;

    private final String dbName;

    public DatabaseNotFoundException(String dbName)
    {
        this.dbName = dbName;
    }

    /**
     * get error message.
     *
     * @return error message
     */
    @Override
    public String getMessage()
    {
        return String.format("Database %s is not found", this.dbName);
    }

    @Override
    public StatusProto.ResponseStatus getResponseStatus()
    {
        return StatusProto.ResponseStatus
                .newBuilder()
                .setStatus(StatusProto.ResponseStatus.State.DATABASE_NOT_FOUND_WARN)
                .build();
    }

    /**
     * get system hint message for user on how to deal with this exception
     *
     * @return hint message
     */
    @Override
    public String getHint()
    {
        return String.format("Please create the database %s first.", this.dbName);
    }

    /**
     * get exception level
     *
     * @return exception level
     */
    @Override
    public ParaFlowExceptionLevel getLevel()
    {
        return ParaFlowExceptionLevel.WARN;
    }
}
