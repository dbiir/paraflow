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

/**
 * @author jelly.guodong.jin@gmail.com
 */
public class FuncNotFoundException extends ParaFlowException
{
    private static final long serialVersionUID = 5600165943587655248L;
    private final String funcName;

    public FuncNotFoundException(String funcName)
    {
        this.funcName = funcName;
    }

    /**
     * get error message.
     *
     * @return error message
     */
    @Override
    public String getMessage()
    {
        return String.format("FiberFunc %s is not found", funcName);
    }

    @Override
    public StatusProto.ResponseStatus getResponseStatus()
    {
        return StatusProto.ResponseStatus
                .newBuilder()
                .setStatus(StatusProto.ResponseStatus.State.FIBER_FUNCTION_NOT_FOUND_WARN)
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
        return String.format("Please create the fiberFunc %s first.", funcName);
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
