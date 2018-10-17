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
package cn.edu.ruc.iir.paraflow.metaserver.action;

import cn.edu.ruc.iir.paraflow.commons.exceptions.ActionParamNotValidException;
import cn.edu.ruc.iir.paraflow.commons.exceptions.ParaFlowException;
import cn.edu.ruc.iir.paraflow.metaserver.connection.Connection;
import cn.edu.ruc.iir.paraflow.metaserver.proto.MetaProto;

import java.util.Optional;

public class GetTblParamAction extends Action
{
    @Override
    public ActionResponse act(ActionResponse input, Connection connection) throws ParaFlowException
    {
        Optional<Object> paramOp = input.getParam();
        Optional<Object> userNameOp = input.getProperties("userName");
        Optional<Object> sfNameOp = input.getProperties("sfName");
        Optional<Object> funcNameOp = input.getProperties("fiberFuncName");
        if (paramOp.isPresent()
                && userNameOp.isPresent()
                && sfNameOp.isPresent()
                && funcNameOp.isPresent()) {
            MetaProto.TblParam tblParam = (MetaProto.TblParam) paramOp.get();
            String userName = userNameOp.get().toString();
            String sfName = sfNameOp.get().toString();
            String funcName = funcNameOp.get().toString();
            MetaProto.TblParam tblParamLast = MetaProto.TblParam.newBuilder()
                    .setDbName(tblParam.getDbName())
                    .setTblName(tblParam.getTblName())
                    .setUserName(userName)
                    .setCreateTime(tblParam.getCreateTime())
                    .setLastAccessTime(tblParam.getLastAccessTime())
                    .setLocationUrl(tblParam.getLocationUrl())
                    .setStorageFormatName(sfName)
                    .setFiberColId(tblParam.getFiberColId())
                    .setTimeColId(tblParam.getTimeColId())
                    .setFuncName(funcName)
                    .setTblId(Long.parseLong(input.getProperties("tblId").get().toString()))
                    .setDbId(Long.parseLong(input.getProperties("dbId").get().toString()))
                    .setIsEmpty(false)
                    .build();
            input.setParam(tblParamLast);
        }
        else {
            throw new ActionParamNotValidException();
        }
        return input;
    }
}
