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
import cn.edu.ruc.iir.paraflow.commons.exceptions.FuncNotFoundException;
import cn.edu.ruc.iir.paraflow.commons.exceptions.ParaFlowException;
import cn.edu.ruc.iir.paraflow.metaserver.connection.Connection;
import cn.edu.ruc.iir.paraflow.metaserver.connection.ResultList;
import cn.edu.ruc.iir.paraflow.metaserver.proto.MetaProto;
import cn.edu.ruc.iir.paraflow.metaserver.utils.SQLTemplate;
import com.google.protobuf.ByteString;

import java.util.Optional;

/**
 * @author jelly.guodong.jin@gmail.com
 */
public class GetFuncAction extends Action
{
    @Override
    public ActionResponse act(ActionResponse input, Connection connection) throws ParaFlowException
    {
        Optional<Object> fiberFuncNameOp = input.getProperties("fiberFuncName");
        Optional<Object> paramOp = input.getParam();
        if (fiberFuncNameOp.isPresent() && paramOp.isPresent()) {
            String fiberFuncName = fiberFuncNameOp.get().toString();
            String sqlStatement = SQLTemplate.getFunc(fiberFuncName);
            ResultList resultList = connection.executeQuery(sqlStatement);
            if (!resultList.isEmpty()) {
                byte[] bytes = resultList.get(0).get(0).getBytes();
                ByteString byteString = ByteString.copyFrom(bytes);
                MetaProto.FuncParam fiberFuncParam
                        = MetaProto.FuncParam.newBuilder()
                        .setFuncName(fiberFuncName)
                        .setFuncContent(byteString)
                        .setIsEmpty(false)
                        .build();
                input.setParam(fiberFuncParam);
            }
            else {
                throw new FuncNotFoundException();
            }
        }
        else {
            throw new ActionParamNotValidException();
        }
        return input;
    }
}
