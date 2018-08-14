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
import cn.edu.ruc.iir.paraflow.commons.exceptions.FilterBlockIndexNotFoundException;
import cn.edu.ruc.iir.paraflow.commons.exceptions.ParaFlowException;
import cn.edu.ruc.iir.paraflow.metaserver.connection.Connection;
import cn.edu.ruc.iir.paraflow.metaserver.connection.ResultList;
import cn.edu.ruc.iir.paraflow.metaserver.proto.MetaProto;
import cn.edu.ruc.iir.paraflow.metaserver.utils.SQLTemplate;

import java.util.ArrayList;
import java.util.Optional;

/**
 * @author jelly.guodong.jin@gmail.com
 */
public class FilterBlockIndexByFiberAction extends Action
{
    @Override
    public ActionResponse act(ActionResponse input, Connection connection) throws ParaFlowException
    {
        Optional<Object> paramOp = input.getParam();
        Optional<Object> tblIdOp = input.getProperties("tblId");
        if (paramOp.isPresent() && tblIdOp.isPresent()) {
            long tblId = (long) tblIdOp.get();
            MetaProto.FilterBlockIndexByFiberParam filterBlockIndexByFiberParam
                    = (MetaProto.FilterBlockIndexByFiberParam) paramOp.get();
            long fiberValue = filterBlockIndexByFiberParam.getValue().getValue();
            ArrayList<String> result = new ArrayList<>();
            String sqlStatement;
            if (filterBlockIndexByFiberParam.getTimeBegin() == -1
                    && filterBlockIndexByFiberParam.getTimeEnd() == -1) {
                //query
                sqlStatement = SQLTemplate.filterBlockIndexByFiber(tblId, fiberValue);
            }
            else if (filterBlockIndexByFiberParam.getTimeBegin() == -1) {
                //query
                sqlStatement = SQLTemplate.filterBlockIndexByFiberEnd(
                        tblId,
                        fiberValue,
                        filterBlockIndexByFiberParam.getTimeEnd());
            }
            else if (filterBlockIndexByFiberParam.getTimeEnd() == -1) {
                //query
                sqlStatement = SQLTemplate.filterBlockIndexByFiberBegin(
                        tblId,
                        fiberValue,
                        filterBlockIndexByFiberParam.getTimeBegin());
            }
            else {
                //query
                sqlStatement = SQLTemplate.filterBlockIndexByFiberBeginEnd(
                        tblId,
                        fiberValue,
                        filterBlockIndexByFiberParam.getTimeBegin(),
                        filterBlockIndexByFiberParam.getTimeEnd());
            }
            ResultList resultList = connection.executeQuery(sqlStatement);
            MetaProto.StringListType stringList;
            if (!resultList.isEmpty()) {
                int size = resultList.size();
                for (int i = 0; i < size; i++) {
                    result.add(resultList.get(i).get(0));
                }
                //result
                stringList = MetaProto.StringListType.newBuilder()
                        .addAllStr(result)
                        .setIsEmpty(false)
                        .build();
                input.setParam(stringList);
            }
            else {
                throw new FilterBlockIndexNotFoundException();
            }
        }
        else {
            throw new ActionParamNotValidException();
        }
        return input;
    }
}
