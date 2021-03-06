/*
 * @Copyright (c) 2018 缪聪(mcg-helper@qq.com)
 * 
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

package com.main.mcg.plugin.execute.strategy;

import java.util.ArrayList;
import com.alibaba.fastjson.JSON;
import com.main.mcg.common.sysenum.EletypeEnum;
import com.main.mcg.common.sysenum.LogTypeEnum;
import com.main.mcg.common.sysenum.MessageTypeEnum;
import com.main.mcg.entity.flow.sqlexecute.FlowSqlExecute;
import com.main.mcg.entity.generate.ExecuteStruct;
import com.main.mcg.entity.generate.RunResult;
import com.main.mcg.entity.message.FlowBody;
import com.main.mcg.entity.message.Message;
import com.main.mcg.plugin.build.McgProduct;
import com.main.mcg.plugin.dbconn.FlowDataAdapterImpl;
import com.main.mcg.plugin.dbconn.McgBizAdapter;
import com.main.mcg.plugin.execute.ProcessStrategy;
import com.main.mcg.plugin.generate.FlowTask;
import com.main.mcg.plugin.websocket.MessagePlugin;
import com.main.mcg.service.FlowService;
import com.main.mcg.service.impl.FlowServiceImpl;
import com.main.mcg.util.DataConverter;

public class FlowSqlExecuteStrategy implements ProcessStrategy
{

	@Override
	public void prepare(ArrayList<String> sequence, McgProduct mcgProduct, ExecuteStruct executeStruct) throws Exception {
	    FlowSqlExecute flowSqlExecute = (FlowSqlExecute)mcgProduct;
		executeStruct.getRunStatus().setExecuteId(flowSqlExecute.getId());
	}

	@Override
	public RunResult run(McgProduct mcgProduct, ExecuteStruct executeStruct) throws Exception {
		
	    FlowSqlExecute flowSqlExecute = (FlowSqlExecute)mcgProduct;
		JSON parentParam = DataConverter.getParentRunResult(flowSqlExecute.getId(), executeStruct);
		
        Message message = MessagePlugin.getMessage();
        message.getHeader().setMesType(MessageTypeEnum.FLOW);
        FlowBody flowBody = new FlowBody();
        flowBody.setEleType(EletypeEnum.SQLEXECUTE.getValue());
        flowBody.setEleTypeDesc(EletypeEnum.SQLEXECUTE.getName() + "--》" + flowSqlExecute.getSqlExecuteProperty().getName());
        flowBody.setEleId(flowSqlExecute.getId());
        flowBody.setComment("参数");
        
        if(parentParam == null) {
        	flowBody.setContent("{}");
        } else {
        	flowBody.setContent(JSON.toJSONString(parentParam, true));
        }
        flowBody.setLogType(LogTypeEnum.INFO.getValue());
        flowBody.setLogTypeDesc(LogTypeEnum.INFO.getName());
        message.setBody(flowBody);
        FlowTask flowTask = FlowTask.executeLocal.get();
        MessagePlugin.push(flowTask.getHttpSessionId(), message);		
		
        flowSqlExecute = DataConverter.flowOjbectRepalceGlobal(DataConverter.addFlowStartRunResult(parentParam, executeStruct), flowSqlExecute);		
		RunResult runResult = new RunResult();
		runResult.setElementId(flowSqlExecute.getId());
		
        FlowService flowService = new FlowServiceImpl();
        McgBizAdapter mcgBizAdapter = new FlowDataAdapterImpl(flowService.getMcgDataSourceById(flowSqlExecute.getSqlExecuteCore().getDataSourceId()));
        
        Integer rows = mcgBizAdapter.executeUpdate(flowSqlExecute.getSqlExecuteCore().getSource(), null);
     
        if(rows == null) {
            runResult.setSourceCode("成功执行");
        } else {
            runResult.setSourceCode("成功执行，影响行数【" + rows  + "】行");
        }
		executeStruct.getRunStatus().setCode("success");
		return runResult;
	}
	
}