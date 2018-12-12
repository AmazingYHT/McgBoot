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
import com.main.mcg.entity.flow.end.FlowEnd;
import com.main.mcg.entity.generate.ExecuteStruct;
import com.main.mcg.entity.generate.RunResult;
import com.main.mcg.plugin.build.McgProduct;
import com.main.mcg.plugin.execute.ProcessStrategy;
import com.main.mcg.util.DataConverter;


public class FlowEndStrategy implements ProcessStrategy
{

	@Override
	public void prepare(ArrayList<String> sequence, McgProduct mcgProduct, ExecuteStruct executeStruct) throws Exception {
		FlowEnd flowEnd = (FlowEnd)mcgProduct;
		executeStruct.getRunStatus().setExecuteId(flowEnd.getEndId());
	}

	@Override
	public RunResult run(McgProduct mcgProduct, ExecuteStruct executeStruct) throws Exception {
		FlowEnd flowEnd = (FlowEnd)mcgProduct;
		JSON parentParam = DataConverter.getParentRunResult(flowEnd.getEndId(), executeStruct);
		flowEnd = DataConverter.flowOjbectRepalceGlobal(DataConverter.addFlowStartRunResult(parentParam, executeStruct), flowEnd);
        RunResult result = new RunResult();
        result.setElementId(flowEnd.getEndId());
        result.setJsonVar(flowEnd.getEndProperty().getComment());		
        executeStruct.getRunStatus().setCode("success");
		return result;
	}
	
}