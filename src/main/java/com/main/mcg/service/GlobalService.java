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

package com.main.mcg.service;

import java.io.IOException;

import com.main.mcg.entity.global.McgGlobal;

/**
 * 
 * @ClassName:   GlobalService   
 * @Description: TODO(全局变量服务) 
 * @author:      缪聪(mcg-helper@qq.com)
 * @date:        2018年3月9日 下午5:44:59  
 *
 */
public interface GlobalService {
	
    /*
     * 修改全局变量时存储数据
     */
    boolean updateGlobal(McgGlobal mcgGlobal) throws IOException;

    /*
     * 保存空流程，检查该流程是否存在流程文件，如果有则删除掉
     */
    boolean saveFlowEmpty(String flowId);
    
}