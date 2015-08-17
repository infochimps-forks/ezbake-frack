/*   Copyright (C) 2013-2014 Computer Sciences Corporation
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
 * limitations under the License. */


package ezbake.frack.common.utils;

import ezbake.base.thrift.EzSecurityToken;
import ezbake.security.client.EzbakeSecurityClient;
import ezbake.services.provenance.thrift.AgeOffRule;
import ezbake.services.provenance.thrift.ProvenanceAgeOffRuleNotFoundException;
import ezbake.services.provenance.thrift.ProvenanceService;
import ezbake.services.provenance.thrift.ProvenanceServiceConstants;
import ezbake.thrift.ThriftClientPool;
import java.util.Properties;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author developer
 */
public class ProvenanceUtils
{
    private static Logger logger = LoggerFactory.getLogger(ProvenanceUtils.class);

    
    public static AgeOffRule GetRule(String name, Properties props)
    {
        //obtain a rule id for String name from the Provenance Service
        ThriftClientPool pool = null;
        EzbakeSecurityClient securityClient = null;
        ProvenanceService.Client client = null;
        
        AgeOffRule rule;        
                
        try
        {
            pool = new ThriftClientPool(props);
            client = pool.getClient(ProvenanceServiceConstants.SERVICE_NAME, ProvenanceService.Client.class);
        
            securityClient = new EzbakeSecurityClient(props);
            EzSecurityToken token = securityClient.fetchAppToken(pool.getSecurityId(ProvenanceServiceConstants.SERVICE_NAME));
            
            rule = client.getAgeOffRule(token, name);
            return rule;
        }
        catch (ProvenanceAgeOffRuleNotFoundException ex)
        {
            return null;       
        }
        catch (TException ex)
        {
            logger.error("Could not retrieve information from the Provenance service", ex);
            throw new RuntimeException(ex);
        }
        finally
        {
            if (pool != null)
            {
                pool.returnToPool(client);
                pool.close();
            }
        }        
        
    }
    
    public static long CreateRule(String name, long retentionDurationSeconds, int maximumExecutionPeriod, Properties props)
    {
        
        //obtain a rule id for String name from the Provenance Service
        ThriftClientPool pool = null;
        EzbakeSecurityClient securityClient = null;
        ProvenanceService.Client client = null;
        
        Long ruleId = null;
        
        try
        {
            pool = new ThriftClientPool(props);
            client = pool.getClient(ProvenanceServiceConstants.SERVICE_NAME, ProvenanceService.Client.class);
        
            securityClient = new EzbakeSecurityClient(props);
            EzSecurityToken token = securityClient.fetchAppToken(pool.getSecurityId(ProvenanceServiceConstants.SERVICE_NAME));
            
            ruleId = client.addAgeOffRule(token, name, retentionDurationSeconds, maximumExecutionPeriod);
            return ruleId;
        }
        catch (ProvenanceAgeOffRuleNotFoundException ex)
        {
            return ruleId;       
        }
        catch (TException ex)
        {
            logger.error("Could not retrieve information from the Provenance service", ex);
            throw new RuntimeException(ex);
        }
        finally
        {
            if (pool != null)
            {
                pool.returnToPool(client);
                pool.close();
            }
        } 
        
        
        
        
        
        
        
        
    }
    
}
