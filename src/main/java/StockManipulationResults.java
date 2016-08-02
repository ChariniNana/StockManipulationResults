
 /*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

 import java.io.File;
 import java.io.FileWriter;
 import java.io.IOException;
 import java.util.List;

 import org.apache.commons.io.input.ReversedLinesFileReader;
 import org.apache.log4j.Logger;
 import org.wso2.carbon.databridge.commons.Credentials;
 import org.wso2.carbon.databridge.commons.Event;
 import org.wso2.carbon.databridge.commons.StreamDefinition;
 import org.wso2.carbon.databridge.core.AgentCallback;
 import org.wso2.carbon.databridge.core.DataBridge;
 import org.wso2.carbon.databridge.core.Utils.AgentSession;
 import org.wso2.carbon.databridge.core.definitionstore.AbstractStreamDefinitionStore;
 import org.wso2.carbon.databridge.core.definitionstore.InMemoryStreamDefinitionStore;
 import org.wso2.carbon.databridge.core.exception.DataBridgeException;
 import org.wso2.carbon.databridge.core.exception.StreamDefinitionStoreException;
 import org.wso2.carbon.databridge.core.internal.authentication.AuthenticationHandler;
 import org.wso2.carbon.databridge.receiver.thrift.ThriftDataReceiver;
 import org.wso2.carbon.user.api.UserStoreException;

 public class StockManipulationResults {
     private static Logger log = Logger.getLogger(StockManipulationResults.class);
     private static ThriftDataReceiver thriftDataReceiver;
     private static AbstractStreamDefinitionStore streamDefinitionStore = new InMemoryStreamDefinitionStore();
     private static final StockManipulationResults testServer = new StockManipulationResults();
     private static String lastDate;
     private static File common = new File("../Results");
     private static File commonPT = new File("../PerfectTraderResults");
     private static File resultsFolderNames;
     private static File resultsFileNamesPT;
     private static String resultsHomePath;
     private static String resultsHomePathPT;
     private static File dir;

     public static void main(String[] args) throws DataBridgeException, StreamDefinitionStoreException, IOException {
         resultsHomePath = common.getCanonicalPath();
         resultsFolderNames = new File(resultsHomePath+File.separator+"ExecutionDetails.csv");
         resultsHomePathPT = commonPT.getCanonicalPath();
         resultsFileNamesPT = new File(resultsHomePathPT+File.separator+"ExecutionDetails.csv");
         startReceivingData("localhost", 7712);
         synchronized (testServer) {
             try {
                 testServer.wait();
             } catch (InterruptedException ignored) {


             }
         }
     }


     private static void startReceivingData(String host, int receiverPort) throws DataBridgeException, StreamDefinitionStoreException {
         WSO2EventServerUtil.setKeyStoreParams();

         DataBridge databridge = new DataBridge(new AuthenticationHandler() {

             public boolean authenticate(String userName,
                                         String password) {
                 return true;// allays authenticate to true


             }

             public String getTenantDomain(String userName) {
                 return "admin";
             }


             public int getTenantId(String s) throws UserStoreException {
                 return -1234;
             }

             @Override
             public void initContext(AgentSession agentSession) {

             }

             @Override
             public void destroyContext(AgentSession agentSession) {

             }

         }, streamDefinitionStore, WSO2EventServerUtil.getDataBridgeConfigPath());


         for (StreamDefinition streamDefinition : WSO2EventServerUtil.loadStreamDefinitions("FR")) {
             streamDefinitionStore.saveStreamDefinitionToStore(streamDefinition, -1234);
             log.info("StreamDefinition of '" + streamDefinition.getStreamId() + "' added to store");
         }

         for (StreamDefinition streamDefinition : WSO2EventServerUtil.loadStreamDefinitions("ID")) {
             streamDefinitionStore.saveStreamDefinitionToStore(streamDefinition, -1234);
             log.info("StreamDefinition of '" + streamDefinition.getStreamId() + "' added to store");

         }

         for (StreamDefinition streamDefinition : WSO2EventServerUtil.loadStreamDefinitions("PD")) {
             streamDefinitionStore.saveStreamDefinitionToStore(streamDefinition, -1234);
             log.info("StreamDefinition of '" + streamDefinition.getStreamId() + "' added to store");

         }

         for (StreamDefinition streamDefinition : WSO2EventServerUtil.loadStreamDefinitions("PerfectTrader")) {
             streamDefinitionStore.saveStreamDefinitionToStore(streamDefinition, -1234);
             log.info("StreamDefinition of '" + streamDefinition.getStreamId() + "' added to store");

         }

         databridge.subscribe(new AgentCallback() {

             public void definedStream(StreamDefinition streamDefinition,
                                       int tenantID) {
                 log.info("StreamDefinition " + streamDefinition);
             }


             public void removeStream(StreamDefinition streamDefinition, int tenantID) {
                 //To change body of implemented methods use File | Settings | File Templates.
             }


             public void receive(List<Event> eventList, Credentials credentials) {
                 if (dir==null) {
                     boolean x = false;
                     try {
                         ReversedLinesFileReader readLast = new ReversedLinesFileReader(resultsFolderNames);
                         String newFolderName = readLast.readLine().split(",")[1];
                         dir = new File(resultsHomePath+ File.separator + newFolderName);
                         if (!dir.exists()) {
                             x = dir.mkdir();
                             lastDate = newFolderName;
                         } else {
                             log.info("Directory " + newFolderName + " already exists. User may have already run a test " +
                                     "considering relevant directory date as last date. Please remove that folder and resume.");
                             StockManipulationResults.stop();

                         }
                     } catch (IOException e) {
                         log.info(e.getMessage()+" directory create: "+x);
                     }
                 }
                 //File theDir = null;
                 String eventName;
                 String path;
                 File f;
                 Object [] payLoad;

                 for (Event event:eventList) {
                     eventName=event.getStreamId();
                     switch (eventName) {
                         case "AlertBrokerFR:1.0.0":
                             path = resultsHomePath+File.separator+lastDate+File.separator+"BrokerFR.csv";
                             f = new File(path);
                             if (lastDate!=null && !f.exists()){
                                 boolean x = false;
                                 try {
                                     x =f.createNewFile();
                                     FileWriter fw = new FileWriter(f.getAbsolutePath(),true);
                                     fw.write("SECURITY, INITIAL_PURCHASE_TIME, INITIAL_TRAN_REF_NO, BUY_QTY, BUY_PRICE, FR_TRAN_REF_NO, SELL_QTY, SELL_PRICE, FR_TIME, CLIENT, BROKER, SELL_VALUE, FR_UNIX_TIME\n");
                                     fw.close();
                                 } catch (IOException e) {
                                     log.info(e.getMessage()+ " file created: "+x);
                                 }
                             }
                             payLoad = event.getPayloadData();
                             try {
                                 FileWriter fw = new FileWriter(f.getAbsolutePath(),true);
                                 for (Object x:payLoad) {
                                     fw.write(x.toString()+",");
                                 }
                                 fw.write("\n");
                                 fw.close();
                             } catch (IOException e) {
                                 log.info(e.getMessage());
                             }
                             break;

                         case "FrontRunners:1.0.0":
                             path = resultsHomePath+File.separator+lastDate+File.separator+"FrontRunning.csv";
                             f = new File(path);
                             if (lastDate!=null && !f.exists()){
                                 boolean x = false;
                                 try {
                                     x = f.createNewFile();
                                     FileWriter fw = new FileWriter(f.getAbsolutePath(),true);
                                     fw.write("DATE_OF_LARGE_SALE, SECURITY, SUSPICIOUS_CLIENT, AVG_PURCHASE_PRICE, AVG_SALE_PRICE, TOT_BUY_QTY, TOT_SELL_QTY, BUY_VALUE, SELL_VALUE, NO_OF_DAYS\n");
                                     fw.close();
                                 } catch (IOException e) {
                                     log.info(e.getMessage()+ " file created: "+x);
                                 }
                             }
                             payLoad = event.getPayloadData();
                             try {
                                 FileWriter fw = new FileWriter(f.getAbsolutePath(),true);
                                 for (Object x:payLoad) {
                                     fw.write(x.toString()+",");
                                 }
                                 fw.write("\n");
                                 fw.close();
                             } catch (IOException e) {
                                 log.info(e.getMessage());
                             }
                             break;

                         case "SummaryOfBrokerFR:1.0.0":
                             path = resultsHomePath+File.separator+lastDate+File.separator+"SummaryOfBrokerFR.csv";
                             f = new File(path);
                             if (lastDate!=null && !f.exists()){
                                 boolean x = false;
                                 try {
                                     x= f.createNewFile();
                                     FileWriter fw = new FileWriter(f.getAbsolutePath(),true);
                                     fw.write("SECURITY, TOTAL_BUY_QTY, AVG_BUY_PRICE, FR_TRAN_REF_NO, SELL_QTY, SELL_PRICE, FR_TIME, CLIENT, BROKER, SELL_VALUE\n");
                                     fw.close();
                                 } catch (IOException e) {
                                     log.info(e.getMessage()+ " file created: "+x);
                                 }
                             }
                             payLoad = event.getPayloadData();
                             try {
                                 FileWriter fw = new FileWriter(f.getAbsolutePath(),true);
                                 for (Object x:payLoad) {
                                     fw.write(x.toString()+",");
                                 }
                                 fw.write("\n");
                                 fw.close();
                             } catch (IOException e) {
                                 log.info(e.getMessage());
                             }
                             break;

                         case "InsiderDealingWithLargePurchases:1.0.0":
                             path = resultsHomePath+File.separator+lastDate+File.separator+"InsiderDealingWithLargePurchases.csv";
                             f = new File(path);
                             if (lastDate!=null && !f.exists()){
                                 boolean x = false;
                                 try {
                                     x = f.createNewFile();
                                     FileWriter fw = new FileWriter(f.getAbsolutePath(),true);
                                     fw.write("TRADE_DATE, SECURITY, BUY_CLIENT, TOTAL_QUANTITY_PER_BUYER, TOTAL_QUANTITY_PER_SECURITY, ANNOUNCEMENT_ID, SHORT_DESC, ANNOUNCEMENT_TIME, ANNOUNCEMENT_TYPE\n");
                                     fw.close();
                                 } catch (IOException e) {
                                     log.info(e.getMessage()+ " file created: "+x);
                                 }
                             }
                             payLoad = event.getPayloadData();
                             try {
                                 FileWriter fw = new FileWriter(f.getAbsolutePath(),true);
                                 for (Object x:payLoad) {
                                     fw.write(x.toString()+",");
                                 }
                                 fw.write("\n");
                                 fw.close();
                             } catch (IOException e) {
                                 log.info(e.getMessage());
                             }
                             break;

                         case "InsiderDealingWithLargeSales:1.0.0":
                             path = resultsHomePath+File.separator+lastDate+File.separator+"InsiderDealingWithLargeSales.csv";
                             f = new File(path);
                             if (lastDate!=null && !f.exists()){
                                 boolean x = false;
                                 try {
                                     x = f.createNewFile();
                                     FileWriter fw = new FileWriter(f.getAbsolutePath(),true);
                                     fw.write("TRADE_DATE, SECURITY, SELL_CLIENT, TOTAL_QUANTITY_PER_SELLER, TOTAL_QUANTITY_PER_SECURITY, ANNOUNCEMENT_ID, SHORT_DESC, ANNOUNCEMENT_TIME, ANNOUNCEMENT_TYPE\n");
                                     fw.close();
                                 } catch (IOException e) {
                                     log.info(e.getMessage()+ " file created: "+x);
                                 }
                             }
                             payLoad = event.getPayloadData();
                             try {
                                 FileWriter fw = new FileWriter(f.getAbsolutePath(),true);
                                 for (Object x:payLoad) {
                                     fw.write(x.toString()+",");
                                 }
                                 fw.write("\n");
                                 fw.close();
                             } catch (IOException e) {
                                 log.info(e.getMessage());
                             }
                             break;

                         case "PurchaseFrequency:1.0.0":
                             path = resultsHomePath+File.separator+lastDate+File.separator+"PurchaseFrequency.csv";
                             f = new File(path);
                             if (lastDate!=null && !f.exists()){
                                 boolean x = false;

                                 try {
                                     x= f.createNewFile();
                                     FileWriter fw = new FileWriter(f.getAbsolutePath(),true);
                                     fw.write("SECURITY, BUY_CLIENT, TOTAL_BUY_QTY_PER_DURATION, TOTAL_BUY_QTY_PER_PAST_MONTHS, BUY_FREQUENCY, AVG_PURCHASE_PRICE_PER_DURATION, AVG_PURCHASE_PRICE_PER_PAST_MONTHS, ANNOUNCEMENT_ID, SHORT_DESC, ANNOUNCEMENT_TIME, ANNOUNCEMENT_TYPE\n");
                                     fw.close();
                                 } catch (IOException e) {
                                     log.info(e.getMessage()+ " file created: "+x);
                                 }
                             }
                             payLoad = event.getPayloadData();
                             try {
                                 FileWriter fw = new FileWriter(f.getAbsolutePath(),true);
                                 for (Object x:payLoad) {
                                     fw.write(x.toString()+",");
                                 }
                                 fw.write("\n");
                                 fw.close();
                             } catch (IOException e) {
                                 log.info(e.getMessage());
                             }
                             break;

                         case "SaleFrequency:1.0.0":
                             path = resultsHomePath+File.separator+lastDate+File.separator+"SaleFrequency.csv";
                             f = new File(path);
                             if (lastDate!=null && !f.exists()){
                                 boolean x = false;

                                 try {
                                     x = f.createNewFile();
                                     FileWriter fw = new FileWriter(f.getAbsolutePath(),true);
                                     fw.write("SECURITY, SELL_CLIENT, TOTAL_SELL_QTY_PER_DURATION, TOTAL_SELL_QTY_PER_PAST_MONTHS, SELL_FREQUENCY, AVG_SALE_PRICE_PER_DURATION, AVG_SALE_PRICE_PER_PAST_MONTHS, ANNOUNCEMENT_ID, SHORT_DESC, ANNOUNCEMENT_TIME, ANNOUNCEMENT_TYPE\n");
                                     fw.close();
                                 } catch (IOException e) {
                                     log.info(e.getMessage()+ " file created: "+x);
                                 }
                             }
                             payLoad = event.getPayloadData();
                             try {
                                 FileWriter fw = new FileWriter(f.getAbsolutePath(),true);
                                 for (Object x:payLoad) {
                                     fw.write(x.toString()+",");
                                 }
                                 fw.write("\n");
                                 fw.close();
                             } catch (IOException e) {
                                 log.info(e.getMessage());
                             }
                             break;

                         case "PriceGrowthContribution:1.0.0":
                             path = resultsHomePath+File.separator+lastDate+File.separator+"PriceGrowthContribution.csv";
                             f = new File(path);
                             if (lastDate!=null && !f.exists()){
                                 boolean x = false;

                                 try {
                                     x = f.createNewFile();
                                     FileWriter fw = new FileWriter(f.getAbsolutePath(),true);
                                     fw.write("TRAN_REF_NO, SECURITY, BUY_CLIENT, PRICE_BEFORE_INCREMENT, PRICE_AFTER_INCREMENT, PRICE_GROWTH_CONTRIBUTION, UNIX_TIME, PUMP_DATE\n");
                                     fw.close();
                                 } catch (IOException e) {
                                     log.info(e.getMessage()+ " file created: "+x);
                                 }
                             }
                             payLoad = event.getPayloadData();
                             try {
                                 FileWriter fw = new FileWriter(f.getAbsolutePath(),true);
                                 for (Object x:payLoad) {
                                     fw.write(x.toString()+",");
                                 }
                                 fw.write("\n");
                                 fw.close();
                             } catch (IOException e) {
                                 log.info(e.getMessage());
                             }
                             break;

                         case "PumpDumpDuringLastDay:1.0.0":
                             path = resultsHomePath+File.separator+lastDate+File.separator+"PumpDumpOn_"+lastDate+".csv";
                             f = new File(path);
                             if (lastDate!=null && !f.exists()){
                                 boolean x = false;

                                 try {
                                     x=f.createNewFile();
                                     FileWriter fw = new FileWriter(f.getAbsolutePath(),true);
                                     fw.write("PUMP_DUMP_DATE, SECURITY, BUY_CLIENT, INCREMENTS_PER_SECURITY, INCREMENTS_PER_BUYER, AVG_PRICE_GROWTH_CONTRIBUTION, SELL_CLIENT, SELL_QTY, SELL_VALUE\n");
                                     fw.close();
                                 } catch (IOException e) {
                                     log.info(e.getMessage()+ " file created: "+x);
                                 }
                             }
                             payLoad = event.getPayloadData();
                             try {
                                 FileWriter fw = new FileWriter(f.getAbsolutePath(),true);
                                 for (Object x:payLoad) {
                                     fw.write(x.toString()+",");
                                 }
                                 fw.write("\n");
                                 fw.close();
                             } catch (IOException e) {
                                 log.info(e.getMessage());
                             }
                             break;

                         case "PumpDumpDuringPeriod:1.0.0":
                             path = resultsHomePath+File.separator+lastDate+File.separator+"PumpDumpDuringOneMonth.csv";
                             f = new File(path);
                             if (lastDate!=null && !f.exists()){
                                 boolean x = false;

                                 try {
                                     x = f.createNewFile();
                                     FileWriter fw = new FileWriter(f.getAbsolutePath(),true);
                                     fw.write("SALE_DATE, SECURITY, BUY_CLIENT, INCREMENTS_PER_SECURITY, INCREMENTS_PER_BUYER, AVG_PRICE_GROWTH_CONTRIBUTION, SELL_CLIENT, SELL_QTY, SELL_VALUE\n");
                                     fw.close();
                                 } catch (IOException e) {
                                     log.info(e.getMessage()+ " file created: "+x);
                                 }
                             }
                             payLoad = event.getPayloadData();
                             try {
                                 FileWriter fw = new FileWriter(f.getAbsolutePath(),true);
                                 for (Object x:payLoad) {
                                     fw.write(x.toString()+",");
                                 }
                                 fw.write("\n");
                                 fw.close();
                             } catch (IOException e) {
                                 log.info(e.getMessage());
                             }
                             break;

                         case "PumpDuringLastDay:1.0.0":
                             path = resultsHomePath+File.separator+lastDate+File.separator+"PumpOn_"+lastDate+".csv";
                             f = new File(path);
                             if (lastDate!=null && !f.exists()){
                                 boolean x = false;

                                 try {
                                     x = f.createNewFile();
                                     FileWriter fw = new FileWriter(f.getAbsolutePath(),true);
                                     fw.write("PUMP_DATE, SECURITY, BUY_CLIENT, INCREMENTS_PER_SECURITY, INCREMENTS_PER_BUYER, AVG_PRICE_GROWTH_CONTRIBUTION, FIRST_PUMP_TIME\n");
                                     fw.close();
                                 } catch (IOException e) {
                                     log.info(e.getMessage()+ " file created: "+x);
                                 }
                             }
                             payLoad = event.getPayloadData();
                             try {
                                 FileWriter fw = new FileWriter(f.getAbsolutePath(),true);
                                 for (Object x:payLoad) {
                                     fw.write(x.toString()+",");
                                 }
                                 fw.write("\n");
                                 fw.close();
                             } catch (IOException e) {
                                 log.info(e.getMessage());
                             }
                             break;

                         case "PumpDuringPeriod:1.0.0":
                             path = resultsHomePath+File.separator+lastDate+File.separator+"PumpDuringOneMonth.csv";
                             f = new File(path);
                             if (lastDate!=null && !f.exists()){
                                 boolean x = false;

                                 try {
                                     x = f.createNewFile();
                                     FileWriter fw = new FileWriter(f.getAbsolutePath(),true);
                                     fw.write("SECURITY, BUY_CLIENT, INCREMENTS_PER_SECURITY, INCREMENTS_PER_BUYER, AVG_PRICE_GROWTH_CONTRIBUTION, FIRST_PUMP_TIME, LAST_DATE_CONSIDERED\n");
                                     fw.close();
                                 } catch (IOException e) {
                                     log.info(e.getMessage()+ " file created: "+x);
                                 }
                             }
                             payLoad = event.getPayloadData();
                             try {
                                 FileWriter fw = new FileWriter(f.getAbsolutePath(),true);
                                 for (Object x:payLoad) {
                                     fw.write(x.toString()+",");
                                 }
                                 fw.write("\n");
                                 fw.close();
                             } catch (IOException e) {
                                 log.info(e.getMessage());
                             }
                             break;

                         case "SuspiciousTradersStream:1.0.0":
                             ReversedLinesFileReader readLast;
                             boolean y = false;
                             try {
                                 readLast = new ReversedLinesFileReader(resultsFileNamesPT);
                                 String newFileName = readLast.readLine().split(",")[1];
                                 path=resultsHomePathPT+File.separator+newFileName;
                                 f = new File(path);
                                 if (!f.exists() && newFileName!=null) {
                                     y = f.createNewFile();
                                     FileWriter fw = new FileWriter(f.getAbsolutePath(), true);
                                     fw.write("CLIENT, SECURITY, DATE, TOTAL_BUY_QTY, TOTAL_SELL_QTY, TOTAL_BUY_VALUE, TOTAL_SELL_VALUE, START_DATE, PORTFOLIO, ANNUAL_PROFIT_RATIO, PT_ANNUAL_PROFIT_RATIO, PROFIT_RATIO_WITH_PT\n");
                                     fw.close();
                                 }
                                 payLoad = event.getPayloadData();
                                 FileWriter fw = new FileWriter(f.getAbsolutePath(),true);
                                 for (Object x:payLoad) {
                                     fw.write(x.toString()+",");
                                 }
                                 fw.write("\n");
                                 fw.close();
                             } catch (IOException e) {
                                 log.info(e.getMessage()+ " file created: "+y);
                             }
                             break;
                     }
                     //event.getStreamId();
                 }
                 //stop();
             }


         });


         thriftDataReceiver = new ThriftDataReceiver(receiverPort, databridge);
         thriftDataReceiver.start(host);

         log.info("Test Server Started");
     }

     private static void stop() {
         if (thriftDataReceiver != null) {
             thriftDataReceiver.stop();
         }

         log.info("Test Server Stopped");
     }
 }