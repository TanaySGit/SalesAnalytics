from datetime import date, datetime, timedelta
import snowflake.connector
import FIFO.config  as config
import logging
from timeit import default_timer as timer
from FIFO import snowflakeconnect

class funnelinout:
 def __init__(self,PERIOD_NAME_VAR_val,CURRENT_WD_INFO_VAR_val,PREV_WD_INFO_VAR_val,batch):

  self.batch=batch
  self.PERIOD_NAME_VAR_val = PERIOD_NAME_VAR_val
  self.CURRENT_WD_INFO_VAR_val = CURRENT_WD_INFO_VAR_val
  self.PREV_WD_INFO_VAR_val = PREV_WD_INFO_VAR_val



 def processdata(self):

  timerstep1 = timer()
  logging.basicConfig(filename="FIFOout.log", format='%(asctime)s %(message)s', level=logging.INFO)
  logging.info("Funnel load tables data processing started")
  print("Funnel load tables data processing started")
  WD=('WD01','WD02','WD05','WD10','WD15','WD14')
  today = datetime.now()
  year=today.year
  '''Get Current Month in MON format'''
  mon_column = today.strftime("%b").upper()
  periodname=mon_column+'-'+str(year)
  snowcon=snowflakeconnect.snowflakeconnect()
  snowcursor=snowcon.createcur()
  try:


   '''Use Current Month in select query to get WORKDAY'''

   checkwdstmnt= "select NVL(WORKDAY,'WE') FROM WD_CALENDER  WHERE %s = CURRENT_DATE; " %(mon_column)
   snowexec=snowcursor.execute(checkwdstmnt)
   getwd=snowexec.fetchone()
   '''Get max number from the load sequence'''
   print("Today the workday is " +getwd[0])
   logging.info("Today the workday is " +getwd[0])
  except Exception as e:
    print('Connection is ok but Snowflake running query issue')
    logging.error("Connection is ok but Snowflake running query issue.")
    logging.error(e)
    return 400

  if getwd[0]in WD:
    COMPARISON_NAME_VAR_val = self.PERIOD_NAME_VAR_val[4:] + '-' + self.PERIOD_NAME_VAR_val[0:3]+'-'+self.CURRENT_WD_INFO_VAR_val+'-'+self.PREV_WD_INFO_VAR_val
    funnelexistsexestr=config.CHECK_FUNNEL_COMP_EXISTS %(COMPARISON_NAME_VAR_val)

    funnelexistsexe = snowcursor.execute(funnelexistsexestr)
    funnel_exists_rec = funnelexistsexe.fetchone()

    if funnel_exists_rec is not None :
     print("The Funnel Comparison Already Exists. So not processing again.")
     logging.info("The Funnel Comparison Already Exists. So not processing again.")
     return 200
    else:
     try:
      timerstep2 = timer()
      runtableinsert="INSERT INTO RUN_TABLE(BATCH_NUM,PROCESS,PROCESS_DESC,START_TIME,END_TIME,TIME_TAKEN_IN_MINT,STATUS_CD,PROCESS_DATE) VALUES( %s, 'Funnel_Comparison_Load',"+"'"+str(COMPARISON_NAME_VAR_val)+"'"+",CURRENT_TIMESTAMP,null,null,'STARTED',CURRENT_TIMESTAMP)"
      runtableinsert = runtableinsert%(str(self.batch))
      print(runtableinsert)
      snowcursor.execute(runtableinsert)
      CURRENT_SNAPSHOT_DATE_VAR_val_exe = snowcursor.execute("select to_char(snapshot_dt,'mm/dd/yyyy') from opportunity_snapshot where period_name='{}' and workday_info='{}' limit 1;".format(self.PERIOD_NAME_VAR_val,self.CURRENT_WD_INFO_VAR_val))
      CURRENT_SNAPSHOT_DATE_VAR_val_rsult = CURRENT_SNAPSHOT_DATE_VAR_val_exe.fetchone()
      CURRENT_SNAPSHOT_DATE_VAR_val=CURRENT_SNAPSHOT_DATE_VAR_val_rsult[0]

      PREV_SNAPSHOT_DATE_VAR_val_exe = snowcursor.execute("select to_char(snapshot_dt,'mm/dd/yyyy') from opportunity_snapshot where period_name='{}' and workday_info='{}' limit 1;".format(self.PERIOD_NAME_VAR_val,self.PREV_WD_INFO_VAR_val))
      PREV_SNAPSHOT_DATE_VAR_val_rsult = PREV_SNAPSHOT_DATE_VAR_val_exe.fetchone()
      PREV_SNAPSHOT_DATE_VAR_val=PREV_SNAPSHOT_DATE_VAR_val_rsult[0]


      FUNNEL_IN_OUT_INSERT_STR=config.FUNNEL_IN_OUT_INSERT.replace('PERIOD_NAME_VAR',"'"+self.PERIOD_NAME_VAR_val+"'").replace('CURRENT_WD_INFO_VAR',"'"+self.CURRENT_WD_INFO_VAR_val+"'").replace('CURRENT_SNAPSHOT_DATE_VAR',CURRENT_SNAPSHOT_DATE_VAR_val)
      FUNNEL_IN_OUT_INSERT_STR=FUNNEL_IN_OUT_INSERT_STR.replace('PREV_WD_INFO_VAR',"'"+self.PREV_WD_INFO_VAR_val+"'").replace('PREV_SNAPSHOT_DATE_VAR',PREV_SNAPSHOT_DATE_VAR_val).replace('COMPARISON_NAME_VAR',"'"+COMPARISON_NAME_VAR_val+"'")
      print("started inserting the funnel data")
      logging.info("started inserting the funnel data")
      insertfunnel=snowcursor.execute(FUNNEL_IN_OUT_INSERT_STR)
      rowcount=insertfunnel.rowcount
      timerstep2 = timer()
      timedelta21=timerstep2-timerstep1
      snowcursor.execute("insert into RUN_TABLE_DETAILS(BATCH,TASK_PHASE,TASK_DESC,STATUS,TABLE_NAME,PROCESS_COUNT,ELAPSED_TIME_IN_MIN,PROCESS_DATE) VALUES (" + str(self.batch) + ",'Funnel_Comparision_Load','"+COMPARISON_NAME_VAR_val+"',"+"'Completed','" + 'FUNNEL_IN_OUT' + "','" + str(rowcount) + "','" + str(timedelta21) + "',CURRENT_TIMESTAMP);")
      print("Loading completed for table FUNNEL_IN_OUT for snapshot  "+ COMPARISON_NAME_VAR_val +". It took "+ str(timedelta21) + " min and loaded # of rows " + str(rowcount))
      logging.info("Loading completed for table FUNNEL_IN_OUT for snapshot  "+ COMPARISON_NAME_VAR_val +". It took "+ str(timedelta21) + " min and loaded # of rows " + str(rowcount))

      updruntable="UPDATE RUN_TABLE SET STATUS_CD='COMPLETED',END_TIME=CURRENT_TIMESTAMP , TIME_TAKEN_IN_MINT= datediff(minute, START_TIME, CURRENT_TIMESTAMP) WHERE BATCH_NUM="+str(self.batch)+ " AND PROCESS_DESC="+"'"+COMPARISON_NAME_VAR_val+"'"
      print(updruntable)
      snowcursor.execute(updruntable)
      return 200
     except Exception as e:
      print("insert into FUNEEL_IN_OUT failed. Please check the sql")
      logging.error("insert into FUNEEL_IN_OUT failed. Please check the sql")
      updruntable = "UPDATE RUN_TABLE SET STATUS_CD='FAILED',END_TIME=CURRENT_TIMESTAMP , TIME_TAKEN_IN_MINT= datediff(minute, START_TIME, CURRENT_TIMESTAMP) WHERE BATCH_NUM=" + str(self.batch) + " AND PROCESS_DESC=" + "'" + COMPARISON_NAME_VAR_val + "'"
      snowcursor.execute(updruntable)
      return 400

     finally:
      snowcursor.close()

  else:
   print("Not a working day")
   logging.info("Not a working day")
   snowcursor.close()
   return 200
  snowcursor.close()
  return 200



#funnelinout=funnelinout('SEP-2018','WD08','WD01',40)
#returncode=funnelinout.processdata()
#print(returncode)