from FIFO import snowflakeconnect
from FIFO import Load_Funnel_Daily_Tables
from FIFO import Load_Funnel_In_Out
from datetime import date, datetime, timedelta
import logging



class fifodataflow():
    '''This class control the workflow of data processing calling other classes and methods.
    It first runs the daily snapsot tables oad and then funnel snapshot. Other than call the  methods
    it crates and passes batch number , triggering evants, return codes'''
    def __init__(self):
        pass
    def processdata(self):
     try:
        logging.basicConfig(filename="FIFOout.log", format='%(asctime)s %(message)s', level=logging.INFO)
        logging.info("Funnel data loading process started")
        print("Funnel data loading process started")

        today = datetime.now()
        year = today.year
        mon_column = today.strftime("%b").upper()
        periodname = mon_column + '-' + str(year)

        snowflakecon = snowflakeconnect.snowflakeconnect()
        snowcur = snowflakecon.createcur()


        '''Get max number from the load sequence'''
        '''Get next batch number'''
        checkbatch = "SELECT NVL(MAX(BATCH_NUM),0) FROM RUN_TABLE; "
        snowexec = snowcur.execute(checkbatch)
        getbatchnum = snowexec.fetchone()
        getbatchnum = int(getbatchnum[0]) + 1
        logging.info("The batch number for this load is "+str(getbatchnum))
        print("The batch number for this load is "+str(getbatchnum))
        '''Check working day for today'''
        checkwdstmnt = "select NVL(WORKDAY,'WE') FROM WD_CALENDER  WHERE %s = CURRENT_DATE; " % (mon_column)
        snowexec = snowcur.execute(checkwdstmnt)
        getwd = snowexec.fetchone()
        wdtoday=getwd[0]

        '''Check if the daily refresh is completed by Stich data, Processed flag 'N' means the data refresh done by Stich but
        not further processed yet in snowflake'''
        snowexe = snowcur.execute("select nvl(PROCESSED_FLAG,'N') from BASE_TABLES_RFRESH_CAPTURE;")
        result = snowexe.fetchone()

        if result[0]=='N':
           daily_load=Load_Funnel_Daily_Tables.loadfunnelailytables(getbatchnum)
           loadresult=daily_load.ProcessData()
           print(loadresult)
           if loadresult==200:

              WD=['WD02','WD05','WD10','WD15']
              print("todays Wd before funnel processing "+wdtoday)
              if wdtoday in WD:
               wdindex=WD.index(wdtoday)
               print("The index is"+str(wdindex))
               if wdindex>0:

                   for i in range(0,wdindex):

                       PERIOD_NAME_VAR_val=periodname
                       CURRENT_WD_INFO_VAR_val=wdtoday
                       PREV_WD_INFO_VAR_val=WD[i]
                       funnelload=Load_Funnel_In_Out.funnelinout(PERIOD_NAME_VAR_val,CURRENT_WD_INFO_VAR_val,PREV_WD_INFO_VAR_val,getbatchnum)
                       loadresultfunnel=funnelload.processdata()
                       if loadresultfunnel==200:
                        print(" Funnel load success for "+CURRENT_WD_INFO_VAR_val+ "and "+PREV_WD_INFO_VAR_val+ "for period "+periodname)
                        logging.info(" Funnel load success for "+CURRENT_WD_INFO_VAR_val+ "and "+PREV_WD_INFO_VAR_val+ "for period "+periodname)
                        return 200



                       else:
                           print(" Funnel load failed for " + CURRENT_WD_INFO_VAR_val + " and " + PREV_WD_INFO_VAR_val + " for period " + periodname)
                           logging.info(" Funnel load failed for " + CURRENT_WD_INFO_VAR_val + " "
                                                                                               "and " + PREV_WD_INFO_VAR_val + " for period " + periodname)
                           return 400
               else:
                   print("Today is WD02. We have to wait for WD05 to process the funnel ")
                   logging.info("Today is WD02. We have to wait for WD05 to process the funnel ")
                   return 200

           else:
               print("Daily snapshot load failed. So, not processing the funnel")
               logging.error("Daily snapshot load failed. So, not processing the funnel")
               return 400

        else:
           print("Todays data is already processed")
           return 200
           snowcur.close()

     except Exception as e:
      print("Sales analytics load failed")
      print(e)
      logging.error("Sales analytics load failed")
      logging.error(e)
     finally:

         print("Closing Connection. Exit form Sales Analytcs Load")
         logging.error("Closing Connection. Exit form Sales Analytcs Load")
         snowcur.close()

fifo=fifodataflow()
resultcode=fifo.processdata()
print(resultcode)