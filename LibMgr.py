#######################################################################################################
# GIS Library Manager
# Complete redesign of library management processes and strategy
#     Used to synchronize GIS library data between three different formats:
#     Coverages, Shapefiles and Enterprise Geodatabases
# Author: Harlan Marshall       Date: 10/10/2019
#
# The application is intended to be scheduled to run on a regular basis (nightly).
# The Metadata SQL Server database is the source for the information needed to keep the data in sync.
# The metadata provides information on the format where the data is maintained and what formats must be
#   updated when the source data changes.
# The COVER_NAME metadata field name is an historical artifact and should be read and understood as 
#   the 8 character feature class name. 
#   That metadata field is the feature class name in the entire library, not just coverages.
#
# Program Parameters:
#   All parameters are case-insensitive
#
#   EditGDBtoLIB - Used to sync changes made in the versioned maintenance geodatabases to library geodatabase.
#   StgSHPtoLIB - Used to sync changes made in shapefiles to the library geodatabase. 
#   StgGDBtoLIB - Used to sync changes made in library stage geodatabase to the library geodatabase.
#   COVtoLIB - Used to sync changes made in coverages to library geodatabase.
#   LIBtoSHP - Final step used to sync changes made in the library geodatabase to the shapefiles on the library file server.
#   FORCE - Used to disconnect users with locks in the library geodatabase.
#   -FC featClass,featClass,... - Indicates a comma-separated list of feature classes to process instead of processing all
#   -FX featClass,featClass,... - Indicates a comma-separated list of feature classes to exclude from processing
#   IGNORESTATUS - Used to cause sync regardless of timestamp comparison.
#
# Revision History:
# Date          Developer           Description
#
####################################################################################################################

logMsg = ''

import arcpy
import ConfigParser
import os, sys, traceback
import logging
import datetime
import pandas
import glob
import Metadata
import PC_Notification
import base64
import UpdateLib
import PC_Python

scriptName = 'GIS_LibraryManager'

# Set connection strings and get other settings from configuration file
parser = ConfigParser.SafeConfigParser()
parser.read('LibMgr.ini')
metaServer = parser.get('Source', 'metaServer').replace('"', '')
metaDb = parser.get('Source', 'metaDb')
libGDBDirectConn = parser.get('Library', 'libGDBDirectConn')
libGDBDb = parser.get('Library', 'libGDBDb')
libGDBSchema = parser.get('Library', 'libGDBSchema').upper()
logPath = parser.get('Local', 'logPath')
outLocalPath = parser.get('Local', 'outLocalPath')
outLocalFGDB = parser.get('Local', 'outLocalFGDB')
exchangeserver = parser.get('Notification', 'exchangeserver')
emailFrom = parser.get('Notification', 'emailFrom')
emailToList = parser.get('Notification', 'emailToList').replace('"','').split(',') # Parse items to python list object

# set up python logging before the overall try..except structure of the script
strDt = datetime.datetime.now().strftime("%Y-%m-%d %H-%M-%S") # string datetime for log file name
logging.basicConfig(level=logging.NOTSET)
logger = logging.getLogger(__name__)
logger.propagate = False
formatter = logging.Formatter('%(asctime)s %(message)s: %(module)s %(lineno)d', datefmt='%H:%M:%S') # create a logging format
formatterMessageOnly = logging.Formatter('%(message)s') # create a logging format for message only
# Info logger
logInfFilePath = logPath + '/' + scriptName + ' DETAIL LOG ' + strDt + '.log' # this will create a new file for every run
handler = logging.FileHandler(logInfFilePath, mode='w') # create an info log file handler
handler.setFormatter(formatterMessageOnly)
handler.setLevel(logging.INFO)
logger.addHandler(handler)
# Error and warning logger
logErrFilePath = logPath + '/' + scriptName + ' ERROR LOG ' + strDt + '.log' # this will create a new file for every run
handler = logging.FileHandler(logErrFilePath, mode='w') # create a warning/error log file handler
handler.setFormatter(formatterMessageOnly)
handler.setLevel(logging.WARNING)
logger.addHandler(handler)
# Console logger to send messages to the console as well
consoleHandler = logging.StreamHandler()
consoleHandler.setFormatter(formatterMessageOnly)
consoleHandler.setLevel(logging.NOTSET)
logger.addHandler(consoleHandler)

lstErrCnt = [0,0]   # create running error/warning count for notification.
                    #lstErrCnt[0] error count
                    #lstErrCnt[1] warning count

attachFilePathList = [logInfFilePath,logErrFilePath] # list used when we are writing to multiple log files
lstRefreshed = []   # list for feature class names that get refreshed for notification
sListRefreshed = ''


try:
    metaConnStr = 'DRIVER={SQL Server};SERVER=' + metaServer + ';DATABASE=' + metaDb + ';Trusted_Connection=yes'

    sNotifyArgs = ''
    sqlLstFC = ''
    sqlLstFX = ''

    ## Store arguments
    editgdbtolib = False
    covtolib = False
    stgshptolib = False
    stggdbtolib = False
    libtoshp = False
    force = False
    ignorestatus = False

    # strip first argument and convert the rest to lowercase
    argv = sys.argv[1:] # delete first argument
    i = 0
    while i < len(argv):
        argv[i] = argv[i].lower() # convert to lower case
        i = i + 1

    if 'editgdbtolib' in argv:
        editgdbtolib = True
        sNotifyArgs = 'EditGDBtoLIB'
    if 'covtolib' in argv:
        covtolib = True
        if len(sNotifyArgs) == 0:
            sNotifyArgs = 'COVtoLIB'
        else:
            sNotifyArgs = sNotifyArgs + ', COVtoLIB'
    if 'stgshptolib' in argv:
        stgshptolib = True
        if len(sNotifyArgs) == 0:
            sNotifyArgs = 'StgSHPtoLIB'
        else:
            sNotifyArgs = sNotifyArgs + ', StgSHPtoLIB'
    if 'stggdbtolib' in argv:
        stggdbtolib = True
        if len(sNotifyArgs) == 0:
            sNotifyArgs = 'StgGDBtoLIB'
        else:
            sNotifyArgs = sNotifyArgs + ', StgGDBtoLIB'
    if 'libtoshp' in argv:
        libtoshp = True
        if len(sNotifyArgs) == 0:
            sNotifyArgs = 'LIBtoSHP'
        else:
            sNotifyArgs = sNotifyArgs + ', LIBtoSHP'
    if 'force' in argv:
        force = True
        if len(sNotifyArgs) == 0:
            sNotifyArgs = 'FORCE'
        else:
            sNotifyArgs = sNotifyArgs + ', FORCE'
    if '-fc' in argv:
        iFc = argv.index('-fc') + 1
        sqlLstFC = '(\'' + '\',\''.join(str(fc) for fc in argv[iFc].split(",")) + '\')'
        if len(sNotifyArgs) == 0:
            sNotifyArgs = 'Include: ' + sqlLstFC
        else:
            sNotifyArgs = sNotifyArgs + ', Process Only: ' + sqlLstFC
    if '-fx' in argv:
        iFc = argv.index('-fx') + 1
        sqlLstFX = '(\'' + '\',\''.join(str(fc) for fc in argv[iFc].split(",")) + '\')'
        if len(sNotifyArgs) == 0:
            sNotifyArgs = 'Process All Excluding: ' + sqlLstFX
        else:
            sNotifyArgs = sNotifyArgs + ', Process All Excluding: ' + sqlLstFX
    if 'ignorestatus' in argv:
        ignorestatus = True
        if len(sNotifyArgs) == 0:
            sNotifyArgs = 'IGNORESTATUS'
        else:
            sNotifyArgs = sNotifyArgs + ', IGNORESTATUS'

    logger.critical('Configuration Settings: Email to = ' + str(emailToList))
    logger.critical('Program arguments: ' + sNotifyArgs + '\n')

    logger.critical('** START *******************************************************************************************')

    for l in logger.handlers: # switch logger formatter to full format
        l.setFormatter(formatter)

    outLocalFGDBPath = outLocalPath + '/' + outLocalFGDB
    if arcpy.Exists(outLocalFGDBPath):
        arcpy.Delete_management(outLocalFGDBPath)
    arcpy.CreateFileGDB_management(outLocalPath, outLocalFGDB)
    logger.info('SUCCESS - Replacing local staging file geodatabase.')

    # Load metadata for processing
    logMsg = 'Loading metadata for processing'
    df = Metadata.LoadMetadata(metaConnStr, sqlLstFC, sqlLstFX) # load from SQL table
    logger.info('SUCCESS - ' + logMsg)
    logMsg = ''

    # iterate metadata dataframe
    i = 0
    while (i < len(df)):
        row = df.iloc[i]
        libInput = row['LIBINPUT'].lower()
        featName = row['feature'].lower()
        libNameQual = libGDBDb + '.' + libGDBSchema + '.' + featName
        libPathName = libGDBDirectConn + '/' + libNameQual
        shpNameQual = featName + '.shp'

        # GDB maintenance feature classes
        if editgdbtolib and libInput == 'gdb std':
            try:
                rsltUpdated = UpdateLib.EditGDBtoLIB(row, ignorestatus, force, logger, lstErrCnt, metaConnStr)
            except Exception as e:
                pass # The error has already been logged and we want to continue with next feature class
            else:
                if rsltUpdated:
                    lstRefreshed.append('EditGDBtoLIB - ' + libNameQual)

        # Stage shapefiles
        if stgshptolib and libInput == 'shape std':
            try:
                rsltUpdated = UpdateLib.StgSHPtoLIB(row, ignorestatus, force, logger, lstErrCnt, metaConnStr)
            except Exception as e:
                pass # The error has already been logged and we want to continue with next feature class
            else:
                if rsltUpdated:
                    lstRefreshed.append('StgSHPtoLIB - ' + libNameQual)

        # Stage GDB feature classes
        if stggdbtolib and libInput == 'gdb spec':
            try:
                rsltUpdated = UpdateLib.StgGDBtoLIB(row, ignorestatus, force, logger, lstErrCnt, metaConnStr)
            except Exception as e:
                pass # The error has already been logged and we want to continue with next feature class
            else:
                if rsltUpdated:
                    lstRefreshed.append('StgGDBtoLIB - ' + libNameQual)

        # Coverages
        if covtolib and libInput == 'cover spec':
            try:
                rsltUpdated = UpdateLib.COVtoLIB(row, ignorestatus, force, logger, lstErrCnt, metaConnStr)
            except Exception as e:
                pass # The error has already been logged and we want to continue with next feature class
            else:
                if rsltUpdated:
                    lstRefreshed.append('COVtoLIB - ' + libNameQual)

        # GDBLib feature classes to library shapefiles
        if libtoshp:
            try:
                rsltUpdated = UpdateLib.LIBtoSHP(row, ignorestatus, force, logger, lstErrCnt)
            except Exception as e:
                pass # The error has already been logged and we want to continue with next feature class
            else:
                if rsltUpdated:
                    lstRefreshed.append('LIBtoSHP - ' + shpNameQual)
        i += 1

    for l in logger.handlers: # switch logger formatter to message only
        l.setFormatter(formatterMessageOnly)

    logger.critical('**  END  *******************************************************************************************')

    if len(lstRefreshed) > 0:
        for fc in lstRefreshed:
            sListRefreshed = sListRefreshed + '\t' + fc + '\n'
    else:
        sListRefreshed = '\tNone'

    logger.critical('\nFeature Classes Refreshed:\n' + sListRefreshed)

    # send email notification of process completion with attached log files
    if lstErrCnt[0] + lstErrCnt[1] > 0:
        subject = scriptName + ' ERRORS or WARNINGS  Error(s):' + str(lstErrCnt[0]) + '    Warning(s):' + str(lstErrCnt[1])
    else: 
        subject = scriptName + ' SUCCESS'
    tbmsg = 'Program arguments: ' + sNotifyArgs + '\n\nFeature classes refreshed:\n' + sListRefreshed
    PC_Notification.SendEmail_ListAttach(exchangeserver, emailFrom, emailToList, subject, tbmsg, attachFilePathList)

except Exception as e:
    # we only get here if there was an unhandled fatal error and we need to send a notification
    # log the current logMsg and traceback message as an error to the log file
    logger.error('ERROR - ' + logMsg, exc_info=True) # exc_info=True will add the traceback message
    logger.error('Fatal error encountered. Exiting the process.')

    # Get the fatal error traceback object seperately to display in the email body
    lstExcInfo = PC_Python.ParseTracebackInfo()
    tbmsg = lstExcInfo[2] + '\n' + sListRefreshed

    subject = scriptName + ' ERRORS or WARNINGS  Error(s):' + str(lstErrCnt[0]) + '    Warning(s):' + str(lstErrCnt[1])

    PC_Notification.SendEmail_ListAttach(exchangeserver, emailFrom, emailToList, subject, tbmsg, attachFilePathList)
