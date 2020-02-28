import arcpy
import PC_Geoprocessing
import sys, os, traceback
import LibMgrUtility
from datetime import datetime as datim
import Metadata
#import PC_Python
import ConfigParser

# custom error exception for all functions in this module
class UpdateLibError(Exception):
    pass

# custom warning exception for all functions in this module
class UpdateLibWarning(Exception):
    pass

def EditGDBtoLIB(row, ignorestatus, bForce, logger, lstErrCnt, metaConnStr):
    # Set connection strings and get other settings from configuration file
    parser = ConfigParser.SafeConfigParser()
    parser.read('LibMgr.ini')
    metaServer = parser.get('Source', 'metaServer')
    metaDb = parser.get('Source', 'metaDb')
    libGDBSchema = parser.get('Library', 'libGDBSchema')
    libGDBDirectConn = parser.get('Library', 'libGDBDirectConn')
    libGDBAdminConn = parser.get('Library', 'libGDBAdminConn')
    libGDBServer = parser.get('Library', 'libGDBServer')
    libGDBDb = parser.get('Library', 'libGDBDb')
    outLocalPath = parser.get('Local', 'outLocalPath')
    outLocalFGDB = parser.get('Local', 'outLocalFGDB')

    rsltUpdated = False
    bSync = False
    ddt = datim(1900,1,1) # 1900/01/01 initial value for target GDB feature class datetime
    sdt = datim(1900,1,1) # 1900/01/01 initial value for source GDB feature class datetime

    # parse STDEXPSOURCE server.dbname.schema to local variables
    stdExpSrc = row['STDEXPSOURCE'].lower()
    srcServerName = stdExpSrc.split('.')[0]
    srcDbName = stdExpSrc.split('.')[1]
    srcSchemaName = stdExpSrc.split('.')[2]
    
    # assemble source connections
    metaDBSqlConn = 'DRIVER={SQL Server};SERVER=' + metaServer + ';DATABASE=' + metaDb + ';Trusted_Connection=yes'
    srcGDBDirectConn = 'Database Connections/logon@' + srcDbName + '@' + srcServerName + '.sde'
    srcGDBSqlConn = 'DRIVER={SQL Server};SERVER=' + srcServerName + ';DATABASE=' + srcDbName + ';Trusted_Connection=yes'

    # assemble library SQL connection string
    libGDBSqlConn = 'DRIVER={SQL Server};SERVER=' + libGDBServer + ';DATABASE=' + libGDBDb + ';Trusted_Connection=yes'

    featName = row['feature'].lower()
    srcFeatNameQual = srcSchemaName + '.' + featName
    srcFeatPath = srcGDBDirectConn + '/' + srcFeatNameQual
    libFeatNameQual = libGDBSchema + '.' + featName
    libFeatPath = libGDBDirectConn + '/' + libFeatNameQual
    localFGDBPath = outLocalPath + '/' + outLocalFGDB
    localFeatPath = localFGDBPath + '/' + featName

    if ignorestatus: # set flag indicating if sync is required
        bSync = True

    logger.info(featName.upper())
    logger.info('\tSource: ' + srcFeatPath)
    logger.info('\tTarget: ' + libFeatPath)

    if not bSync: # if the sync flag is still false get the last modified datetimes to make determination
        if arcpy.Exists(srcFeatPath): # Check that the source feature class exists.
            try:
                srcdt = PC_Geoprocessing.GetVersionedGDBLastModDate(featName, srcGDBSqlConn, srcSchemaName)
                if arcpy.Exists(libFeatPath): # If the target GDBLib feature class exists, get last modified date
                    libdt = PC_Geoprocessing.GetNonVersionedGDBCreateDate(featName, libGDBSchema, libGDBSqlConn) # get last modified date (create date)
                else:
                    bSync = True
                if not bSync: # if the sync flag is still false, determine if source is newer than target
                    if srcdt > libdt:
                        bSync = True
            except Exception as e:
                bSync = True # if we can't determine the last modified datetimes, just go ahead with the update
        else: # source feature class doesn't exist. Raise UpdateLibError
            logger.error('\tERROR - Source feature class does not exist. Skipping refresh - ' + featName)
            lstErrCnt[0] += 1
            return
        logger.info('\tSUCCESS - Determining if source is newer than target - ' + str(bSync))

    if bSync: # finally, if the sync flag is true, proceed with update
        # create local copy of the source feature class to prep for library
        try:
            arcpy.FeatureClassToFeatureClass_conversion(srcFeatPath,localFGDBPath,featName)
        except Exception as e:
            logger.error('\tERROR - Creating local copy of the source feature class - ' + featName, exc_info=True)
            lstErrCnt[0] += 1
            return
        else:
            logger.info('\tSUCCESS - Creating local copy of the source feature class')

        # make required field changes to prep for library
        try:
            LibMgrUtility.PrepFieldsForLibrary(localFeatPath)
        except Exception as e:
            logger.error('\tERROR - Preparing feature class fields for library - ' + featName, exc_info=True)
            lstErrCnt[0] += 1
            return
        else:
            logger.info('\tSUCCESS - Preparing feature class fields for library')

        # remove domains to prep for library
        try:
            LibMgrUtility.PrepFeatClassForLibrary(localFeatPath, logger, lstErrCnt)
        #except LibMgrUtility.LibMgrUtilityWarning as e:
        #    logger.warning(e.message + ' on ' + featName)
        #    lstErrCnt[1] += 1
        except Exception as e:
            logger.error('\tERROR - Preparing feature class properties for library - ' + featName, exc_info=True)
            lstErrCnt[0] += 1
            return
        else:
            logger.info('\tSUCCESS - Preparing feature class properties for library')

        # copy the staged feature class to the library
        try:
            lstDisconnected = PC_Geoprocessing.SafeConvert(localFGDBPath, featName, libGDBDirectConn, libFeatNameQual, logger, '', '', bForce, libGDBAdminConn)
        except PC_Geoprocessing.PC_GeoprocessingError as e:
            logger.error('\tERROR - SafeConverting local copy to target - ' + featName, exc_info=True)
            lstErrCnt[0] += 1
            return
        except PC_Geoprocessing.PC_GeoprocessingWarning as e:
            logger.warning('\tWARNING - SafeConverting local copy to target - ' + featName, exc_info=True)
            lstErrCnt[1] += 1
        except Exception as e:
            logger.error('\tERROR - SafeConverting local copy to target - ' + featName, exc_info=True)
            lstErrCnt[0] += 1
            return
        else:
            updateDT = datim.strptime(datim.now().strftime('%Y-%m-%d %H:%M:%S'),'%Y-%m-%d %H:%M:%S')
            rsltUpdated = True

            # Update feature class metadata from the library metadata table
            try:
                Metadata.UpdateFCMetadata(libFeatPath, metaConnStr)
            except Exception as e:
                logger.warning('\tWARNING - Updating feature class metadata for ' + featName, exc_info=True)
                lstErrCnt[1] += 1
            else:
                logger.info('\tSUCCESS - Updating feature class metadata')

            # set privileges on the updated feature class
            try:
                LibMgrUtility.SetFeatureClassPrivileges(row['SENSITIVE'], row['SenSubGrp'], libGDBDirectConn, libGDBDb, libGDBSchema, featName, True)
            except Exception as e:
                logger.error('\tError - Setting privileges - ' + featName, exc_info=True)
                lstErrCnt[0] += 1
            else:
                logger.info('\tSUCCESS - Setting privileges')

            # update metadata
            row['DATETOUCHED'] = updateDT
            row['GDBDATE'] = updateDT
            try:
                Metadata.UpdateLibraryMetadata(row, metaDBSqlConn, logger)
            except Exception as e:
                logger.warning('\tWARNING - Updating metadata table for library feature class - ' + featName, exc_info=True)
                lstErrCnt[1] += 1
            else:
                logger.info('\tSUCCESS - Updating metadata table for library feature class')
    else:
        logger.info('\tLibrary feature class is up to date. Skipping refresh')

    return rsltUpdated


def StgSHPtoLIB(row, ignorestatus, bForce, logger, lstErrCnt, metaConnStr):
    # Set connection strings and get other settings from configuration file
    parser = ConfigParser.SafeConfigParser()
    parser.read('LibMgr.ini')
    metaServer = parser.get('Source', 'metaServer')
    metaDb = parser.get('Source', 'metaDb')
    libGDBSchema = parser.get('Library', 'libGDBSchema')
    libGDBDirectConn = parser.get('Library', 'libGDBDirectConn')
    libGDBAdminConn = parser.get('Library', 'libGDBAdminConn')
    libGDBServer = parser.get('Library', 'libGDBServer')
    libGDBDb = parser.get('Library', 'libGDBDb')
    outLocalPath = parser.get('Local', 'outLocalPath')
    outLocalFGDB = parser.get('Local', 'outLocalFGDB')

    rsltUpdated = False
    bSync = False
    ddt = datim(1900,1,1) # 1900/01/01 initial value for target GDB feature class datetime
    sdt = datim(1900,1,1) # 1900/01/01 initial value for source GDB feature class datetime
    
    # assemble metadata SQL connection string
    metaDBSqlConn = 'DRIVER={SQL Server};SERVER=' + metaServer + ';DATABASE=' + metaDb + ';Trusted_Connection=yes'

    # assemble library SQL connection string
    libGDBSqlConn = 'DRIVER={SQL Server};SERVER=' + libGDBServer + ';DATABASE=' + libGDBDb + ';Trusted_Connection=yes'
    
    featName = row['feature'].lower()
    srcPath = row['stagepath'].lower()
    if srcPath[-1] != '\\' and srcPath[-1] != '/': # Check for trailing slash. Set one if not there.
        srcPath = srcPath + '/'
    srcFeatNameQual = featName + '.shp'
    srcFeatPath = srcPath + srcFeatNameQual
    libFeatNameQual = libGDBSchema + '.' + featName
    libFeatPath = libGDBDirectConn + '/' + libFeatNameQual
    localFGDBPath = outLocalPath + '/' + outLocalFGDB
    localFeatPath = localFGDBPath + '/' + featName

    if ignorestatus: # set flag indicating if sync is required
        bSync = True

    logger.info(featName.upper()) # for staged shapefiles, determine if exists before logging anything
    logger.info('\tSource: ' + srcFeatPath)
    logger.info('\tTarget: ' + libFeatPath)

    if not bSync: # if the sync flag is still false get the last modified datetimes to make determination
        if os.path.exists(srcFeatPath): # Check that the source shapefile exists
            try:
                srcdt = PC_Geoprocessing.GetShapefileLastModDate(srcFeatPath)
                if arcpy.Exists(libFeatPath): # If the target GDBLib feature class exists, get last modified date
                    libdt = PC_Geoprocessing.GetNonVersionedGDBCreateDate(featName, libGDBSchema, libGDBSqlConn) # get last modified date (create date)
                else:
                    bSync = True
                if not bSync: # if the sync flag is still false, determine if source is newer than target
                    if srcdt > libdt:
                        bSync = True
            except Exception as e:
                bSync = True # if we can't determine the last modified datetimes, just go ahead with the update
        else: # source shapefile doesn't exist. Don't log any message. Get next shapefile.
            return
        logger.info('\tSUCCESS - Determining if source is newer than target - ' + str(bSync))

    if bSync: # finally, if the sync flag is true, proceed with update
        # create local FGDB version of the source shapefile to prep for library
        try:
            arcpy.FeatureClassToFeatureClass_conversion(srcFeatPath,localFGDBPath,featName)
        except Exception as e:
            logger.error('\tERROR - Creating local FGDB version of the source shapefile - ' + featName, exc_info=True)
            lstErrCnt[0] += 1
            return
        else:
            logger.info('\tSUCCESS - Creating local FGDB version of the source shapefile')

        # make required field changes to prep for library
        try:
            LibMgrUtility.PrepFieldsForLibrary(localFeatPath)
        except Exception as e:
            logger.error('\tERROR - Preparing feature class fields for library - ' + featName, exc_info=True)
            lstErrCnt[0] += 1
            return
        else:
            logger.info('\tSUCCESS - Preparing feature class fields for library')

        # copy the staged feature class to the library
        try:
            lstDisconnected = PC_Geoprocessing.SafeConvert(localFGDBPath, featName, libGDBDirectConn, libFeatNameQual, logger, '', '', bForce, libGDBAdminConn)
        except PC_Geoprocessing.PC_GeoprocessingError as e:
            logger.error('\tERROR - SafeConverting local copy to target - ' + featName, exc_info=True)
            lstErrCnt[0] += 1
            return
        except PC_Geoprocessing.PC_GeoprocessingWarning as e:
            logger.warning('\tWARNING - SafeConverting local copy to target - ' + featName, exc_info=True)
            lstErrCnt[1] += 1
        except Exception as e:
            logger.error('\tERROR - SafeConverting local copy to target - ' + featName, exc_info=True)
            lstErrCnt[0] += 1
            return
        else:
            updateDT = datim.strptime(datim.now().strftime('%Y-%m-%d %H:%M:%S'),'%Y-%m-%d %H:%M:%S')
            rsltUpdated = True

            # Update feature class metadata from the library metadata table
            try:
                Metadata.UpdateFCMetadata(libFeatPath, metaConnStr)
            except Exception as e:
                logger.warning('\tWARNING - Updating feature class metadata for ' + featName, exc_info=True)
                lstErrCnt[1] += 1
            else:
                logger.info('\tSUCCESS - Updating feature class metadata')

            try:
                LibMgrUtility.SetFeatureClassPrivileges(row['SENSITIVE'], row['SenSubGrp'], libGDBDirectConn, libGDBDb, libGDBSchema, featName, True)
            except Exception as e:
                logger.error('\tError - Setting privileges - ' + featName, exc_info=True)
                lstErrCnt[0] += 1
            else:
                logger.info('\tSUCCESS - Setting privileges')

            # update metadata
            row['DATETOUCHED'] = updateDT
            row['GDBDATE'] = updateDT
            try:
                Metadata.UpdateLibraryMetadata(row, metaDBSqlConn, logger)
            except Exception as e:
                logger.warning('\tWARNING - Updating metadata table for library feature class - ' + featName, exc_info=True)
                lstErrCnt[1] += 1
            else:
                logger.info('\tSUCCESS - Updating metadata table for library feature class')
    else:
        logger.info('\tLibrary feature class is up to date. Skipping refresh')

    return rsltUpdated

def StgGDBtoLIB(row, ignorestatus, bForce, logger, lstErrCnt, metaConnStr):
    # Set connection strings and get other settings from configuration file
    parser = ConfigParser.SafeConfigParser()
    parser.read('LibMgr.ini')
    metaServer = parser.get('Source', 'metaServer')
    metaDb = parser.get('Source', 'metaDb')
    stageGDBDirectConn = parser.get('Source', 'stageGDBDirectConn')
    stageGDBServer = parser.get('Source', 'stageGDBServer')
    stageGDBDb = parser.get('Source', 'stageGDBDb')
    stageGDBSchema = parser.get('Source', 'stageGDBSchema')
    libGDBSchema = parser.get('Library', 'libGDBSchema')
    libGDBDirectConn = parser.get('Library', 'libGDBDirectConn')
    libGDBAdminConn = parser.get('Library', 'libGDBAdminConn')
    libGDBServer = parser.get('Library', 'libGDBServer')
    libGDBDb = parser.get('Library', 'libGDBDb')
    outLocalPath = parser.get('Local', 'outLocalPath')
    outLocalFGDB = parser.get('Local', 'outLocalFGDB')

    rsltUpdated = False
    bSync = False
    ddt = datim(1900,1,1) # 1900/01/01 initial value for target GDB feature class datetime
    sdt = datim(1900,1,1) # 1900/01/01 initial value for source GDB feature class datetime
    
    # assemble source SQL connection strings
    metaDBSqlConn = 'DRIVER={SQL Server};SERVER=' + metaServer + ';DATABASE=' + metaDb + ';Trusted_Connection=yes'
    gdbStgSqlConn = 'DRIVER={SQL Server};SERVER=' + stageGDBServer + ';DATABASE=' + stageGDBDb + ';Trusted_Connection=yes'

    # assemble library SQL connection string
    libGDBSqlConn = 'DRIVER={SQL Server};SERVER=' + libGDBServer + ';DATABASE=' + libGDBDb + ';Trusted_Connection=yes'

    featName = row['feature'].lower()
    srcFeatNameQual = stageGDBSchema + '.' + featName
    srcFeatPath = stageGDBDirectConn + '/' + srcFeatNameQual
    libFeatNameQual = libGDBSchema + '.' + featName
    libFeatPath = libGDBDirectConn + '/' + libFeatNameQual
    localFGDBPath = outLocalPath + '/' + outLocalFGDB
    localFeatPath = localFGDBPath + '/' + featName

    if ignorestatus: # set flag indicating if sync is required
        bSync = True

    logger.info(featName.upper()) # for staged gdb feature classes, determine if exists before logging anything
    logger.info('\tSource: ' + srcFeatPath)
    logger.info('\tTarget: ' + libFeatPath)

    if not bSync: # if the sync flag is still false get the last modified datetimes to make determination
        if arcpy.Exists(srcFeatPath): # Check that the source feature class exists.

            try:
                srcdt = PC_Geoprocessing.GetNonVersionedGDBCreateDate(featName, stageGDBSchema, gdbStgSqlConn)
                if arcpy.Exists(libFeatPath): # If the target GDBLib feature class exists, get last modified date
                    libdt = PC_Geoprocessing.GetNonVersionedGDBCreateDate(featName, libGDBSchema, libGDBSqlConn) # get last modified date (create date)
                else:
                    bSync = True
                if not bSync: # if the sync flag is still false, determine if source is newer than target
                    if srcdt > libdt:
                        bSync = True
            except Exception as e:
                bSync = True # if we can't determine the last modified datetimes, just go ahead with the update
        else: # source feature class doesn't exist. Don't log any message. Get next feature class.
            return
        logger.info('\tSUCCESS - Determining if source is newer than target - ' + str(bSync))

    if bSync: # finally, if the sync flag is true, proceed with update
        # create local copy of the source feature class to prep for library
        try:
            arcpy.FeatureClassToFeatureClass_conversion(srcFeatPath,localFGDBPath,featName)
        except Exception as e:
            logger.error('\tERROR - Creating local copy of the source feature class - ' + featName, exc_info=True)
            lstErrCnt[0] += 1
            return
        else:
            logger.info('\tSUCCESS - Creating local copy of the source feature class')

        # make required field changes to prep for library
        try:
            LibMgrUtility.PrepFieldsForLibrary(localFeatPath)
        except Exception as e:
            logger.error('\tERROR - Preparing feature class fields for library - ' + featName, exc_info=True)
            lstErrCnt[0] += 1
            return
        else:
            logger.info('\tSUCCESS - Preparing feature class fields for library')

        # remove domains to prep for library
        try:
            LibMgrUtility.PrepFeatClassForLibrary(localFeatPath, logger, lstErrCnt)
        except LibMgrUtility.LibMgrUtilityWarning as e:
            logger.warning(e.message + ' on ' + featName)
            lstErrCnt[1] += 1
        except Exception as e:
            logger.error('\tERROR - Preparing feature class properties for library - ' + featName, exc_info=True)
            lstErrCnt[0] += 1
            return
        else:
            logger.info('\tSUCCESS - Preparing feature class properties for library')

        # copy the staged feature class to the library
        try:
            lstDisconnected = PC_Geoprocessing.SafeConvert(localFGDBPath, featName, libGDBDirectConn, libFeatNameQual, logger, '', '', bForce, libGDBAdminConn)
        except PC_Geoprocessing.PC_GeoprocessingError as e:
            logger.error('\tERROR - SafeConverting local copy to target - ' + featName, exc_info=True)
            lstErrCnt[0] += 1
            return
        except PC_Geoprocessing.PC_GeoprocessingWarning as e:
            logger.warning('\tWARNING - SafeConverting local copy to target - ' + featName, exc_info=True)
            lstErrCnt[1] += 1
        except Exception as e:
            logger.error('\tERROR - SafeConverting local copy to target - ' + featName, exc_info=True)
            lstErrCnt[0] += 1
            return
        else:
            updateDT = datim.strptime(datim.now().strftime('%Y-%m-%d %H:%M:%S'),'%Y-%m-%d %H:%M:%S')
            rsltUpdated = True

            # Update feature class metadata from the library metadata table
            try:
                Metadata.UpdateFCMetadata(libFeatPath, metaConnStr)
            except Exception as e:
                logger.warning('\tWARNING - Updating feature class metadata for ' + featName, exc_info=True)
                lstErrCnt[1] += 1
            else:
                logger.info('\tSUCCESS - Updating feature class metadata')

            try:
                LibMgrUtility.SetFeatureClassPrivileges(row['SENSITIVE'], row['SenSubGrp'], libGDBDirectConn, libGDBDb, libGDBSchema, featName, True)
            except Exception as e:
                logger.error('\tError - Setting privileges - ' + featName, exc_info=True)
                lstErrCnt[0] += 1
            else:
                logger.info('\tSUCCESS - Setting privileges')

            # update metadata
            row['DATETOUCHED'] = updateDT
            row['GDBDATE'] = updateDT
            try:
                Metadata.UpdateLibraryMetadata(row, metaDBSqlConn, logger)
            except Exception as e:
                logger.warning('\tWARNING - Updating metadata table for library feature class - ' + featName, exc_info=True)
                lstErrCnt[1] += 1
            else:
                logger.info('\tSUCCESS - Updating metadata table for library feature class')
    else:
        logger.info('\tLibrary feature class is up to date. Skipping refresh')

    return rsltUpdated


def COVtoLIB(row, ignorestatus, bForce, logger, lstErrCnt, metaConnStr):
    # Set connection strings and get other settings from configuration file
    parser = ConfigParser.SafeConfigParser()
    parser.read('LibMgr.ini')
    metaServer = parser.get('Source', 'metaServer')
    metaDb = parser.get('Source', 'metaDb')
    libGDBSchema = parser.get('Library', 'libGDBSchema')
    libGDBDirectConn = parser.get('Library', 'libGDBDirectConn')
    libGDBAdminConn = parser.get('Library', 'libGDBAdminConn')
    libGDBServer = parser.get('Library', 'libGDBServer')
    libGDBDb = parser.get('Library', 'libGDBDb')
    outLocalPath = parser.get('Local', 'outLocalPath')
    outLocalFGDB = parser.get('Local', 'outLocalFGDB')

    rsltUpdated = False
    bSync = False
    ddt = datim(1900,1,1) # 1900/01/01 initial value for target GDB feature class datetime
    sdt = datim(1900,1,1) # 1900/01/01 initial value for source coverage datetime
    
    # assemble metadata SQL connection string
    metaDBSqlConn = 'DRIVER={SQL Server};SERVER=' + metaServer + ';DATABASE=' + metaDb + ';Trusted_Connection=yes'

    # assemble library SQL connection string
    libGDBSqlConn = 'DRIVER={SQL Server};SERVER=' + libGDBServer + ';DATABASE=' + libGDBDb + ';Trusted_Connection=yes'
    
    featName = row['feature'].lower()
    srcPath = row['updatepath'].lower().replace('\\shapes', '\\covers')
    if srcPath[-1] != '\\' and srcPath[-1] != '/': # Check for trailing slash. Set one if not there.
        srcPath = srcPath + '/'
    srcFeatType = row['FEAT_TYPE'].lower()
    featType = srcFeatType.replace('line', 'arc') # if metadata FEAT_TYPE is "line", replace with coverage type "arc"
    srcFeatPath = srcPath + featName
    srcFeatNameQual = featName + '\\' + featType
    srcFeatTypePath = srcFeatPath + '\\' + featType
    libFeatNameQual = libGDBSchema + '.' + featName
    libFeatPath = libGDBDirectConn + '/' + libFeatNameQual
    localFGDBPath = outLocalPath + '/' + outLocalFGDB
    localFeatPath = localFGDBPath + '/' + featName

    if ignorestatus: # set flag indicating if sync is required
        bSync = True

    logger.info(featName.upper())
    logger.info('\tSource: ' + srcFeatPath)
    logger.info('\tTarget: ' + libFeatPath)

    if not bSync: # if the sync flag is still false get the last modified datetimes to make determination
        if arcpy.Exists(srcFeatPath): # Check that the source coverage exists
            try:
                srcdt = PC_Geoprocessing.GetCoverageLastModDate(srcFeatPath)
                if arcpy.Exists(libFeatPath): # If the target GDBLib feature class exists, get last modified date
                    libdt = PC_Geoprocessing.GetNonVersionedGDBCreateDate(featName, libGDBSchema, libGDBSqlConn) # get last modified date (create date)
                else:
                    bSync = True
                if not bSync: # if the sync flag is still false, determine if source is newer than target
                    if srcdt > libdt:
                        bSync = True
            except Exception as e:
                bSync = True # if we can't determine the last modified datetimes, just go ahead with the update
        else: # source coverage doesn't exist. Raise UpdateLibError
            logger.error('\tERROR - Source coverage does not exist. Skipping refresh - ' + featName)
            lstErrCnt[0] += 1
            return
        logger.info('\tSUCCESS - Determining if source is newer than target - ' + str(bSync))

    if bSync: # finally, if the sync flag is true, proceed with update
        # create local FGDB version of the source coverage to prep for library
        try:
            arcpy.FeatureClassToFeatureClass_conversion(srcFeatTypePath,localFGDBPath,featName)
        except Exception as e:
            logger.error('\tERROR - Creating local FGDB version of the source coverage - ' + featName, exc_info=True)
            lstErrCnt[0] += 1
            return
        else:
            logger.info('\tSUCCESS - Creating local FGDB version of the source coverage')

        # make required field changes to prep for library
        try:
            LibMgrUtility.PrepFieldsForLibrary(localFeatPath)
        except Exception as e:
            logger.error('\tERROR - Preparing feature class fields for library - ' + featName, exc_info=True)
            lstErrCnt[0] += 1
            return
        else:
            logger.info('\tSUCCESS - Preparing feature class fields for library')

        # copy the staged feature class to the library
        try:
            lstDisconnected = PC_Geoprocessing.SafeConvert(localFGDBPath, featName, libGDBDirectConn, libFeatNameQual, logger, '', '', bForce, libGDBAdminConn)
        except PC_Geoprocessing.PC_GeoprocessingError as e:
            logger.error('\tERROR - SafeConverting local copy to target - ' + featName, exc_info=True)
            lstErrCnt[0] += 1
            return
        except PC_Geoprocessing.PC_GeoprocessingWarning as e:
            logger.warning('\tWARNING - SafeConverting local copy to target - ' + featName, exc_info=True)
            lstErrCnt[1] += 1
        except Exception as e:
            logger.error('\tERROR - SafeConverting local copy to target - ' + featName, exc_info=True)
            lstErrCnt[0] += 1
            return
        else:
            updateDT = datim.strptime(datim.now().strftime('%Y-%m-%d %H:%M:%S'),'%Y-%m-%d %H:%M:%S')
            rsltUpdated = True

            # Update feature class metadata from the library metadata table
            try:
                Metadata.UpdateFCMetadata(libFeatPath, metaConnStr)
            except Exception as e:
                logger.warning('\tWARNING - Updating feature class metadata for ' + featName, exc_info=True)
                lstErrCnt[1] += 1
            else:
                logger.info('\tSUCCESS - Updating feature class metadata')

            try:
                LibMgrUtility.SetFeatureClassPrivileges(row['SENSITIVE'], row['SenSubGrp'], libGDBDirectConn, libGDBDb, libGDBSchema, featName, True)
            except Exception as e:
                logger.error('\tError - Setting privileges - ' + featName, exc_info=True)
                lstErrCnt[0] += 1
            else:
                logger.info('\tSUCCESS - Setting privileges')

            # update metadata
            row['DATETOUCHED'] = updateDT
            row['GDBDATE'] = updateDT
            try:
                Metadata.UpdateLibraryMetadata(row, metaDBSqlConn, logger)
            except Exception as e:
                logger.warning('\tWARNING - Updating metadata table for library feature class - ' + featName, exc_info=True)
                lstErrCnt[1] += 1
            else:
                logger.info('\tSUCCESS - Updating metadata table for library feature class')
    else:
        logger.info('\tLibrary feature class is up to date. Skipping refresh')

    return rsltUpdated


def LIBtoSHP(row, ignorestatus, force, logger, lstErrCnt):
    # Set connection strings and get other settings from configuration file
    parser = ConfigParser.SafeConfigParser()
    parser.read('LibMgr.ini')
    metaServer = parser.get('Source', 'metaServer')
    metaDb = parser.get('Source', 'metaDb')
    libGDBSchema = parser.get('Library', 'libGDBSchema')
    libGDBDirectConn = parser.get('Library', 'libGDBDirectConn')
    libGDBServer = parser.get('Library', 'libGDBServer')
    libGDBDb = parser.get('Library', 'libGDBDb')
    geographicWKID = parser.get('Settings', 'geographicWKID')
    outLocalPath = parser.get('Local', 'outLocalPath')
    outLocalFGDB = parser.get('Local', 'outLocalFGDB')

    rsltUpdated = False
    bSync = False
    ddt = datim(1900,1,1) # 1900/01/01 initial value for target GDB feature class datetime
    sdt = datim(1900,1,1) # 1900/01/01 initial value for source coverage datetime
    
    # assemble metadata SQL connection string
    metaDBSqlConn = 'DRIVER={SQL Server};SERVER=' + metaServer + ';DATABASE=' + metaDb + ';Trusted_Connection=yes'

    # assemble library SQL connection string
    libGDBSqlConn = 'DRIVER={SQL Server};SERVER=' + libGDBServer + ';DATABASE=' + libGDBDb + ';Trusted_Connection=yes'

    featName = row['feature'].lower()
    srcFeatNameQual = libGDBSchema + '.' + featName
    srcFeatPath = libGDBDirectConn + '/' + srcFeatNameQual
    shplibFeatNameQual = featName + '.shp'
    shplibPath = row['updatepath'].lower().replace('\\covers\\','\\shapes\\')
    if shplibPath[-1] != '\\' and shplibPath[-1] != '/': # Check for trailing slash. Set one if not there.
        shplibPath = shplibPath + '/'
    shplibFeatPath = shplibPath + shplibFeatNameQual

    if ignorestatus: # set flag indicating if sync is required
        bSync = True

    logger.info(featName.upper())
    logger.info('\tSource: ' + srcFeatPath)
    logger.info('\tTarget: ' + shplibFeatPath)

    if not bSync: # if the sync flag is still false get the last modified datetimes to make determination
        if arcpy.Exists(srcFeatPath): # Check that the source feature class exists.
            try:
                srcdt = PC_Geoprocessing.GetNonVersionedGDBCreateDate(featName, libGDBSchema, libGDBSqlConn)
                if arcpy.Exists(shplibFeatPath): # If the target library shapefile exists, get last modified date
                    shplibdt = PC_Geoprocessing.GetShapefileLastModDate(shplibFeatPath) # get last modified date
                else:
                    bSync = True
                if not bSync: # if the sync flag is still false, determine if source is newer than target
                    if srcdt > shplibdt:
                        bSync = True
            except Exception as e:
                bSync = True # if we can't determine the last modified datetimes, just go ahead with the update
        else: # source feature class doesn't exist. Raise UpdateLibError
            logger.error('\tERROR - Source feature class does not exist. Skipping refresh - ' + featName)
            lstErrCnt[0] += 1
            return
        logger.info('\tSUCCESS - Determining if source is newer than target - ' + str(bSync))

    if bSync: # finally, if the sync flag is true, proceed with update
        # copy the staged feature class to the library
        try:
            lstDisconnected = PC_Geoprocessing.SafeConvert(libGDBDirectConn, srcFeatNameQual, shplibPath, shplibFeatNameQual, logger)
        except PC_Geoprocessing.PC_GeoprocessingError as e:
            logger.error('\tERROR - SafeConverting local copy to target - ' + featName, exc_info=True)
            lstErrCnt[0] += 1
            return
        except PC_Geoprocessing.PC_GeoprocessingWarning as e:
            logger.warning('\tWARNING - SafeConverting local copy to target - ' + featName, exc_info=True)
            lstErrCnt[1] += 1
        except Exception as e:
            logger.error('\tERROR - SafeConverting local copy to target - ' + featName, exc_info=True)
            lstErrCnt[0] += 1
            return
        else:
            updateDT = datim.strptime(datim.now().strftime('%Y-%m-%d %H:%M:%S'),'%Y-%m-%d %H:%M:%S')
            rsltUpdated = True

            # project shapefile to copy in shapes_latlon folder if MAKELATLON is true
            if ((row['MAKELATLON']) and (not row['SENSITIVE'])): # don't project copies of sensitive shapefiles
                logger.info('\tProjecting feature class to geographic spatial reference (lat/lon) shapefile')
                shplibGeoPath = shplibPath.replace('\\shapes\\', '\\shapes_lat_lon\\')
                shplibGeoFeatFilePath = shplibGeoPath + shplibFeatNameQual
                srGeographic = arcpy.SpatialReference(int(geographicWKID))
                try: # project the feature class to geographic spatial reference (lat/lon) in local FGDB
                    localFGDB = outLocalPath + '/' + outLocalFGDB
                    localGeoFeatFilePath = localFGDB + '/' + featName
                    if arcpy.Exists(localGeoFeatFilePath):
                        arcpy.Delete_management(localGeoFeatFilePath)
                    arcpy.Project_management(in_dataset=srcFeatPath, out_dataset=localGeoFeatFilePath, out_coor_system=srGeographic)
                except Exception as e:
                    logger.error('\tERROR - Projecting feature class to lat/lon in local FGDB', exc_info=True)
                    lstErrCnt[0] += 1
                else:
                    logger.info('\tSUCCESS - Projecting feature class to lat/lon in local FGDB')

                # copy the projected feature class to geographic spatial reference (lat/lon) shapefile library
                try:
                    lstDisconnected = PC_Geoprocessing.SafeConvert(localFGDB, featName, shplibGeoPath, shplibFeatNameQual, logger)
                except PC_Geoprocessing.PC_GeoprocessingError as e:
                    logger.error('\tERROR - SafeConverting local copy to lat/lon target - ' + featName, exc_info=True)
                    lstErrCnt[0] += 1
                except PC_Geoprocessing.PC_GeoprocessingWarning as e:
                    logger.warning('\tWARNING - SafeConverting local copy to lat/lon target - ' + featName, exc_info=True)
                    lstErrCnt[1] += 1
                except Exception as e:
                    logger.error('\tERROR - SafeConverting local copy to lat/lon target - ' + featName, exc_info=True)
                    lstErrCnt[0] += 1

            # update metadata
            row['DATETOUCHED'] = updateDT
            row['SHPDATE'] = updateDT
            row['SHAPEFILESIZE'] = PC_Geoprocessing.GetShapefileSize(shplibPath, shplibFeatNameQual)
            try:
                Metadata.UpdateLibraryMetadata(row, metaDBSqlConn, logger)
            except Exception as e:
                logger.warning('\tWARNING - Updating metadata table for library feature class - ' + featName, exc_info=True)
                lstErrCnt[1] += 1
            else:
                logger.info('\tSUCCESS - Updating metadata table for library feature class')
    else:
        logger.info('\tLibrary shapefile is up to date. Skipping refresh')

    return rsltUpdated
