# LibMgrUtility.py - Functions to perform miscellaneous utility tasks
#
# Revision History:
# Date          Developer           Description
# 
######################################################################################################################

import arcpy
import PC_Python
import ConfigParser

# custom error exception for all functions in this module
class LibMgrUtilityError(Exception):
    pass

# custom warning exception for all functions in this module
class LibMgrUtilityWarning(Exception):
    pass


# Grant or revoke proper permissions (parameter "grant" is True=Grant, False=Revoke)
def SetFeatureClassPrivileges(sensitive, sensubgrp, sdeConn, dbName, schemaOwner
                              , featName, grant):
    funcName = 'SetFeatureClassPrivileges'
    userName = []

    if sensitive:
        userName.append('SPREAD')
        if sensubgrp.lower() <> 'none' and sensubgrp <> None:
            if sensubgrp.lower() == 'cultres':
                userName.append('cultres_read')
            if sensubgrp.lower() == 'infotech':
                userName.append('infotech_read')
            if sensubgrp.lower() == 'cultres_natres':
                userName.append('cultres_natres_read')
            if sensubgrp.lower() == 'natres':
                userName.append('natres_read')
            if sensubgrp.lower() == 'wwconres':
                userName.append('wwconres_read')
    else:
        userName.append('GISREAD')
    inFeature = '"' + sdeConn + '\\' + dbName + '.' + schemaOwner + '.' + featName + '"'
    if grant:
        for user in userName:
            arcpy.ChangePrivileges_management(in_dataset=inFeature, user=user, View="GRANT", Edit="AS_IS")
    else:
        for user in userName:
            arcpy.ChangePrivileges_management(in_dataset=inFeature, user=user, View="REVOKE", Edit="REVOKE")


def PrepFieldsForLibrary(featPath):
    # Set connection strings and get other settings from configuration file
    parser = ConfigParser.SafeConfigParser()
    parser.read('LibMgr.ini')
    geographicWKID = parser.get('Settings', 'geographicWKID')

    srGeographic = arcpy.SpatialReference(int(geographicWKID))

    # create list of field names (uppercase)
    lstFields = arcpy.ListFields(featPath)
    lstFieldNames = []
    for fld in lstFields:
        lstFieldNames.append(fld.name.lower()) # change all field names to lowercase

    ## if field PC_UID doesn't exist, add and calculate it
    #if 'pc_uid' not in lstFieldNames:
    #    # Add and set unique ID 'PC_UID' field
    #    arcpy.AddField_management(in_table=featPath, \
    #        field_name="PC_UID", field_type="LONG", field_precision="", field_scale="", \
    #        field_length="", field_alias="", field_is_nullable="NULLABLE", \
    #        field_is_required="NON_REQUIRED", field_domain="")

    #    if 'objectid' in lstFieldNames:
    #        strExp = "!OBJECTID! + 1"
    #    else:
    #        strExp = "!FID! + 1"

    #    arcpy.CalculateField_management(in_table=featPath, field="PC_UID", \
    #        expression=strExp, expression_type="PYTHON_9.3")
    
    # if field starting with 'EDITOR_N' (EDITOR_NAM or EDITOR_NAME) exists, delete it
    iRslt = PC_Python.FindStartsWithInStringList(lstFieldNames, 'EDITOR_N', False) # case insensitive compare
    if iRslt >= 0:
        dropField = lstFieldNames[iRslt]
        arcpy.DeleteField_management(in_table=featPath,drop_field=dropField)
    
    # if field starting with 'EDIT_D' (EDIT_DATE) exists, delete it
    iRslt = PC_Python.FindStartsWithInStringList(lstFieldNames, 'EDIT_D', False) # case insensitive compare
    if iRslt >= 0:
        dropField = lstFieldNames[iRslt]
        arcpy.DeleteField_management(in_table=featPath,drop_field=dropField)
    
    # if field starting with 'GLOBALID' (GLOBALID) exists, delete it
    iRslt = PC_Python.FindStartsWithInStringList(lstFieldNames, 'GLOBALID', False) # case insensitive compare
    if iRslt >= 0:
        dropField = lstFieldNames[iRslt]
        arcpy.DeleteField_management(in_table=featPath,drop_field=dropField)
    
    # determine if feature class is point geometry type
    bPt = False
    desc = arcpy.Describe(featPath) # Get the shapefile feature class description
    if (desc.shapeType == 'Point') or (desc.shapeType == 'MultiPoint'):
        bPt = True
    
    # Add and calculate LAT and LON coordinate fields for point geometry type
    if bPt: # check that this is a point feature type
        if ('lat' not in lstFieldNames) and ('lon' not in lstFieldNames): # check that the fields are not already there
            # Add lat/lon fields
            # Prepare for adding XY point fields. Transform state plane to latitude/longitude
            arcpy.env.outputCoordinateSystem = srGeographic # Set output XY coordinate system to WGS84
            arcpy.env.geographicTransformations = "NAD_1983_HARN_To_WGS_1984" # Set geographic transformation
            arcpy.AddXY_management(featPath) # add the new calculated Point_X and Point_Y fields
            arcpy.env.outputCoordinateSystem = None # Clear output XY coordinate system
            arcpy.env.geographicTransformations = None # Clear geographic transformation
            # Recalculate previous lat/lon fields or add new if they don't exist
            fndLAT = False
            fndLON = False
            for fld in desc.fields:
                if fld.name == 'LAT':
                    fndLAT = True
                if fld.name == 'LON':
                    fndLON = True
            if not fndLAT:
                arcpy.AddField_management(in_table=featPath, \
                    field_name="LAT", field_type="DOUBLE", field_precision="", field_scale="", \
                    field_length="", field_alias="", field_is_nullable="NULLABLE", \
                    field_is_required="NON_REQUIRED", field_domain="")
            if not fndLON:
                arcpy.AddField_management(in_table=featPath, \
                        field_name="LON", field_type="DOUBLE", field_precision="", field_scale="", \
                        field_length="", field_alias="", field_is_nullable="NULLABLE", \
                        field_is_required="NON_REQUIRED", field_domain="")
            arcpy.CalculateField_management(in_table=featPath, field="LAT", \
                expression="!POINT_Y!", expression_type="PYTHON", code_block="")
            arcpy.CalculateField_management(in_table=featPath, field="LON", \
                expression="!POINT_X!", expression_type="PYTHON", code_block="")
            # Delete temp Point_X and Point_Y fields
            arcpy.DeleteField_management(in_table=featPath, drop_field="POINT_Y")
            arcpy.DeleteField_management(in_table=featPath, drop_field="POINT_X")


def PrepFeatClassForLibrary(featPath, logger, lstErrCnt):
    # get all subtype codes in order to remove domains from subtypes
    subtypeCodes = []
    subtypes = arcpy.da.ListSubtypes(featPath)
    for stcode, stdict in list(subtypes.items()):
        subtypeCodes.append(stcode) # append subtype codes to a list

    # Remove domains from subtypes and fields
    lstDomains = []
    ndx = featPath.rfind('/')
    featWrkspc = featPath[0:ndx] # get just the workspace part of the featPath
    featName = featPath.split('/')[-2] + '/' + featPath.split('/')[-1] # geodatabase/name for log
    lstFields = arcpy.ListFields(featPath)
    for fld in lstFields:
        if len(fld.domain) > 0:
            if fld.domain not in lstDomains:
                lstDomains.append(fld.domain)
            try:
                arcpy.RemoveDomainFromField_management(featPath,fld.name,subtypeCodes) # remove domain from all subtypes
            except Exception as e:
                logger.warning('\tUnable to remove domain from subtype on field ' + fld.name)
                #lstErrCnt[1] += 1 # Don't count warning so that email notification will ignore
            try:
                arcpy.RemoveDomainFromField_management(featPath,fld.name) # remove domain from field
            except Exception as e:
                logger.warning('\tUnable to remove domain from field ' + fld.name)
                #lstErrCnt[1] += 1 # Don't count warning so that email notification will ignore
    # delete the domains from the workspace
    for dmn in lstDomains:
        try:
            arcpy.DeleteDomain_management(featWrkspc,dmn)
        except Exception as e:
            logger.warning('\tUnable to delete domain ' + dmn + ' on ' + featName)
            #lstErrCnt[1] += 1 # Don't count warning so that email notification will ignore
