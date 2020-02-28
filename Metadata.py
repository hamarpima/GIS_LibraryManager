# Metadata.py - Functions to read and update metadata
#
# Revision History:
# Date          Developer           Description
# 4/20/2017     Harlan Marshall     Added parameter to process only specific feature classes
# 1/9/2018      Harlan Marshall     Alter metadata query to allow for final '\' in path. Add MAKELATLON in query return.
# 5/2/2018      Harlan Marshall     Added sensitive subgroup privilege handling from new metadata field SENSUBGRP
# 8/7/2018      Harlan Marshall     For gdbtoshape, changed to get source data location from STDEXPSOURCE in metadata (server.schema.db)
#                                   rather than multiple config sections
######################################################################################################################

from pyodbc import connect as odbcconn
from pandas import read_sql_query as readsqlqry # pandas 0.19.x
import LibMgrUtility
import PC_Python
import PC_Geoprocessing
import xml.etree.ElementTree as ET
import os
import arcpy
import ConfigParser
import datetime
import base64

# custom error exception for all functions in this module
class MetadataError(Exception):
    pass

# custom warning exception for all functions in this module
class MetadataWarning(Exception):
    pass

# Read metadata into pandas DataFrame object. Returns DataFrame object
def LoadMetadata(metaConnStr, sqlLstFC, sqlLstFX):
    conn = odbcconn(metaConnStr)
    metaqry = 'SELECT COVER_NAME AS \'ndx\', COVER_NAME AS feature' + \
        ', [PATH] AS updatepath, SUBSTRING([PATH], LEN([PATH]) - 6, 6) AS subpath' +\
        ', REPLACE(REPLACE([PATH],\'mars1\',\'libstage\mars1\'),\'sdcp\',\'libstage\\sdcp\') AS stagepath' +\
        ', CONVERT(DATETIME, CONVERT(CHAR(19), DATETOUCHED, 120)) AS DATETOUCHED' +\
        ', CONVERT(DATETIME, CONVERT(CHAR(19), SHPDATE, 120)) AS SHPDATE' +\
        ', CONVERT(DATETIME, CONVERT(CHAR(19), COVDATE, 120)) AS COVDATE' +\
        ', CONVERT(DATETIME, CONVERT(CHAR(19), GDBDATE, 120)) AS GDBDATE' +\
        ', SHAPEFILESIZE, SENSITIVE, LIBINPUT, FEAT_TYPE, MAKELATLON' +\
        ', SenSubGrp , STDEXPSOURCE ' +\
	    'FROM metadata ' +\
	    'WHERE LIBINPUT IN (\'Shape Std\',\'GDB Std\',\'GDB Spec\',\'Cover Spec\') ' +\
	    'AND [INACTIVE] = 0'

    if sqlLstFC != '':
        metaqry = metaqry + ' AND COVER_NAME IN ' + sqlLstFC

    if sqlLstFX != '':
        metaqry = metaqry + ' AND COVER_NAME NOT IN ' + sqlLstFX

    metaqry = metaqry + ' ORDER BY COVER_NAME' # set sort order last

    df = None
    df = readsqlqry(metaqry, conn, index_col = 'ndx') # pandas 0.19.1 load query result to pandas dataframe

    return df


# Update single feature class file size and last modified dates in the metadata
def UpdateLibraryMetadata(dfFeat, connStr, logger):
    funcName = 'UpdateLibraryMetadata'
    featName = '\'' + dfFeat['feature'].lower() + '\''

    # build sql update statement
    strSql = 'UPDATE [dbo].[metadata] SET '
    strDt = str(dfFeat['DATETOUCHED'])
    if PC_Python.isdate(strDt):     # if this is a valid date, update the field. Otherwise, don't update.
        strSql = strSql + '[DATETOUCHED] = \'' + strDt + '\','
    strDt = str(dfFeat['SHPDATE'])
    if PC_Python.isdate(strDt):     # if this is a valid date, update the field. Otherwise, don't update.
        strSql = strSql + '[SHPDATE] = \'' + strDt + '\','
    strDt = str(dfFeat['COVDATE'])
    if PC_Python.isdate(strDt):     # if this is a valid date, update the field. Otherwise, don't update.
        strSql = strSql + '[COVDATE] = \'' + strDt + '\','
    strDt = str(dfFeat['GDBDATE'])
    if PC_Python.isdate(strDt):     # if this is a valid date, update the field. Otherwise, don't update.
        strSql = strSql + '[GDBDATE] = \'' + strDt + '\','
    shpSize = str(dfFeat['SHAPEFILESIZE'])
    strSql = strSql + '[SHAPEFILESIZE] = ' + shpSize + ' WHERE [COVER_NAME] = ' + featName

    conn = odbcconn(connStr)
    curs = conn.cursor()
    curs.execute(strSql)
    curs.commit()
    curs.close()
    conn.close()


# build a concatenated string of multiple metadata values
def BuildMetadataSupString(df_row):
    supField = ''
    if df_row['KNOWNERROR'] and len(df_row['KNOWNERROR']) > 0:
        supField = supField + '\n' + 'Known Errors/Qualifications: ' + df_row['KNOWNERROR']
    if df_row['LINEAGE'] and len(df_row['LINEAGE']) > 0:
        supField = supField + '\n' + 'Lineage: ' + df_row['LINEAGE']
    if df_row['DOMAIN'] and len(df_row['DOMAIN']) > 0:
        supField = supField + '\n' + 'Spatial Domain: ' + df_row['DOMAIN']
    if df_row['RECTIFIED'] and len(df_row['RECTIFIED']) > 0 and df_row['RECTIFIED'].upper() != 'UNKNOWN':
        supField = supField + '\n' + 'Rectified: ' + df_row['RECTIFIED']
    if df_row['MAINTORG'] and len(df_row['MAINTORG']) > 0:
        supField = supField + '\n' + 'Maintenance Organization: ' + df_row['MAINTORG']
    if df_row['MAINTDESC'] and len(df_row['MAINTDESC']) > 0:
        supField = supField + '\n' + 'Maintenance Description: ' + df_row['MAINTDESC']
    if df_row['MAINTFREQ'] and len(df_row['MAINTFREQ']) > 0 and df_row['MAINTFREQ'].upper() != 'N/A':
        supField = supField + '\n' + 'Maintenance Frequency: ' + df_row['MAINTFREQ']
    if df_row['LIBINPUT'] and len(df_row['LIBINPUT']) > 0:
        supField = supField + '\n' + 'Library Input: ' + df_row['LIBINPUT']
    if df_row['SOURCNAME'] and len(df_row['SOURCNAME']) > 0:
        supField = supField + '\n' + 'Primary Source Organization: ' + df_row['SOURCNAME']
    if df_row['SOURCCONTACT'] and len(df_row['SOURCCONTACT']) > 0:
        supField = supField + '\n' + 'Primary Source Contact: ' + df_row['SOURCCONTACT']
    if df_row['SOURCDOCNAME'] and len(df_row['SOURCDOCNAME']) > 0:
        supField = supField + '\n' + 'Primary Source Document: ' + df_row['SOURCDOCNAME']
    if df_row['SOURCDATE'] and len(df_row['SOURCDATE']) > 0:
        supField = supField + '\n' + 'Primary Source Date: ' + df_row['SOURCDATE']
    if df_row['SOURCSCALE'] and df_row['SOURCSCALE'] > 0:
        supField = supField + '\n' + 'Primary Source Scale: ' + str(df_row['SOURCSCALE'])
    if df_row['SOURCFORMAT'] and len(df_row['SOURCFORMAT']) > 0 and df_row['SOURCFORMAT'].upper() != 'UNKNOWN':
        supField = supField + '\n' + 'Primary Source Format: ' + df_row['SOURCFORMAT']
    if df_row['SOUR2NAME'] and len(df_row['SOUR2NAME']) > 0:
        supField = supField + '\n' + 'Secondary Source Organization: ' + df_row['SOUR2NAME']
    if df_row['SOUR2CONTACT'] and len(df_row['SOUR2CONTACT']) > 0:
        supField = supField + '\n' + 'Secondary Source Contact: ' + df_row['SOUR2CONTACT']
    if df_row['SOUR2DOCNAME'] and len(df_row['SOUR2DOCNAME']) > 0:
        supField = supField + '\n' + 'Secondary Source Document: ' + df_row['SOUR2DOCNAME']
    if df_row['SOUR2DATE'] and len(df_row['SOUR2DATE']) > 0:
        supField = supField + '\n' + 'Secondary Source Date: ' + df_row['SOUR2DATE']
    if df_row['SOUR2SCALE'] > 0:
        supField = supField + '\n' + 'Secondary Source Scale: ' + str(df_row['SOUR2SCALE'])
    if df_row['SOUR2FORMAT'] and len(df_row['SOUR2FORMAT']) > 0 and df_row['SOUR2FORMAT'].upper() != 'UNKNOWN':
        supField = supField + '\n' + 'Secondary Source Format: ' + df_row['SOUR2FORMAT']
    if df_row['OWNERNAME'] and len(df_row['OWNERNAME']) > 0:
        supField = supField + '\n' + 'GIS Contact: ' + df_row['OWNERNAME']
    if df_row['ONMG'] and df_row['ONMG'].upper() == 'YES':
        supField = supField + '\n\nMapGuide Layer Name: ' + df_row['MGLAYERNAME'] +\
            '\nMapGuide Scale Range: ' + str(df_row['MGSCALELOW']) +\
            ' - ' + str(df_row['MGSCALEHIGH'])# + '\n'

    return supField


# Update feature class metadata
def UpdateFCMetadata(fcPathName, metaConnStr):
    # Set connection strings and get other settings from configuration file
    parser = ConfigParser.SafeConfigParser()
    parser.read('LibMgr.ini')
    disclaimerFile = parser.get('Metadata', 'disclaimerFile')
    idCredit = parser.get('Metadata', 'idCredit')
    constraint_useLimit = parser.get('Metadata', 'constraint_useLimit')
    organization = parser.get('Metadata', 'organization')
    timeperd_current = parser.get('Metadata', 'timeperd_current')
    addrtype = parser.get('Metadata', 'addrtype')
    address = parser.get('Metadata', 'address')
    city = parser.get('Metadata', 'city')
    state = parser.get('Metadata', 'state')
    zip = parser.get('Metadata', 'zip')
    country = parser.get('Metadata', 'country')
    phone = parser.get('Metadata', 'phone')
    librarian = parser.get('Metadata', 'librarian')
    thumbnailsPath = parser.get('Metadata', 'thumbnailsPath')

    num_elements = 0
    featName = fcPathName.split('.')[-1]
    conn = odbcconn(metaConnStr)
    metaqry = 'SELECT [FULL_NAME],[COVER_NAME],[ABSTRACT],[UPDATEDATE],[OWNERNAME]' +\
                    ',[PATH],[METAACCESS],[ONMAINT],[MAINTFREQ],[KNOWNERROR],[LINEAGE]' +\
                    ',[DOMAIN],[RECTIFIED],[MAINTORG],[MAINTDESC],[LIBINPUT],[SOURCNAME]' +\
                    ',[SOURCCONTACT],[SOURCDOCNAME],[SOURCDATE],[SOURCSCALE],[SOURCFORMAT]' +\
                    ',[SOUR2NAME],[SOUR2CONTACT],[SOUR2DOCNAME],[SOUR2DATE],[SOUR2SCALE]' +\
                    ',[SOUR2FORMAT],[ONMG],[MGLAYERNAME],[MGSCALELOW],[MGSCALEHIGH] ' +\
                    'FROM [dbo].[metadata] WHERE [COVER_NAME] = \'' + featName + '\''
    df_FCMeta = readsqlqry(metaqry, conn) # pandas 0.19.1 load query result to pandas dataframe
    df_row = df_FCMeta.iloc[0]

    qry = 'SELECT [FieldName] AS \'ndx\',[FieldName],[Description] FROM [dbo].[master_metafield] WHERE [CoverName] = \'' + featName + '\''
    df_fieldMeta = readsqlqry(qry, conn, index_col = 'ndx') # pandas 0.19.1 load query result to pandas dataframe

    arcpy.env.overwriteOutput = True

    #    install location
    dir = arcpy.GetInstallInfo('desktop')['InstallDir'] 

    #    stylesheet to use
    copy_xslt = r'{0}'.format(os.path.join(dir,'Metadata\Stylesheets\gpTools\exact copy of.xslt'))

    #    temporary XML file
    xmlfile = arcpy.CreateScratchName('.xml',workspace=arcpy.env.scratchFolder)
    
    # export xml
    arcpy.XSLTransform_conversion(fcPathName, copy_xslt, xmlfile, '')
    
    # read in XML
    tree = ET.parse(xmlfile)
    root = tree.getroot()

    # build the supplemental info string
    sSuppInfo = BuildMetadataSupString(df_row)

    # get the dataIdInfo element
    dataIdInfoEl = root.find('dataIdInfo')

    # dataIdInfo purpose element
    subEl = ET.SubElement(dataIdInfoEl,'idPurp')
    subEl.text = df_row['FULL_NAME']
    num_elements += 1

    # dataIdInfo abstract element
    subEl = ET.SubElement(dataIdInfoEl,'idAbs')
    subEl.text = df_row['ABSTRACT'] + sSuppInfo
    num_elements += 1

    # dataIdInfo access constraint element
    subEl = ET.SubElement(dataIdInfoEl,'accconst')
    subEl.text = df_row['METAACCESS']
    num_elements += 1

    # dataIdInfo credit element
    subEl = ET.SubElement(dataIdInfoEl,'idCredit')
    subEl.text = idCredit
    num_elements += 1

    # dataIdInfo maintenance frequency element
    subEl = ET.SubElement(dataIdInfoEl,'resMaint')
    subEl = ET.SubElement(subEl,'usrDefFreq')
    subEl = ET.SubElement(subEl,'duration')
    subEl.text = df_row['MAINTFREQ']
    num_elements += 1

    # dataIdInfo use limit element
    subEl = ET.SubElement(dataIdInfoEl,'resConst')
    subEl = ET.SubElement(subEl,'Consts')
    subEl = ET.SubElement(subEl,'useLimit')
    subEl.text = constraint_useLimit
    num_elements += 1

    # dataIdInfo keyword elements obtained from FULL_NAME
    searchKeysEl = ET.SubElement(dataIdInfoEl,'searchKeys')
    keywords = df_row['FULL_NAME'].split(' ')
    for keyword in keywords:
        newKeyEl = ET.SubElement(searchKeysEl,'keyword')
        newKeyEl.text = keyword
        num_elements += 1

    # create the idInfo element
    idInfoEl = ET.SubElement(root,'idInfo')

    # idinfo use constraint element
    with open(disclaimerFile, 'r') as file: # read the disclaimer text file to a string
        disclaimer = file.read()
    subEl = ET.SubElement(idInfoEl,'useconst')
    subEl.text = disclaimer
    num_elements += 1

    # idinfo citation onlink element
    # remove the server name portion of the path
    path = df_row['PATH'].split('\\')[2]
    pathRoot = '\\\\' + path + '\\'
    onlink = df_row['PATH'].replace(pathRoot,'')

    subEl = ET.SubElement(idInfoEl,'citation')
    citeinfoEl = ET.SubElement(subEl,'citeinfo')
    subEl = ET.SubElement(subEl,'onlink')
    subEl.text = onlink
    num_elements += 1

    # idinfo citation origin element
    subEl = ET.SubElement(citeinfoEl,'origin')
    subEl.text = organization
    num_elements += 1

    # idinfo citation pubdate element
    subEl = ET.SubElement(citeinfoEl,'pubdate')
    subEl.text = datetime.datetime.now().strftime("%B %d, %Y")
    num_elements += 1

    # create the idInfo timeperd element
    timeperdEl = ET.SubElement(idInfoEl,'timeperd')

    # idinfo timeperd update date comment element
    subEl = ET.SubElement(timeperdEl,'current')
    subEl.text = timeperd_current
    num_elements += 1

    # idinfo timeperd update date element
    subEl = ET.SubElement(timeperdEl,'timeinfo')
    subEl = ET.SubElement(subEl,'sngdate')
    subEl = ET.SubElement(subEl,'caldate')
    subEl.text = df_row['UPDATEDATE']
    num_elements += 1

    # create the idInfo descript element
    descriptEl = ET.SubElement(idInfoEl,'descript')

    # idinfo descript abstract element
    subEl = ET.SubElement(descriptEl,'abstract')
    subEl.text = df_row['ABSTRACT']
    num_elements += 1

    # idinfo descript purpose element
    subEl = ET.SubElement(descriptEl,'purpose')
    subEl.text = df_row['FULL_NAME']
    num_elements += 1

    # idinfo descript supplinf element
    subEl = ET.SubElement(descriptEl,'supplinf')
    subEl.text = sSuppInfo
    num_elements += 1

    # idinfo keywords themekey element
    subEl = ET.SubElement(idInfoEl,'keywords')
    subEl = ET.SubElement(subEl,'theme')
    subEl = ET.SubElement(subEl,'themekey')
    subEl.text = df_row['FULL_NAME']
    num_elements += 1

    # create the idInfo point of contact elements
    subEl = ET.SubElement(idInfoEl,'ptcontac')
    cntinfoEl = ET.SubElement(subEl,'cntinfo')
    cntperpEl = ET.SubElement(cntinfoEl,'cntperp')
    cntaddrEl = ET.SubElement(cntinfoEl,'cntaddr')
    cntvoiceEl = ET.SubElement(cntinfoEl,'cntvoice')

    # idinfo point of contact person element
    subEl = ET.SubElement(cntperpEl,'cntper')
    subEl.text = df_row['OWNERNAME']
    num_elements += 1

    # idinfo point of contact organization element
    subEl = ET.SubElement(cntperpEl,'cntorg')
    subEl.text = organization
    num_elements += 1

    # idinfo point of contact address type element
    subEl = ET.SubElement(cntaddrEl,'addrtype')
    subEl.text = addrtype
    num_elements += 1

    # idinfo point of contact address element
    subEl = ET.SubElement(cntaddrEl,'address')
    subEl.text = address
    num_elements += 1

    # idinfo point of contact city element
    subEl = ET.SubElement(cntaddrEl,'city')
    subEl.text = city
    num_elements += 1

    # idinfo point of contact state element
    subEl = ET.SubElement(cntaddrEl,'state')
    subEl.text = state
    num_elements += 1

    # idinfo point of contact zip element
    subEl = ET.SubElement(cntaddrEl,'postal')
    subEl.text = zip
    num_elements += 1

    # idinfo point of contact country element
    subEl = ET.SubElement(cntaddrEl,'country')
    subEl.text = country
    num_elements += 1

    # idinfo point of contact phone element
    subEl = ET.SubElement(cntinfoEl,'cntvoice')
    subEl.text = phone
    num_elements += 1

    # create the metainfo point of contact elements
    metainfoEl = ET.SubElement(root,'metainfo')
    subEl = ET.SubElement(metainfoEl,'metc')
    cntinfoEl = ET.SubElement(subEl,'cntinfo')
    cntorgpEl = ET.SubElement(cntinfoEl,'cntorgp')
    cntaddrEl = ET.SubElement(subEl,'cntaddr')

    # metainfo point of contact person element
    subEl = ET.SubElement(cntorgpEl,'cntper')
    subEl.text = librarian
    num_elements += 1

    # metainfo point of contact organization element
    subEl = ET.SubElement(cntorgpEl,'cntorg')
    subEl.text = df_row['OWNERNAME'] + '\n' + organization
    num_elements += 1

    # metainfo point of contact address type element
    subEl = ET.SubElement(cntaddrEl,'addrtype')
    subEl.text = addrtype
    num_elements += 1

    # metainfo point of contact address element
    subEl = ET.SubElement(cntaddrEl,'address')
    subEl.text = address
    num_elements += 1

    # metainfo point of contact city element
    subEl = ET.SubElement(cntaddrEl,'city')
    subEl.text = city
    num_elements += 1

    # metainfo point of contact state element
    subEl = ET.SubElement(cntaddrEl,'state')
    subEl.text = state
    num_elements += 1

    # metainfo point of contact zip element
    subEl = ET.SubElement(cntaddrEl,'postal')
    subEl.text = zip
    num_elements += 1

    # metainfo point of contact country element
    subEl = ET.SubElement(cntaddrEl,'country')
    subEl.text = country
    num_elements += 1

    # metainfo point of contact phone element
    subEl = ET.SubElement(cntinfoEl,'cntvoice')
    subEl.text = phone
    num_elements += 1

    # idinfo maintenance status element
    statusEl = ET.SubElement(idInfoEl,'status')
    subEl = ET.SubElement(statusEl,'progress')
    if df_row['ONMAINT'] and df_row['ONMAINT'].upper() == 'Y':
        subEl.text = 'Maintained'
        num_elements += 1
    else:
        subEl.text = 'Not Maintained'
        num_elements += 1

    # idinfo maintenance frequency element
    subEl = ET.SubElement(statusEl,'update')
    subEl.text = df_row['MAINTFREQ']
    num_elements += 1

    # add descriptions from library metadata table master_metafields to the feature class fields
    attrEls = root.findall('eainfo/detailed/attr')
    for attrEl in attrEls: # iterate feature class fields
        lablEl = attrEl.find('attrlabl') # find the attribute name element
        if lablEl is not None: # for unknown reason, the root.findall sometimes gets attributes that are empty
            fldname = lablEl.text
            try:
                descrip = df_fieldMeta.loc[fldname]['Description'] # get the field description from the dataframe
            except Exception as e:
                #print('\tNo description for field ' + fldname)
                pass # ignore error returned by dataframe loc. Field not in field metadata table.
            else:
                subEl = ET.SubElement(attrEl,'attrdef') #field description element
                subEl.text = descrip
                num_elements += 1
                subEl = ET.SubElement(attrEl,'attrdefs') #field description source element
                subEl.text = 'Pima County'
                num_elements += 1

    # set metadata thumbnail
    jpgFile = thumbnailsPath + '/' + featName + '.jpg'
    if os.path.exists(jpgFile):
        with open(jpgFile, "rb") as img_file:
            strEncoded = base64.b64encode(img_file.read())

        attrib = {'EsriPropertyType':'PictureX'}
        subEl = ET.SubElement(root,'Binary')
        subEl = ET.SubElement(subEl,'Thumbnail')
        subEl = ET.SubElement(subEl,'Data',attrib)
        subEl.text = strEncoded
        num_elements += 1

    if num_elements > 0:
        # save modifications to XML
        try:
            tree.write(xmlfile)
            arcpy.MetadataImporter_conversion(xmlfile, fcPathName)
        except Exception as e:
            print(e.message)
    else:
        print('No changes to save')


## Refresh all metadata dates and file sizes
#def RefreshMetadata(df, metaConnStr, gdbConnStr, libGDBSchema, logger):
#    funcName = 'RefreshMetadata'
#    i = 0
#    while (i < len(df)):
#        row = df.iloc[i]
#        maintFmt = row['LIBINPUT'].lower()
#        srcPath = row['updatepath'].lower() # get source path from metadata
#        featName = row['feature'].lower() # get feature name
#        srcFeatPath = srcPath + featName # set source file path for the feature class
#        shpPath = srcPath.replace('\\covers\\', '\\shapes\\') # if the path contains '\covers\', convert it to it's respective shapes path
#        metaSDt = str(row['SHPDATE'])
#        metaCDt = str(row['COVDATE'])
#        metaGDt = str(row['GDBDATE'])
#        metaTDt = str(row['DATETOUCHED'])
#        metaSSize = row['SHAPEFILESIZE']

#        cDt = '1900-01-01 00:00:00' # initialize cover date to be used for all but coverages. Coverage date 1900-01-01 indicates no coverage format

#        # the following dates and size are needed for all maintformats
#        sDt = str(PC_Geoprocessing.GetShapefileLastModDate(shpPath, featName, logger, lstErrCnt))
#        gDt = str(PC_Geoprocessing.GetNonVersionedGDBCreateDate(featName, gdbConnStr, libGDBSchema, logger, lstErrCnt))
#        sSize = PC_Geoprocessing.GetShapefileSize(shpPath, featName, logger, lstErrCnt)
#        tDt = sDt # assume touch date is shape date unless libInput turns out to be 'coverage'

#        # if libInput is 'coverage' then we need the COVDATE and conversion of source path for other dates
#        if maintFmt == 'coverage':
#            cDt = str(PC_Geoprocessing.GetCoverageLastModDate(srcFeatPath, logger, lstErrCnt))
#            tDt = cDt
#            sSize = PC_Geoprocessing.GetShapefileSize(shpPath, featName, logger, lstErrCnt)

#        # Check each date to see if it needs to be updated in metadata. Set bUpdate True if update needed.
#        bUpdate = False
#        if metaSDt <> sDt:
#            row['SHPDATE'] = sDt
#            logger.info('\tSHPDATE Mismatch: Metadata: ' + str(metaSDt) + '\tActual: ' + str(sDt))
#            bUpdate = True
#        if metaCDt <> cDt:
#            row['COVDATE'] = cDt
#            logger.info('\tCOVDATE Mismatch: Metadata: ' + str(metaCDt) + '\tActual: ' + str(cDt))
#            bUpdate = True
#        if metaGDt <> gDt:
#            row['GDBDATE'] = gDt
#            logger.info('\tGDBDATE Mismatch: Metadata: ' + str(metaGDt) + '\tActual: ' + str(gDt))
#            bUpdate = True
#        if metaTDt <> tDt:
#            row['DATETOUCHED'] = tDt
#            logger.info('\tDATETOUCHED Mismatch: Metadata: ' + str(metaTDt) + '\tActual: ' + str(tDt))
#            bUpdate = True
#        if metaSSize <> sSize:
#            row['SHAPEFILESIZE'] = sSize
#            logger.info('\tSHAPEFILESIZE Mismatch: Metadata: ' + str(metaSSize) + '\tActual: ' + str(sSize))
#            bUpdate = True

#        # if an update is required, update all for this feature class
#        if bUpdate:
#            try:
#                UpdateLibraryMetadata(row, metaConnStr, logger, lstErrCnt)
#            except Exception as e:
#                logger.error('\tERROR - Refreshing metadata for ' + row['feature'].lower(), exc_info=True)
#        else:
#            logger.info('\tNo update required for ' + row['feature'].lower())

#        i = i + 1
