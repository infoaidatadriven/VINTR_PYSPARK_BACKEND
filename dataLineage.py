import json
import pandas as pd



def EntryDataLineage(sourcepath,source_ID,source_filename,upload_ID,process_time,module,action,allavailableColumns):

    data = json.load(open("dbDataLineage.json","r"))
    log_old =  data['logs']
    new_log_data = {"sourcepath": sourcepath, "sourceId" : source_ID, "sourceFileName": source_filename,
                     "uploadId": upload_ID, "processTime":process_time,"module": module, 
                     "action": action,"availableColumns":allavailableColumns}
    log_old.append(new_log_data)
    data['logs']= log_old
    json.dump(data, open("dbDataLineage.json","w"), default=str)
    return "true"

def getcatalo():    
    res=dataLineageforcolumn("Age")
    return True

def dataLineageforcolumn(col):
    data = json.load(open("dbDataLineage.json","r"))
    log_old =  data['logs']
    df_result =  [x for x in log_old if col in x['availableColumns']]
    lResultDictList=[]

    for eachresult in df_result:
        lResultDict={}
        lineageName=""
        LineageType=""
        metadata=getfewcatalogueforcolumns(col)
        if 'File Received' in eachresult['action'] :
            lineageName="File Storage"
            LineageType="File"
            lName=eachresult['sourcepath'] 
            lSourceId=eachresult['sourceId']
            lResultDict['LineageName']=lineageName
            lsubresultDict={}
            lsubresultDict['name']=  lName
            lsubresultDict['type']=LineageType
            lsubresultDict['id']=  lSourceId
        
            lsubresultDict['metaData']= metadata
            lResultDict['Details']= lsubresultDict
            lResultDictList.append(lResultDict)
        if 'File storage' in eachresult['action'] :
            lineageName="Extract"
            LineageType="Dataset"
            lName=eachresult['sourcepath']
            lResultDict['LineageName']=lineageName
            lsubresultDict={}
            lsubresultDict['name']=  lName
            lsubresultDict['type']=LineageType
            
            lsubresultDict['metaData']= metadata
            lResultDict['Details']= lsubresultDict
            lResultDictList.append(lResultDict)     
        if 'Cleaning Completed-' in  eachresult['action'] :
            lineageName="Cleaning Process"
            LineageType="Dataset"
            lName=eachresult['sourcepath']
            lSourceId=eachresult['sourceId']
            lAction=eachresult['action']
            lResultDict['LineageName']=lineageName
            lsubresultDict={}
            lsubresultDict['name']=lName
            lsubresultDict['type']=LineageType
            lsubresultDict['id']= lSourceId
            lsubresultDict['action']= lAction

            if col in lAction:
                lsubresultDict['metaData']= metadata
                lResultDict['Details']= lsubresultDict
                lResultDictList.append(lResultDict)
         

        if 'report '   in  eachresult['action'] :
            lineageName="Report"
            LineageType="Report"
            lName=eachresult['sourcepath']
            lSourceId=eachresult['sourceId']
            lResultDict['LineageName']=lineageName
            lsubresultDict={}
            lsubresultDict['name']=  lName
            lsubresultDict['type']=LineageType
            lsubresultDict['id']=  lSourceId

            lsubresultDict['metaData']= metadata
            lResultDict['Details']= lsubresultDict
            lResultDictList.append(lResultDict)
       
        
    return lResultDictList

def getfewcatalogueforcolumns(scolumns):    
        resdata={}
        with open('dCatalogue.json', 'r') as openfile:
                json_object = json.load(openfile)

        data = json_object
        dictionary ={}
        IDList=[]
        populated =  [{'Column':x['Column'],'id':x['id'] ,'BusinessTerm':x['BusinessTerm']  }for x in data['datacatalogues'] if x['Column'] in scolumns]
        return(populated)