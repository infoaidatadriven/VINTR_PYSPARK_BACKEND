import json
from dataLineage import dataLineageforcolumn
def getcataloguewithoutdq():    
    resdata={}
    resdata = json.load(open("dCatalogue.json","r"))    
    jsonString = json.dumps(resdata)
    return jsonString


def getcatalogue():    
        resdata={}
        with open('dCatalogue.json', 'r') as openfile:
                json_object = json.load(openfile)

        data = json_object
        dictionary ={}
        IDList=[]

        with open('qualityResults.json', 'r') as openfile:
                json_object1 = json.load(openfile)

        data1 = json_object1
        qulaitylistt=[]
        qulaityresultList= data1['Results']   
        for everyx in qulaityresultList:
               qulaitylistt=qulaitylistt+(everyx['results']) 
        print(qulaitylistt)       
        list=[]
        for x in data['datacatalogues']:
            listoveralldq=[]
            listoutlier=[]
            if x["Column"]:
                col= x["Column"]
                populated =  [x1 for x1 in qulaitylistt if x1['ColumnName'] ==x["Column"]]
                for eachx in populated:
                    listoveralldq.append(float(eachx['overall']))
                if (len(listoveralldq)>0):
                    overall=sum(listoveralldq)/len(listoveralldq)
                    x['dqaggregate']=overall
                    x['dq']=populated[0]
                    x['outliercount']=0
                x['datalineage']=dataLineageforcolumn(col)
                list.append(x)    
        res={}
        res["datacatalogues"]  =list          
        jsonString = json.dumps(res)
        return jsonString

def getcatalogueMaxId():
        with open('dCatalogue.json', 'r') as openfile:
                json_object = json.load(openfile)

        data = json_object
        dictionary ={}
        IDList=[]
        IDList.append('0')
        for obj in data['datacatalogues']:
                    IDList.append((obj["id"])[1:])
        dictionary["IdsList"] = IDList
        print(str(IDList))
        dli2 = [int(s) for s in IDList]
        return (str(max(dli2)))

def createDatacatalogue(content):
    
    columnName=content['Column']
    BusinessTerm = content['BusinessTerm']
    Definition = content['Definition']
    classification = content['Classification']
    DataDomain = content['DataDomain']
    DataSubDomain = content['DataSubDomain']
    RelatedTerms = content['RelatedTerms']
    RelatedSource = content['RelatedSource']
    RelatedReports = content['RelatedReports']
    DataOwners = content['DataOwners']
    DataUsers = content['DataUsers']
    RelatedTags=content['RelatedTags']
    res={}
    res["Column"]=columnName
    res["Definition"]=Definition
    res["BusinessTerm"]=BusinessTerm
    res["Classification"]=classification
    res["DataDomain"]=DataDomain
    res["DataSubDomain"]=DataSubDomain
    res["RelatedTerms"]=RelatedTerms
    res["RelatedSource"]=RelatedSource
    res["RelatedReports"]=RelatedReports
    res["DataOwners"]=DataOwners
    res["DataUsers"]=DataUsers
    res['RelatedTags']=RelatedTags
    newcatalogueId='c'+ str(int(getcatalogueMaxId())+1)
    res['id']=newcatalogueId
    with open('dCatalogue.json', 'r') as openfile:
            json_object = json.load(openfile)
    deptList={}
    data=json_object
    # for obj in data['datacatalogues']:
    #     if (obj["label"]==sourceName ):
    #         deptList=obj
    #         break
    rdata={}

    if 1==1:
        data['datacatalogues'].append(res)
        json.dump(data, open("dCatalogue.json","w"))
        rdata['responseMsg']= 'dCatalogue added successfully'
        rdata['errorflag']= 'False'
        rdata['res']=res
    else:
        rdata['errorMsg']= 'dCatalogue already Exist'
        rdata['errorflag']= 'True'
    jsonString = json.dumps(rdata)
    return jsonString


def editDatacatalogue(content):    
    columnName=content['Column']
    
    id=content['id']
    BusinessTerm = content['BusinessTerm']
    Definition = content['Definition']
    classification = content['Classification']
    DataDomain = content['DataDomain']
    DataSubDomain = content["DataSubDomain"]
    RelatedTerms = content['RelatedTerms']
    RelatedSource = content['RelatedSource']
    RelatedReports = content['RelatedReports']
    DataOwners = content['DataOwners']
    DataUsers = content['DataUsers']
    RelatedTags=content['RelatedTags']
    res={}
    res["Column"]=columnName
    res["Definition"]=Definition
    res["BusinessTerm"]=BusinessTerm
    res["Classification"]=classification
    res["DataDomain"]=DataDomain
    res["DataSubDomain"]=DataSubDomain
    res["RelatedTerms"]=RelatedTerms
    res["RelatedSource"]=RelatedSource
    res["RelatedReports"]=RelatedReports
    res["DataOwners"]=DataOwners
    res["DataUsers"]=DataUsers
    res['RelatedTags']=RelatedTags
    res['id']=id
    with open('dCatalogue.json', 'r') as openfile:
            json_object = json.load(openfile)
    eList={}
    totaleObj=[]
    data=json_object
    for obj in data['datacatalogues']:
        if (obj["id"]==id):
            eList=obj
        else:
            totaleObj.append(obj)
    rdata={}

    if not eList:

        rdata['responseMsg']= 'datacatalogue not exist'
        rdata['errorflag']= 'True'
    else:
        rdata['errorMsg']= 'datacatalogue updated successfully'
        rdata['errorflag']= 'False'
        rdata['res']=res
        totaleObj.append(res)
        data['datacatalogues']=totaleObj
        json.dump(data, open("dCatalogue.json","w"))
    jsonString = json.dumps(rdata)
    return jsonString

def getcatalogueforcolumns(scolumns):    
    resdata={}
    with open('dCatalogue.json', 'r') as openfile:
        json_object = json.load(openfile)

    data = json_object
        # dictionary ={}
        # IDList=[]
        # populated =  [x for x in data['datacatalogues'] if x['Column'] in scolumns]
        # print(populated)
        # return(populated)
    populated = []
    for item in data['datacatalogues']:
        sel_col_low=[x. lower() for x in scolumns]
        if item['Column'].lower() in sel_col_low:
            confidence_level = 100.0 if item['Column'].lower() == sel_col_low else 80.0
            item['Confidence Level'] = confidence_level
        populated.append(item)
    return(populated)

        