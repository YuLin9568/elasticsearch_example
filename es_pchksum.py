import elasticsearch
from elasticsearch_dsl import MultiSearch, Search, Q
from datetime import datetime
es = elasticsearch.Elasticsearch("http://for_query:for_query@172.23.134.168:9200/", timeout=500, max_retries=2, retry_on_timeout=True)
def multiple_layer_fields(multiple_dict):
      dict_return = {}
      for i in multiple_dict:
          if type(multiple_dict[i]) == dict:
              v = multiple_layer_fields(multiple_dict[i])
              for j in v :
                  dict_return[i+"."+j] = v[j]
          else :
              dict_return[i] = multiple_dict[i]
      return (dict_return)

def filter_key_input(field):
  
      if not field : return []
      q = []
      for filter in field:
          q.append(Q("exists", **{"field":str(filter)}))
  
      return q


def get_pchksum_by_fileidlist(file_id_list):
    file_id_list = list(map(str, file_id_list))
    DB_data = Search(using=es, index="tw.database") \
             .source(includes=["File_ID", "pchksum"]) \
             .query("ids", values=file_id_list) \
             .extra(size = 500000) \
             .params(request_timeout = 300) 
#    DB_data = es.search(           
#                index="tw.database", 
#                size="500000",      
#                _source = ["File_ID", "pchksum"],
#                request_timeout = 300, 
#                body={"query" : {"ids": {"values": file_id_list, }}}                                
#        )                                  
    response = DB_data.execute()                                    
    list_all = []
    for x in response.hits.hits:                 
        dict_output = {}
        for y in x._source:               
            if type(x._source.to_dict()[y]) == dict:  
                v = multiple_layer_fields(x._source.to_dict()[y])   
                for i in v:                                 
                    dict_output[y+"."+i] = v[i]            
            else :                                        
                dict_output[y] = x._source.to_dict()[y]
        list_all.append(dict_output)                    
    return list_all


def get_pchksum(File_ID):
    DB_data = Search(using=es, index="tw.database") \
             .source(includes=["pchksum"]) \
             .query("match", File_ID=File_ID) \
             .extra(size = 500000) \
             .params(request_timeout = 60) 

    response = DB_data.execute()                                    
    for x in response.hits.hits:                 
        for y in x._source:
            return x._source[y]            

def get_identical_fileids(fileid):

    pchksum = get_pchksum(str(fileid))  
    DB_data = Search(using=es, index="tw.database") \
             .source(includes=["File_ID"]) \
             .query("match", pchksum=pchksum) \
             .extra(size = 500000) \
             .params(request_timeout=60) 

    docs = []
    response = DB_data.execute()                                    
    for x in response.hits.hits:                 
        for y in x._source:
            docs.append(x._source[y])   
    return docs

##Get the fileids and must vale with same pchksum
def get_identical_must(fileid, must_exists_value):

    pchksum = get_pchksum(str(fileid))  
    DB_data = Search(using=es, index="tw.database") \
             .source(includes=["File_ID"] + must_exists_value) \
             .query("bool", must=filter_key_input(must_exists_value)) \
             .query("match", pchksum=pchksum) \
             .extra(size = 500000) \
             .params(request_timeout=60) 
    response = DB_data.execute()                                    
    list_all = []
    for x in response.hits.hits:                 
        dict_output = {}
        for y in x._source:               
            if type(x._source.to_dict()[y]) == dict:  
                v = multiple_layer_fields(x._source.to_dict()[y])   
                for i in v:                                 
                    dict_output[y+"."+i] = v[i]            
            else :                                        
                dict_output[y] = x._source.to_dict()[y]
        list_all.append(dict_output)                    
    return list_all

def multisearch_identical_fileids(list_pchksum, day_start, day_end):
    msearch = MultiSearch(using=es)

    for pchksum in list_pchksum:
        msearch = msearch.add(
                Search()
                .filter("match", pchksum=pchksum)
                .filter("exists", field="File_ID")
                .filter("range" ,  **{"@timestamp": {"gte": day_start , "lt": day_end, "time_zone": "Asia/Taipei"}})
                .extra(size=50000)
                )
    responses = msearch.execute()
    lookup_fileid = {i:[] for i in list_pchksum}
    for response in responses:
        if response.success() and response.hits:
            for hit in response.hits:
                lookup_fileid[hit.pchksum].append(hit.meta.id)
                #lookup_fileid[hit.pchksum].append(hit.File_ID)

    return lookup_fileid

##Prevent the File_ID with other type (extract File_ID from _id to keep str type)
def multisearch_identical_fileids_v2(list_pchksum, day_start, day_end):
    msearch = MultiSearch(using=es)

    for pchksum in list_pchksum:
        msearch = msearch.add(
                Search()
                .filter("match", pchksum=pchksum)
                .filter("exists", field="File_ID")
                .filter("range" ,  **{"@timestamp": {"gte": day_start , "lt": day_end, "time_zone": "Asia/Taipei"}})
                .extra(size=50000)
                )
    #print(msearch)
    responses = msearch.execute()
    time1 = datetime.now()
    lookup_fileid = {i:[] for i in list_pchksum}
    for response in responses:
        if response.success() and response.hits:
            #print(response)
            for hit in response.hits:
                #print(hit.meta)
                lookup_fileid[hit.pchksum].append(hit.meta.id)
                lookup_fileid[hit.pchksum].append(hit.File_ID)

    return lookup_fileid

if __name__ == '__main__':
    print(get_identical_must('1430506555', ['pchksum', 'File_Type']))
    #day_start = "2022-03-28T00:00:00" 
    #day_end = "2022-03-30T09:40:00" 
    #list_pchksum = ["883fee26", "3decf1ec"]
    #dict_fileid = multisearch_identical_fileids_v2(list_pchksum, day_start, day_end)
    #print(dict_fileid)
