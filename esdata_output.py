'''
# Created by Pierre Lee
# Required --- Installation : pip install elasticsearch, pytz, python-dateutil
#          --- Customize your specific parameters
#
# ---------------CSV setting-------------------
# csv_columns --- keys you want to list in csv columns 
#                 e.g. => ["column_1", "column_2", ....] 
# csv_path    --- e.g. => "./db_output.csv"
# ----------------- Filter --------------------
# Must_exists_value --- Returns data only if the input key contain a value, [string1, string2, ....]
# 
# Specific_value    --- if need to query specific value, [dict1, dict2, ...]
#                   e.g. [{"column_1": "value_1"}, {"column_2" : "value_2"}]
#
# NOT_exists_value  --- Returns data only if the input key "NOT" contain a value, [string1, string2, ....]
#
# NOT_Specific_value--- if need to query "WITHOUT" the specific value, [dict1, dict2, ...]
#                   e.g. [{"column_1": "value_1"}, {"column_2" : "value_2"}]
#
# p.s. If no need to set filter, empty the list, e.g. filter_AND = []
Time format:year-month-dayTHour:Minute:Second, EX: "2022-03-30T09:40:00"
'''
import elasticsearch
from elasticsearch_dsl import Search
from elasticsearch_dsl import Q
import csv
from datetime import datetime

es = elasticsearch.Elasticsearch("http://for_query:for_query@172.23.134.168:9200/")

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

def filter_dict_input(key_value):

    if not key_value : return []
    q = []
    for filter in key_value:
        for k, v in filter.items():
            q.append(Q("match", **{str(k):str(v)}))
    return  q

def filter_key_input(field):

    if not field : return []
    q = []
    for filter in field:
        q.append(Q("exists", **{"field":str(filter)}))

    return q

def data_output():

    DB_data = Search(using=es, index="tw.database") \
              .source(includes= csv_columns) \
              .query("bool", must=filter_key_input(Must_exists_value)) \
              .query('bool', must=filter_dict_input(Specific_value)) \
              .query('bool', should=filter_dict_input(filter_OR)) \
              .query('bool', must_not=filter_key_input(NOT_exists_value)) \
              .query('bool', must_not=filter_dict_input(NOT_Specific_value)) \
              .filter("range", **{"@timestamp": {"gte": start_date, "lt": end_date, "time_zone": "Asia/Taipei"}}) \
              .extra(size = 500000) \
              .params(request_timeout = 60)
    response = DB_data.execute()

    docs = []
    for x in response.hits.hits:
        for y in x._source:
            docs.append(x._source[y])
    print(docs)
    return docs

def data_output_allarg(csv_columns, Must_exists_value, Specific_value, filter_OR, NOT_exists_value, NOT_Specific_value, start_date, end_date):

    DB_data = Search(using=es, index="tw.database") \
              .source(includes= csv_columns) \
              .query("bool", must=filter_key_input(Must_exists_value)) \
              .query('bool', must=filter_dict_input(Specific_value)) \
              .query('bool', should=filter_dict_input(filter_OR)) \
              .query('bool', must_not=filter_key_input(NOT_exists_value)) \
              .query('bool', must_not=filter_dict_input(NOT_Specific_value)) \
              .filter("range", **{"@timestamp": {"gte": start_date, "lt": end_date, "time_zone": "Asia/Taipei"}}) \
              .extra(size = 500000) \
              .params(request_timeout = 60)
    response = DB_data.execute()

    docs = []
    for x in response.hits.hits:
        for y in x._source:
            docs.append(x._source[y])
    print(docs)
    return docs

def data_output_multi_soruce(csv_columns, Must_exists_value, Specific_value, filter_OR, NOT_exists_value, NOT_Specific_value, start_date, end_date):

    DB_data = Search(using=es, index="tw.database") \
              .source(includes= csv_columns) \
              .query("bool", must=filter_key_input(Must_exists_value)) \
              .query('bool', must=filter_dict_input(Specific_value)) \
              .query('bool', should=filter_dict_input(filter_OR)) \
              .query('bool', must_not=filter_key_input(NOT_exists_value)) \
              .query('bool', must_not=filter_dict_input(NOT_Specific_value)) \
              .filter("range", **{"@timestamp": {"gte": start_date, "lt": end_date, "time_zone": "Asia/Taipei"}}) \
              .extra(size = 500000) \
              .params(request_timeout = 400)
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
                dict_output[y] = x._source[y]
        list_all.append(dict_output)
    return list_all

##Input series name and output fileid list of FP sample                                                                                                                                 
def data_FP_file(series, start_date, end_date):
    #key, value, start date, end date            
    csv_columns=["File_ID"]

    Must_exists_value = []
    Specific_value = [{"AutoExtraction.Result" : series}]
    filter_OR = []

    NOT_exists_value = []
    NOT_Specific_value = [{"Tracer_Result" : series}]

    start_date = "{}T{}".format(start_date,"00:00:00")
    end_date = "{}T{}".format(end_date,"23:59:59")
    #end_date = []  
    list_fileid = data_output_allarg(csv_columns, Must_exists_value, Specific_value, filter_OR, NOT_exists_value, NOT_Specific_value, start_date, end_date)
    print(str(len(list_fileid)) + ' file FP')

    return list_fileid

##TWO argument: 
##tag_list:[Tracer_Result, Sig_ID]
##fileid_list
def get_info_by_fileidlist(tag_list, file_id_list):

    DB_data = Search(using=es, index="tw.database") \
              .source(includes=tag_list) \
              .query("ids", values=file_id_list) \
              .extra(size=500000) \
              .params(request_timeout=300)

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
                dict_output[y] = x._source[y]
        list_all.append(dict_output)
    return list_all

def get_info_fileids_must(tag_list, file_id_list, must_value_list):

    DB_data = Search(using=es, index="tw.database") \
              .source(includes= tag_list) \
              .query("bool", must=filter_key_input(must_value_list)) \
              .query("ids", values = file_id_list) \
              .extra(size = 500000) \
              .params(request_timeout = 300)

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
                dict_output[y] = x._source[y]
        list_all.append(dict_output)
    return list_all


if __name__ == '__main__':
    csv_columns = ['File_ID']
    Must_exists_value = []
    Specific_value = [{'Tracer_Result':'EPJunk'}]
    filter_OR = []
    NOT_exists_value = []
    NOT_Specific_value = []
    start_date = "{}T{}".format('2022-06-12','06:05:00')
    end_date = "{}T{}".format('2022-06-13','15:07:00')
    list_fileids = data_output_allarg(csv_columns, Must_exists_value, Specific_value, filter_OR, NOT_exists_value, NOT_Specific_value, start_date, end_date)
    print(list_fileids)
