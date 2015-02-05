#!/usr/bin/python
#encoding:utf-8

from elasticsearch import Elasticsearch,helpers
#from _functools import partial
#from filter import *
import datetime
action_cols = ["_op_type","_index","_type","_id","_source"]
    
class MyEsManager():
    '''elasticsearch operation python tools
    '''
    def __init__(self,host=None):
        self.es = Elasticsearch(host)
        self.buff = []
        self.N = 0

    def isAlive(self):
        '''Returns True if the cluster is up, False otherwise.
        '''
        return self.es.ping()
    
    def count(self,_index=None,_type=None):
        '''Get number by index,type
        '''
        return self.es.count(_index,_type)['count']

    def get(self,_index,_type,size=1):
        '''Get a dict_data list by index,type.
           size is the number of results
        '''
        obj = self.es.search(_index,_type,size=size)['hits']['hits']
        datas = []
        while obj:
            print len(obj)
            data = obj.pop()['_source']
            datas.append(data)
        return datas
          
    def getAll(self,_index,_type):
        '''Get all dict_data by index,type
        '''
        size = self.count(_index, _type)
        return self.get(_index, _type, size)

    def create_action(self,op_type,_index,_type,_id,data={}):
        '''create action for index,delete,update
        '''
        if op_type == "delete":
            action_cols.pop(4)
            return dict(zip(action_cols,[op_type,_index,_type,_id]))
        else:
            return dict(zip(action_cols,[op_type,_index,_type,_id,data]))
    
    def bulk_exec(self,actions,max_size=200):
        while len(actions) > max_size:
            helpers.bulk(self.es, actions)
            del actions[:max_size]
        helpers.bulk(self.es,actions)

if __name__ == "__main__":
    #my = MyEsManager('202.121.97.61')
    '''
    with open("/home/lexxe/new_data2.txt","r") as f:
        datas = list(f)
        actions = []
        while datas:
            data = eval(datas.pop())
            actions.append(my.create_action("index", "test_index", "usa_test3", data.pop("id"), data))
        my.bulk_exec(actions)
    '''
    my = MyEsManager("192.168.1.166")
    with open("/home/lexxe/china_usa_bk.txt",'r') as f:
        actions = []
        for a in f:
            actions.append(eval(a))
        my.bulk_exec(actions)