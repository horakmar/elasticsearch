#!/opt/rh/python27/root/usr/bin/python
# vim:fileencoding=utf-8:tabstop=4:shiftwidth=4

############################################ 
#  Transport trendu jedne polozky
#  ze Zabbixu do Elasticsearch
#
#  Autor: Martin Horak
#  Verze: 1.0
#  Datum: 
#
############################################
import sys
from datetime import datetime
import pyzabbix
from elasticsearch import helpers, Elasticsearch

import pprint

## Environment ## ==========================
################# ==========================
zbx_url  = 'http://localhost/zabbix/'
zbx_user = 'apiuser'
zbx_pwd  = 'ApiZab04'

index_settings = {
  "settings": {
    "number_of_shards": 3,
    "number_of_replicas": 1
  },
  "mappings": {
    "_default_": {
      "properties": {
        "@timestamp": {
          "type": "date",
          "format": "epoch_second"
        }
      }
    }
  }
}

## Functions ## ============================
############### ============================
## Usage ## --------------------------------
def Usage():
    '''Usage help'''

    usage = """
Pouziti:
    {script_name} [-h] [-tvq] -g <group> -k <key> [-n <elastic_key_name>] -s <start> [-N] [-i <index>] [-y <type>]

Program transportuje trendy jedne polozky ze Zabbixu do Elasticsearch

Parametry:
    -h  ... help - tato napoveda
    -t  ... test - neprovadet prikazy, pouze vypsat
    -v  ... verbose - vypisovat vice informaci
    -q  ... quiet - vypisovat mene informaci
    -N  ... smazat index a vytvorit novy
    <group> ... skupina v Zabbixu pro kterou vybrat trendy
    <key>   ... klic vybirane polozky
    <start> ... datum a cas zacatku ve formatu YY/MM/DD,hh:mm:ss
    <elastic_key_name> ... jmeno polozky v Elasticsearch
    <index> ... jmeno indexu v Elasticsearch [timetest]
    <type>  ... jmeno typu v Elasticsearch [trends]

Chyby:

"""
    print usage.format(script_name = sys.argv[0])
    return

## Usage end ## ----------------------------

## getItems ## -----------------------------
def getItems(zapi, zbx_group, zbx_key):
    r = zapi.do_request('item.get', {
      'output': 'itemid',
      'selectHosts': ['host'], 
      'group' : zbx_group,
      'monitored' : True,
      'filter': {'key_': zbx_key}
    })
    return r['result']
## getItems end ## -------------------------

def getTrends(zapi, items, start, keyname):
    for item in items:
        if verbose > 1:
            print("Host: {} ...".format(item['hosts'][0]['host'], end=''))
        r = zapi.do_request('trend.get', {
          'output': ('clock', 'value_min', 'value_avg', 'value_max'),
          'itemids': item['itemid'],
          'time_from': start
        })
        for i in r['result']:
            i['host'] = item['hosts'][0]['host']
            i['@timestamp'] = int(i.pop('clock'))
            i['metric'] = keyname
            for j in ('min','avg','max'):
                i['v_{}'.format(j)] = float(i.pop('value_{}'.format(j)))
            yield i
    print

## Main ## =================================
########## =================================
def main():
    '''Hlavni program'''

    try:
        zapi = pyzabbix.ZabbixAPI(zbx_url, user=zbx_user, password=zbx_pwd)
    except Exception as e:
        print("Nelze se pripojit k Zabbixu. {}".format(e))
        return


#    pp = pprint.PrettyPrinter(indent=2)

    items = getItems(zapi, zbx_group, zbx_key)
    if not items:
        print("Zadne polozky nenalezeny.")
        return

    es = Elasticsearch('http://caplan-es.cezdata.corp:80/')

    if newindex:
        es.indices.delete(index=es_index, ignore=[400, 404])
        es.indices.create(index=es_index, body=index_settings)
    
    helpers.bulk(es, getTrends(zapi, items, startstamp, keyname), index=es_index, doc_type=es_type)

    if verbose > 1: print("Main succesful end.")
## Main end =================================
########### =================================

## Variables ## ============================
############### ============================
test = False
verbose = 1
zbx_group = None
zbx_key = None
start = None
keyname = None
es_index = 'timetest'
es_type = 'trends'
newindex = False

## Getparam ## -----------------------------
if __name__ == '__main__': 
    argn = []
    args = sys.argv;
    i = 1
    try:
        while(i < len(args)):
            if(args[i][0] == '-'):
                for j in args[i][1:]:
                    if j == 'h':
                        Usage()
                        sys.exit()
                    elif j == 't':
                        test = True
                    elif j == 'v':
                        verbose += 1
                    elif j == 'q':
                        verbose -= 1
                    elif j == 'g':
                        i += 1
                        zbx_group = args[i]
                    elif j == 'k':
                        i += 1
                        zbx_key = args[i]
                    elif j == 's':
                        i += 1
                        start = args[i]
                    elif j == 'n':
                        i += 1
                        keyname = args[i]
                    elif j == 'i':
                        i += 1
                        es_index = args[i]
                    elif j == 'y':
                        i += 1
                        es_type = args[i]
                    elif j == 'N':
                        newindex = True
            else:
                argn.append(args[i])
            i += 1
    except IndexError:
        print("Chyba cteni parametru.")
        Usage()
        sys.exit(3)
    ## Getparam end ## -------------------------

    if(zbx_group == None or zbx_key == None or start == None):
        print("Chyba - nezadany povinne parametry.")
        sys.exit(1)
    try:
        starttime = datetime.strptime(start, '%d.%m.%Y,%H:%M:%S')
    except ValueError:
        print("Chybne zadany start.")
        sys.exit(2)
    startstamp = (starttime - datetime(1970, 1, 1)).total_seconds()

    if not keyname:
        keyname = zbx_key

    main()


