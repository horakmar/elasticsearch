from elasticsearch import helpers, Elasticsearch
import csv, sys

def safeFloat(key):
    try:
        return float(key)
    except:
        return 0.0

def safeInt(key):
    try:
        return int(key)
    except:
        return 0

def tweaker(reader):
    for i in reader:
        del i['Server storage']
        del i['System disk origin']
        i['Pocet CPU'] = safeInt(i['Pocet CPU'])
        i['Pocet jader CPU'] = safeInt(i['Pocet jader CPU'])
        i['RAM'] = safeInt(i['RAM'])
        i['Frekvence CPU'] = safeFloat(i['Frekvence CPU'])
        yield i

reload(sys)
sys.setdefaultencoding('UTF8')

es = Elasticsearch('http://es-http.oos.svc.kube5.corp:80/')

with open('netstat.csv') as f:
    reader = csv.DictReader(f, delimiter=';')
#    for i in tweaker(reader):
#    for i in reader:
#        print i
#    helpers.bulk(es, tweaker(reader), index='caplan', doc_type='srv-attr')
    helpers.bulk(es, reader, index='etumigr', doc_type='netstat')
