# Programky pro praci s Elasticsearch

## Priklady pouziti

### Datova pumpa trendu ze Zabbixu do Elasticsearch
```bash
./trends_zbx_elastic.py -g "OS AIX" -s 01.08.2017,00:00:00 -k 'zbx.collect.nmon.system.stat[cpu,pc]' -n "CPU physical" -i os_aix -y trends -v -N
./trends_zbx_elastic.py -g "IBM Power" -s 01.08.2017,00:00:00 -k 'zbx_hmc_cpu_pools.sh["{HOST.CONN}","total"]' -n "CPU usage" -i time2 -y trends -v -N
```
