from urllib3.util import connection
import dns.resolver
import random
import pickle
import multiprocess as mp
import pandas as pd
import requests


def my_dns_resolver(x):
    resolver = dns.resolver.Resolver(configure=False)
    srv=random.choice(dns_servers)
    resolver.nameservers=[srv]
    answer = resolver.resolve(x)
    for rr in answer:
        return rr.to_text()

def patched_create_connection(address, *args, **kwargs):
    """Wrap urllib3's create_connection to resolve the name elsewhere"""
    host, port = address
    hostname = my_dns_resolver(host)
    return _orig_create_connection((hostname, port), *args, **kwargs)

def foo(i,short_url,resolved_urls):
    connection.create_connection = patched_create_connection
    try:
        resolved_urls[short_url]=requests.get(short_url,allow_redirects=True,timeout=10).url
    except Exception as e:
        e=str(e)
        resolved_urls[short_url]=e
    print(i,len(resolved_urls))

number=0
manager=mp.Manager()
resolved_urls=manager.dict()

dns_servers=["8.8.8.8","8.8.4.4","9.9.9.9","208.67.222.222","208.67.220.220","1.0.0.1"]
df=pickle.load(open('/effectcrawl/ashwin/french_urls.pkl','rb'))
print("File Loaded")
_orig_create_connection = connection.create_connection

pool=mp.Pool(processes=500)
for i in range(len(df)):
    pool.apply_async(foo,args=[i,df['urls'].iloc[i],resolved_urls])
pool.close()
pool.join()
print("Threads joined")
resolved_urls=dict(resolved_urls)
resolved_urls_df=pd.DataFrame(resolved_urls.items(),columns=['short_url','resolved_url'])
resolved_urls_df.to_csv('/effectcrawl/ashwin/french_resolved.csv',index=False)
