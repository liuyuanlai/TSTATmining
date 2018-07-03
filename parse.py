import glob, sys, pytz, itertools, os
import pandas as pd
import numpy as np
from pyspark import SparkContext, SparkConf
pd.options.mode.chained_assignment = None 

def dataload(fn):
    try:
        _pdf = pd.read_csv(fn, sep=' ')
        pdf  = _pdf.dropna(0, 'any')
    except:
        print 'load %s failed' % (fn)
        return None

    if _pdf.shape[0] != pdf.shape[0]:
        print "%d records of %s are dropped due to nan" % (_pdf.shape[0] - pdf.shape[0], fn)

    local = pytz.timezone ("America/Los_Angeles")
    pdf['first:29'] = pdf['first:29'].apply(lambda x: pd.to_datetime(x, unit='ms')\
                                           ).apply(lambda x: local.localize(x, is_dst=True)\
                                           ).apply(lambda x: x.astimezone(pytz.utc)\
                                           ).apply(lambda x: x.replace(tzinfo=None))
    
    pdf['last:30']  = pdf['last:30' ].apply(lambda x: pd.to_datetime(x, unit='ms')\
                                           ).apply(lambda x: local.localize(x, is_dst=True)\
                                           ).apply(lambda x: x.astimezone(pytz.utc)\
                                           ).apply(lambda x: x.replace(tzinfo=None))

    _col = [x.split(':')[0] for x in pdf.columns];_col[0] = 'c_ip'; pdf.columns = _col

    return pdf
    
def dataload_multi(fns, fn_out):
    if len(fns) == 0: return None
    ret = dataload(fns[0])
    for fn in fns[1:]:
        _ret = dataload(fn)
        if _ret is None: continue
        ret = ret.append(_ret)
    ret.to_csv(fn_out, index=False)

def month_fp(para):
    ndx, m = para
    fns = glob.glob('dtn%02d-%02d/2017_%02d_*/log_tcp_complete' % (ndx, m, m))[:]
    if len(fns) == 0: return 0
    fn_out = 'dtn%02d-%02d.csv' % (ndx, m)
    if os.path.exists(fn_out):
        print "%s already exists, return now" % (fn_out)
        return len(fns)
    dataload_multi(fns, fn_out)
    print "%d files for %s has been processed" % (len(fns), fn_out[:-4]) 
    return len(fns)

if __name__ == '__main__':
    conf = (SparkConf().setAppName('TSTAT logs process')
                       .set("spark.executor.memory", "80g")
                       .set('spark.driver.memory', '10g')
                       .set('spark.driver.maxResultSize', '0g')
                       .set("spark.executor.heartbeatInterval", "10s"))
    
    sc = SparkContext(conf=conf)
    space = sc.parallelize([x for x in itertools.product([1, 2,]+range(4, 11), range(1, 13))], 30)

    nf = space.map(month_fp).collect() 
    print '%d files have been processed' % (np.sum(nf))

