import datetime
from pandas import DataFrame,Series
    
def applyrange(ts,dr):
    if ( isinstance(ts, Series) or isinstance(ts,DataFrame) ) and len(ts.index) > 0:
	if type(ts.index[0]) is datetime.date:
	    index = filter( lambda d: dr[0] <= d <= dr[1], ts.index )
	else:
	    assert type(ts.index[0]) is datetime.datetime
	    index = filter( lambda d: dr[0] <= d.date() <= dr[1], ts.index )
	ts    = ts.reindex(index)
    return ts

def dropdupe(df, keys):
    df3              = df
    df3['origindex'] = Series( df3.index, index=df3.index)
    g                = df3.groupby(keys)
    badindex         = []

    for k,v in g:
        for i in range(len(v)-1):
            badindex.append( v['origindex'].ix[i] )
    if len(badindex) > 0:
        print 'Removing %d dupe rows' % (len(badindex))
    return df.drop(badindex,axis=0)
