from datetime import datetime, timedelta
import os, pandas.core.common

'''
TODO: date check should be holiday/weekend aware too
TODO: should compare old version from cache to fresh one
'''

_cache   = {}
_updated = {}

def maxdate(df):
    if 'date' in df.columns:
	return max(df['date'])
    else:
	return max(df.index)
	
def _set_up_to_date(fn, keyname):
    if fn not in _updated:
	_updated[fn] = {}
    _updated[fn][keyname] = True
    
def _is_up_to_date(df, fn, keyname):
    try:
	if _updated[fn][keyname]:
	    return True
    except KeyError:
	pass
    today = datetime.now().date()
    if today.weekday() == 5 or today.weekday() == 6:
        today = today - timedelta(days=today.weekday()-4)
    md    = maxdate(df)
    return md >= today

def wrap(f, mapkey=lambda x: x, persist=True, sep=':'): # mapkey good for str.upper or str.lower
    fn = f.func_name
    if fn not in _cache:
        _cache[fn] = {}
    def g(*args):
        keyname = mapkey(sep.join(args))
        try:
            return _cache[fn][keyname]
        except KeyError:
	    #maybe it is persisted and up to date
	    try:
		df = _load(fn, keyname)
		if _is_up_to_date(df, fn, keyname):
		    return df
            except IOError:
                df = None
            v = f(*args)
            v = _merge(v,df)
            _cache[fn][keyname] = v
            print 'saved results for %s, %s to cache' % (fn, keyname)
            if persist:
		_persist(fn, keyname, v, df)
            return v
    return g

_archivedir  = os.path.expanduser("~/tsplot/data/funccache")
    
def _persist(fn, keyname, df, dfold):
    if not os.path.isdir(_archivedir):
	os.makedirs(_archivedir)
    dirname  = os.path.join(_archivedir, fn)
    if not os.path.isdir(dirname):
	os.makedirs(dirname)
    filename = os.path.join(dirname, keyname)
    df.save(filename)
    _set_up_to_date(fn, keyname)
    
def _load(fn, keyname):
    filename = os.path.join(_archivedir, fn, keyname)
    return pandas.core.common.load( filename )
    
def _merge(df, dfo):
    if not dfo:
	return df
    # assumed they have a column 'date' with unique values
    dfi        = df.copy()
    dfoi       = dfo.copy()
    dfi.index  = dfi['date']
    dfoi.index = dfoi['date']
    rv         = dfoi.combine_first(dfi)
    print '%d new points merged, %d missing' % (len(set(rv.index)-set(dfoi.index)),
        len(set(rv.index)-set(dfi.index)))
    rv.index   = range(len(rv.index))
    return rv
    
