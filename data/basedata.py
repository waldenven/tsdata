import os, pandas.core.common, re
from pandas import DataFrame, Series, DateRange
from datetime import datetime, date, timedelta

class basedata:
    archivedir  = os.path.expanduser("~/tsplot/data")
    archivename = '***FILL**IN**NAME***.pickle'
    tag         = 'basedata'
    earliest    = datetime(2011,1,1)
    chunktype   = 'YEAR' # or 'MONTH' or 'DAY'
    lookupkey   = 'symbol'
    _cache      = None
    _changed    = False
    _updated    = False
    _scaling    = {}
    
    def archivefilename(self):
        return os.path.join(self.archivedir, self.archivename )
    
    def _loadarchive(self):
        if self._cache is None:
            self._cache = pandas.core.common.load( self.archivefilename() )
            
    def _savearchive(self):
        if self._cache is not None:
            self._cache.save(self.archivefilename())

    def data(self):
        self._loadarchive()
        return self._cache
        
    def _tagmap(self, tag):
	return tag
        
    def parsesymbol(self, symbol):
        synre      = re.compile('%s_([^@]*)@(.*)' % self.tag )
        synrenoat  = re.compile('%s_([^@]*)'      % self.tag )
        m          = synre.match( symbol )
        if m:
            tag    = self._tagmap( m.group(2) )
            commod = m.group(1)
            return { 'column' : tag, 'filter' : { self.lookupkey : commod } }
        m          = synrenoat.match( symbol )
        tag        = m.group(1)
        return { 'column' : tag }
    
    def ts(self, symbol):
        df    = self.data()
        parse = self.parsesymbol(symbol)
        if 'filter' in parse:
            for k,v in parse['filter'].items():
                df = df[df[k]==v]
        if 'date' in df.columns:
            ret = Series(df[parse['column']],index=df['date'])
        else:
            ret = df[parse['column']]
        return ret.sort_index() * self._scaling.get(parse['column'],1)
        
    # Subclasses can override to say when data in cache is not final        
    def isfinal(self, d):
	return True

    def update(self, today=datetime.today()):
        df    = self.data()
        final = False
        while not final:
	    if 'date' in df.columns:
		maxdate = max(df['date'])
	    else:
		maxdate = max(df.index)
	    final = self.isfinal(maxdate)
	    if not final:
		print '%s is not final, stripping' % maxdate
		if 'date' in df.columns:
		    df = df[df['date'] != maxdate]
		else:
		    df = df.reindex(df.index - [maxdate])
        print 'maxdate = %s, today = %s' % (maxdate, today)
        newdf = DataFrame()
        if self.chunktype == 'YEAR':
            for y in range(maxdate.year, today.year+1):
		print 'performing update for %d' % (y)
                updf  = self._updateyear(y)
                print updf[-3:]
                newdf = newdf.append(updf,ignore_index='date' in df.columns)
        elif self.chunktype == 'DAY':
	    start = maxdate + timedelta(days=1)
	    start = datetime(*(start.timetuple()[:6]))
	    dr    = DateRange(start, today)
	    for d in dr:
                if d == datetime(2011,12,26) or d == datetime(2012,1,2):
                    continue
		print 'performing update for %s' % (d)
		updf  = self._updateday(d.date())
		print updf[-3:]
		newdf = newdf.append(updf,ignore_index='date' in df.columns)
        else:
	    raise NameError('unknown chunktype ' + self.chunktype)
	
        if 'date' in df.columns:
            newdf = newdf[newdf['date']>maxdate]
        else:
	    print newdf.index[-3:]
	    newindex = filter(lambda d: d>maxdate, newdf.index)
	    print 'fetched %d rows, %d rows more recent than maxdate = %s' % (len(newdf), len(newindex), maxdate)
            newdf    = newdf.reindex(newindex)
        print 'end of new data: %s' % (newdf[-3:])
        self._cache = df.append(newdf, ignore_index='date' in df.columns)

    def initialupdate(self):
        if self.chunktype == 'YEAR':
            y = self.earliest.year
            updf  = self._updateyear(y)
        elif self.chunktype == 'DAY':
            d = self.earliest
            updf  = self._updateday(d)
        else:
	    raise NameError('unknown chunktype ' + self.chunktype)
        
        self._cache = updf
