from pandas.io.data import DataReader
import re, datetime

class pandasdatareader:
    def parsesymbol(self, symbol):
        synre     = re.compile('(eq|fred)_([^@]*)@(.*)' )
        synrenoat = re.compile('(eq|fred)_([^@]*)' )
        m         = synre.match( symbol )
        if m:
            tag   = m.group(3)
            eq    = m.group(2)
            proto = m.group(1)
            if proto == 'eq':
	      proto = 'yahoo'
            return { 'hlocv' : tag.lower(), 'eq' : eq, 'proto' : proto }
        else:
            m     = synrenoat.match( symbol )
            if not m:
                raise NameError(symbol)
            eq    = m.group(2)
            proto = m.group(1)
            if proto == 'eq':
	      proto = 'yahoo'
            if proto == 'fred':
	      hlocv = 'value'
	    else:
	      hlocv = 'close'
            return { 'hlocv' : hlocv, 'eq' : eq, 'proto' : proto }
    
    def ts(self, symbol):
        parse = self.parsesymbol(symbol)
        df    = DataReader(parse['eq'], parse['proto'],start=datetime.datetime(1950,1,1))
        df    = df.rename(columns=lambda x: '_'.join(x.split()).lower()) # Need for Adj Close :(
        #print df.columns
        ts = df[parse['hlocv']]
        ts.index = map(lambda x: x.date(), ts.index)
        ts.name  = parse['eq']
        if '@' in symbol:
	    ts.name += '@%s' % (parse['hlocv'])
        return ts

"""
We need dividends too...

Also FRED

Plus this:
#tips = com.load_data('tips', package='reshape2')
tips['tip_pct'] = tips['tip'] / tips['total_bill']
tips.head()
"""
