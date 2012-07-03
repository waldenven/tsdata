import urllib2, re, os
from pandas import DataFrame, Series, load
import datetime

def etflookup(p, family, ticker, element = 'NAV'):
    symbol = '%s_%s@%s' % ( family, ticker, element )
    ts     = p.resolve(symbol)
    return p.applyrange(ts)
