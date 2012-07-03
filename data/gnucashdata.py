from gnucash import Session, Account, Split
from pandas import DataFrame, Series
from tsdata.data.basedata import basedata
from tsdata.util import dropdupe
import re

def df_for_pricelist(pl):
    cols = ['get_commodity',
            'get_currency',
            'get_instance',
            'get_source',
            'get_time',
            'get_type',
            'get_typestr',
            'get_value',
            ]
    i=[[r.get_time(),r.get_value().num,r.get_value().denom,r.get_source(),r.get_typestr()]
            for r in pl]
    df = DataFrame({k:[x[j] for x in i] 
            for j,k in enumerate(['date', 'num', 'denom', 'source', 'type'])})
    return df

def get_all_commods(commod_table):
    ns = commod_table.get_namespaces_list()
    allstock = []
    for n in ns:
        allstock.extend(n.get_commodity_list())
    return allstock

def full_price_list(commod_table, pdb):
    commods = get_all_commods(commod_table)
    usd     = commod_table.lookup('CURRENCY', 'USD')
    df      = DataFrame()
    for c in commods:
        pl                   = pdb.get_prices(c,usd)
        thisdf               = df_for_pricelist(pl)
        thisdf['namespace' ] = Series( c.get_namespace(), index=thisdf.index )
        thisdf['commod' ]    = Series( c.get_mnemonic(),  index=thisdf.index )
        df                   = df.append(thisdf, ignore_index=True)
    return(df)

class gnucashdata(basedata):
    FILE = "/home/leif/finances/2007"
    url  = "xml://" + FILE
    tag  = 'gnc'
    
    # Override _loadarchive to build the DataFrame from the gnucash archive
    def _loadarchive(self):
        if self._cache is not None:
            return
        session      = Session(self.url, True, False, False)
        root         = session.book.get_root_account()
        book         = session.book
        account      = book.get_root_account()
        commod_table = book.get_table()
        pdb          = book.get_price_db()
        df           = full_price_list(commod_table, pdb)
        df['value']  = df['num'] * 1.0 / df['denom']
        session.end()
        session.destroy()
        self._cache  = dropdupe(df, ['date', 'namespace', 'commod'])
    
    def parsesymbol(self, symbol):
        synrenoat  = re.compile('%s_([^@_]*)'      % self.tag )
        
        m          = synrenoat.match( symbol )
        commod = m.group(1)
        tag    = 'value'
        return { 'column' : tag, 'filter' : { 'commod' : commod } }

