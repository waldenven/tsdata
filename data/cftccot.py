from zipfile import *
import time, datetime, urllib2
from pandas import *
from xlrd import open_workbook
import tempfile
from datetime import date
import os
import re
from tsdata.util import dropdupe
from tsdata.data.basedata import basedata

symbols = {
u'COTTON NO. 2 - NEW YORK BOARD OF TRADE'                                   : 'CT',
u'LEAN HOGS - CHICAGO MERCANTILE EXCHANGE'                                  : 'LH',
u'WHEAT - CHICAGO BOARD OF TRADE'                                           : 'W',
u'CRUDE OIL, LIGHT SWEET - NEW YORK MERCANTILE EXCHANGE'                    : 'CL',
u'LIVE CATTLE - CHICAGO MERCANTILE EXCHANGE'                                : 'LC',
u'PLATINUM - NEW YORK MERCANTILE EXCHANGE'                                  : 'PL',
u'SOYBEAN MEAL - CHICAGO BOARD OF TRADE'                                    : 'SM',
u'COFFEE C - ICE FUTURES U.S.'                                              : 'KC',
u'CRUDE OIL, LIGHT SWEET - ICE EUROPE'                                      : 'CS',
u'SILVER - COMMODITY EXCHANGE INC.'                                         : 'SI',
u'BRENT FINANCIAL - NEW YORK MERCANTILE EXCHANGE'                           : 'LCO',
u'SUGAR NO. 11 - NEW YORK BOARD OF TRADE'                                   : 'SB',
u'COCOA - ICE FUTURES U.S.'                                                 : 'CC',
u'FRZN CONCENTRATED ORANGE JUICE - ICE FUTURES U.S.'                        : 'OJ',
u'ROUGH RICE - CHICAGO BOARD OF TRADE'                                      : 'RR',
u'OATS - CHICAGO BOARD OF TRADE'                                            : 'O',
u'COTTON NO. 2 - ICE FUTURES U.S.'                                          : 'CT',
u'WTI CRUDE OIL FINANCIAL - NEW YORK MERCANTILE EXCHANGE'                   : '26',
u'GASOIL (ICE) SWAP - NEW YORK MERCANTILE EXCHANGE'                         : 'LGO',
u'NATURAL GAS - NEW YORK MERCANTILE EXCHANGE'                               : 'NG',
u'WHEAT - MINNEAPOLIS GRAIN EXCHANGE'                                       : 'MW',
u'GASOLINE BLENDSTOCK (RBOB) - NEW YORK MERCANTILE EXCHANGE'                : 'RB',
u'WHEAT - KANSAS CITY BOARD OF TRADE'                                       : 'KW',
u'FRZN PORK BELLIES - CHICAGO MERCANTILE EXCHANGE'                          : 'PB',
u'RBOB GASOLINE FINANCIAL - NEW YORK MERCANTILE EXCHANGE'                   : '27',
u'CORN - CHICAGO BOARD OF TRADE'                                            : 'C',
u'PALLADIUM - NEW YORK MERCANTILE EXCHANGE'                                 : 'PA',
u'SUGAR NO. 11 - ICE FUTURES U.S.'                                          : 'SB',
u'MILK, Class III - CHICAGO MERCANTILE EXCHANGE'                            : 'DA',
u'GOLD - COMMODITY EXCHANGE INC.'                                           : 'GC',
u'CRUDE OIL, LIGHT SWEET - ICE FUTURES EUROPE'                              : 'CS',
u'FEEDER CATTLE - CHICAGO MERCANTILE EXCHANGE'                              : 'FC',
u'SOYBEANS - CHICAGO BOARD OF TRADE'                                        : 'S',
u'NO. 2 HEATING OIL, N.Y. HARBOR - NEW YORK MERCANTILE EXCHANGE'            : 'HO',
}

tags = {
 u'M_Money_Positions_Long_ALL'     : 'mml',
 u'M_Money_Positions_Short_ALL'    : 'mms',
 u'M_Money_Positions_Spread_ALL'   : 'mmsp',
 u'NonRept_Positions_Long_All'     : 'nrl',
 u'NonRept_Positions_Short_All'    : 'nrs',
 u'Open_Interest_All'              : 'oi',
 u'Other_Rept_Positions_Long_ALL'  : 'orl',
 u'Other_Rept_Positions_Short_ALL' : 'ors',
 u'Other_Rept_Positions_Spread_ALL': 'orsp',
 u'Prod_Merc_Positions_Long_ALL'   : 'pml',
 u'Prod_Merc_Positions_Short_ALL'  : 'pms',
 u'Swap_Positions_Long_All'        : 'sdl',
 u'Swap__Positions_Short_All'      : 'sds',
 u'Swap__Positions_Spread_All'     : 'sdsp',
}

tagrev = {v:k for k, v in tags.items()}

def parsecftc(url):
    f = urllib2.build_opener(urllib2.HTTPCookieProcessor()).open(url)
    bin = f.read()
    f.close()
    tf = tempfile.NamedTemporaryFile()
    tf.write(bin)
    tf.seek(0)
    zf = ZipFile(tf)
    nl = zf.namelist()
    assert len(nl) == 1
    fn = zf.extract(nl[0])
    
    wb = open_workbook(fn)
    assert len(wb.sheets()) == 1
    s  = wb.sheets()[0]
    rowlabels=[s.cell(x,0).value for x in range(0,s.nrows)]
    collabels=[s.cell(0,y).value for y in range(0,s.ncols)]
    df = DataFrame(dict([(n,Series([s.cell(x,j).value for x in range(1,s.nrows)]))
                    for j,n in enumerate(collabels)]))
    
    dates=Series(map(lambda x: date.fromordinal(int(x)+693594), df['Report_Date_as_MM_DD_YYYY']),index=df.index)
    df['date']=dates
    
    symrev = {v:k for k, v in symbols.items()}
    subdf=df[map(lambda x: x in symbols,df['Market_and_Exchange_Names'])]
    syms=Series(map(lambda x: symbols[x], subdf['Market_and_Exchange_Names']),index=subdf.index)
    df['symbol']=syms
    
    return df

class cot(basedata):
    archivename = 'cftccot.pickle'
    tag         = 'docot'
    _cache      = None
    _changed    = False
    _updated    = False
    chunktype   = 'YEAR'
    earliest    = datetime(2007,1,1)
        
    def _updateyear(self, y):
        # docot
        if y<2011:
            url = 'http://cftc.gov/files/dea/history/com_disagg_xls_hist_2006_2010.zip'
        else:
            url = 'http://cftc.gov/files/dea/history/com_disagg_xls_%d.zip' % (y)
        df   = parsecftc(url)
        return dropdupe(df, ['date', 'Market_and_Exchange_Names'])
        
    def _tagmap(self, tag):
	return tagrev[tag]
