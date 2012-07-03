from pandas import DataFrame, Series
from xlrd import open_workbook
from xlrd import xldate_as_tuple
from datetime import datetime
from tsdata.data.basedata import basedata

def etfs_one_sheet(num):
    # Needs to be downloaded and extracted
    fn = '/tmp/Hist NAV Outst AUM.xls'
    wb = open_workbook(fn)
    sheets = wb.sheets()

    s = sheets[num]  # 5 is special, 7 is (AUD), 8 changes shrout

    headers     = [s.cell(0,j).value for j in range(s.ncols)]
    name_cols   = [s.cell(1,j).value for j in range(s.ncols)]
    ticker_cols = [s.cell(2,j).value for j in range(s.ncols)]
    if s.name == 'Gold Bullion':
	headers[1:]   = name_cols[1:]
	ticker_cols   = [ '', '' ] + [ 'GBS' ] * 3
	name_cols     = [ '', '' ] + [ 'Gold Bullion Securities' ] * 3

    idxmap = { 'date' : [ 'Date' ],
	'AUM'        : [ 'Assets Under Management', 'Asset Under Management', 'AUM',
			'Assets Under Management (in USD except for CARB which is in EUR)', 'Assets Under Management (AUD)' ],
	'NAV'        : [ 'NAV', 'NAV (USD except CARB which is in EUR)', 'NAV (AUD)' ],
	'shrout'     : [ 'Number of Shares on ISSUE', 'Shares on issue', 'Shares' ] }
	
    idx = {}
    for k,l in idxmap.iteritems():
	for tag in l:
	    if tag in headers:
		idx[k] = headers.index(tag)
		break

    df           = DataFrame()
    NAV_range    = (idx['NAV'],     idx['shrout'])
    shrout_range = (idx['shrout'],  idx['AUM'] )
    AUM_range    = (idx['AUM'],     s.ncols)    

    names, tickers, NAVs, dates, shrouts, AUMs =  [], [], [], [], [], []
    assetdict         = {}
    assetdict[s.name] = {}
    for row in range(3,s.nrows):
	d  = datetime(*xldate_as_tuple(s.cell(row, idx['date']).value,wb.datemode)).date()
	S = {}
	for l, r in zip( ['NAV', 'shrout', 'AUM'], [NAV_range, shrout_range, AUM_range]):
	    for col in range(r[0], r[1]):
		name   = name_cols[col]
		ticker = ticker_cols[col]
		if ticker == '':
		    continue
		assetdict[s.name][ticker] = name
		v      = s.cell(row,col).value
		if ticker not in S:
		    S.update({ticker : {}})
		S[ticker][l] = v
	rowdf = DataFrame({'ticker' : Series(S.keys()),
			'NAV'       : Series(map(lambda x: x['NAV'],    S.itervalues())),
			'shrout'    : Series(map(lambda x: x['shrout'], S.itervalues())),
			'AUM'       : Series(map(lambda x: x['AUM'],    S.itervalues()))
			})
	rowdf['date']   = Series(d,index=rowdf.index)
	df = df.append(rowdf,ignore_index=True)
    return df
    
class etfs(basedata):
    archivename = 'etfs.pickle'
    tag         = 'etfs'
    earliest    = datetime(2006,1,1)
    chunktype   = 'YEAR'
    lookupkey   = 'ticker'
    