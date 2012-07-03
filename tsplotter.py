import time, datetime, urllib2
from pandas import DataFrame,Series
from xlrd import open_workbook
import tempfile
from datetime import date
from datetime import timedelta
import os
import re
from tsdata.data.cftccot import cot
from tsdata.data.pandasdata import pandasdatareader
from tsdata.data.kitco import kitco
from tsdata.data.futs import futs
from tsdata.data.gnucashdata import gnucashdata
from tsdata.data.commodindex import commodindex
from tsdata.data.etfs import etfs
from tsdata.func.bbgquote import bbgquote
from tsdata.func.bigcharts import bigcharts
from tsdata.func.wsjfs import wsjfs
from tsdata.func.etflookup import etflookup
import tsdata.util
from itertools import izip
from matplotlib.dates import date2num

def commodcot(*args):
    p, t, commod, tag  = args[0:4]
    ts = tsplotter.resolve('%s_%s@%s' % (t,commod,tag))
    return p.applyrange(ts)

def ind(p,ts):
    ts2 = p.applyrange(ts)
    if len(ts2)==0: return ts2
    else:           return ts2 / ts2[min(ts2.index)]

class tsplotter:
    prefixes    = { "docot_" : cot(),
                    "eq_"    : pandasdatareader(),
                    'kitco_' : kitco(),
                    'fred_'  : pandasdatareader(),
                    "f_"     : futs(),
                    'gnc_'   : gnucashdata(),
                    'ix_'    : commodindex(),
                    'etfs_'  : etfs(),
                  }
    funcs       = { 'l1'      : lambda dr,ts:   ts.shift(1),
                   'l'        : lambda dr,ts,n: ts.shift(n),
                   'd1'       : lambda df,ts:   ts-ts.shift(1),
                   'd'        : lambda dr,ts,n: ts-ts.shift(n),
                   'r1i'      : lambda dr,ts:   ts/ts.shift(1) - 1,
                   'cum'      : lambda dr,ts:   ts.cumsum(),
                   'zapz'     : lambda dr,ts,f: ts[f!=0],
                   'ind'      : ind,
                   'commodcot' : commodcot,
                   'bbgquote' : bbgquote,
                   'etf'      : etflookup,
                   'bigcharts' : bigcharts,
                   'wsjfs'    : wsjfs,
                   # rp : filna with method=ffill, rpl : filna with method=bfill
                   }
    symbols     = { "then" : None}

    @classmethod
    def resolve(cls,symbol): # start end too
        reuscore = re.compile("([a-zA-Z][a-zA-Z0-9]*_)")
        m    = reuscore.match( symbol )
        if symbol in cls.symbols:
            return cls.symbols[symbol].ts(symbol)
        elif symbol in cls.funcs:
            return cls.funcs[symbol]
        elif m:
            assert(m.group(1) in cls.prefixes)
            return cls.prefixes[m.group(1)].ts(symbol)
        else:
            raise NameError(symbol)
    
    def parse(self):
        stacks  = []
        exprs   = []
        self.parser.__init__(self)  # clear state
        pat     = self.parser.getpattern()
        for expr,b in zip(self.expressions,self.blocked):
            if not b:
                self.parser.__init__(self)
                parserv = pat.parseString(expr)
                stacks.append(self.parser.getexprstack())
                exprs.append(expr)
        return { 'exprs' : exprs, 'stacks' : stacks }
    
    def add(self,expr):
	self._evalcache = None
        exprs = expr.splitlines()
        for e in exprs:
	    if e[0]=='*':
                if e[1] in '>|': offset = 2
                else:            offset = 1
		self.expressions.append(e[offset:])
		self.hidden.append(True)
                self.axis.append('L')
            elif e[0] in '>|':
		self.expressions.append(e[1:])
		self.hidden.append(False)
		if   e[0]=='>': self.axis.append('R')
		elif e[0]=='|': self.axis.append('V')
		else:           raise Exception('should never happen')
            else:
		self.expressions.append(e)
		self.hidden.append(False)
                self.axis.append("L")
            self.blocked.append(False)
        
    def applyrange(self, ts):
	return tsdata.util.applyrange(ts, self.daterange)
	
    def __iadd__(self,str):
        self.add(str)
        return self
    
    def __init__(self,str = ''):
        self.expressions = []
        self.hidden      = []
        self.blocked     = []
        self.axis        = []
        self.parser      = tsparser(self)
        today            = datetime.date.today()
        self.daterange   = ( today + timedelta(days=-365), today ) # DateRange class not good with date
        self._tsscache   = None
        if str != '':
            self.add(str)
        
    def eval(self):
	if self._evalcache is not None:
	    return self._evalcache
        parsed   = self.parse()
        reuscore = re.compile("([a-zA-Z][a-zA-Z0-9]*_)")
        tss      = []
        for expr,stack in zip(parsed['exprs'], parsed['stacks']):
	    self.parser.setexprstack(list(stack))
	    ts   = self.parser.eval()
	    if type(ts) == Series:
	        if len(stack)>=2 and ( stack[-1] == '~label' or stack[-1] == '~assign'):
		    ts.name = stack[-2]
	        else:
		    ts.name = expr
	    tss.append(self.applyrange(ts))

	self._evalcache = tss
        return tss
         
    def dataframe(self):
	tss   = self.eval()
	df  = DataFrame()
	# FIXME: should do something about potential for dupe names
	for ts,h in zip(tss, self.hidden):
	    if not h and type(ts) != type(''):
		df = df.join(ts,how='outer')
	return df		 
    
    def plot(self, *args, **kwargs):
        tss = self.eval()
        if 'ax' in kwargs: ax = kwargs['ax']
	else:              ax = gca()
        if 'R' in self.axis:
            kwargsr       = kwargs.copy()    
            kwargsr['ax'] = ax.twinx()
        for ts,h,a in zip(tss, self.hidden, self.axis):
	    if not h and type(ts) != type(''):
                if a!='R':
		    kw = kwargs.copy()
                    #ts.plot(*args, **kwargs)
                else:
		    kw = kwargsr.copy()
		    l  = ax.plot([],label=ts.name)
		    kw['color'] = l[0].get_color()
		if a=='V':
		    if ts[0] != 0: ds = [ts.index[0]]
		    else:          ds = []
		    for d, x, lx in zip(ts.index, ts, ts.shift(1))[1:]:
			if (x==0) != (lx==0): ds.append(d)
		    if len(ds) % 2 != 0:
			ds.append(es[1].index[-1])
		    it = iter(ds)
		    for pair in izip(it,it):
			x1=date2num(pair[0])
			x2=date2num(pair[1])
			ax.axvspan(x1,x2, facecolor='b', alpha=0.16)
		else:
		    kw['label'] = ts.name
		    ts.dropna().plot(*args, **kw)

from pyparsing import Word, alphas, ParseException, Literal, CaselessLiteral \
, Combine, Optional, nums, Or, Forward, ZeroOrMore, StringEnd, alphanums, oneOf, Group, delimitedList, Empty, dblQuotedString, removeQuotes, SkipTo
import math, re

class tsparser:
    debug_flag=False

    def __init__(self, parent):
        self.exprStack = []
        self.varStack  = []
        self.variables = {}
        self.parent    = parent

    def pushFirst( self, str, loc, toks ):
        self.exprStack.append( toks[0] )
        
    def pushString( self, str, loc, toks ):
        self.exprStack.append( toks[0][0] )
        self.exprStack.append( '~string' )
        
    def pushLabel( self, str, loc, toks ):
	self.exprStack.append( toks[0] )
	self.exprStack.append( '~label' )
        
    def pushFunctionCall( self, str, loc, toks ):
        self.exprStack.append( toks[0] )
        self.exprStack.append( '~call' )

    def pushArgCount( self, str, loc, toks ):
        self.exprStack.append( len(toks[0]))
        
    def pushUnaryminus( self, str, loc, toks ):
        if toks and toks[0] == '-':
            self.exprStack.append( '~unaryminus' )
            
    def pushAssign( self, str, loc, toks ):
        if len(self.varStack):
            self.exprStack.append( self.varStack.pop() )
            self.exprStack.append( '~assign' )

    def assignVar( self, str, loc, toks ):
        self.varStack.append( toks[0] )

    def getpattern(self):
        # define grammar
        point = Literal('.')
        e = CaselessLiteral('E')
        plusorminus = Literal('+') | Literal('-')
        number = Word(nums) 
        integer = Combine( Optional(plusorminus) + number )
        floatnumber = Combine( integer +
                       Optional( point + Optional(number) ) +
                       Optional( e + integer )
                     )

        ident = Word(alphas,alphanums + '_@' ) 
        plus,minus,mult,div = map(Literal,'+-*/')
        lpar,rpar,comma,semicolon = map(lambda x: Literal(x).suppress(), '(),;')
        addop  = plus | minus
        multop = mult | div
        expop = Literal( "^" )
        assign = Literal( "=" )
        
        expr = Forward()
        
        arglist = Group(Group(expr) + ZeroOrMore( comma + Group(expr)) | Empty()).setParseAction(self.pushArgCount)

        atom = (
                Optional(oneOf("- +")) + (ident+lpar+arglist+rpar).setParseAction(self.pushFunctionCall)
                | (Optional(oneOf("- +")) +
                 (floatnumber|integer|ident).setParseAction(self.pushFirst))
                | Optional(oneOf("- +")) + Group(lpar+expr+rpar)
                ).setParseAction(self.pushUnaryminus)
        
        factor = Forward()
        factor << atom + ZeroOrMore( ( expop + factor ).setParseAction( self.pushFirst ) )
        
        term = factor + ZeroOrMore( ( multop + factor ).setParseAction( self.pushFirst ) )
        expr << ( term + ZeroOrMore( ( addop + term ).setParseAction( self.pushFirst ) )
                | Group(dblQuotedString.setParseAction(removeQuotes)).setParseAction( self.pushString ) )
        bnf = (Optional((ident + assign).setParseAction(self.assignVar)) + expr).setParseAction( self.pushAssign )
        label = semicolon + SkipTo(StringEnd()).setParseAction( self.pushLabel)

        pattern =  bnf + Optional(label) + StringEnd()
        
        return pattern

    # map operator symbols to corresponding arithmetic operations
    opn = { "+" : ( lambda a,b: a + b ),
        "-" : ( lambda a,b: a - b ),
        "*" : ( lambda a,b: a * b ),
        "/" : ( lambda a,b: a / b ),
        "^" : ( lambda a,b: a ** b ) }
    
    def evaluateStack( self ):
        op = self.exprStack.pop()
        if type(op) == type('') and op in "+-*/^":
            self.evaluateStack()
            op2 = self.exprStack.pop()
            self.evaluateStack()
            op1 = self.exprStack.pop()
            self.exprStack.append(self.opn[op]( op1, op2 ))
        elif op in self.variables:
            self.exprStack.append(self.variables[op])
        elif type(op) == type('') and re.search('^[a-zA-Z][a-zA-Z0-9_@]*$',op):
            if self.variables.has_key(op):
                self.exprStack.append( self.variables[op] )
            else:
                self.exprStack.append(self.parent.resolve(op))
        elif op == '~unaryminus':
            self.evaluateStack()
            self.exprStack.append( -self.exprStack.pop() )
        elif op == '~string':
	    pass
	elif op == '~label':
	    self.exprStack.pop()
	    self.evaluateStack()
        elif op == '~assign':
            var = self.exprStack.pop()
            self.evaluateStack()
            val = self.exprStack.pop()
            self.variables[var] = val
            self.exprStack.append(val)
        elif op == '~call':
            funcname = self.exprStack.pop()
            argcount = self.exprStack.pop()
            args     = []
            for i in range(argcount):
                self.evaluateStack()
                v    = self.exprStack.pop()
                args.append(v)
            func     = self.parent.resolve(funcname)
            args.reverse()  # needed because args on stack are actually processed in reverse order
            args.insert(0,self.parent)
            self.exprStack.append(apply(func,args))
        elif type(op) == type('') and re.search('^[-+]?[0-9]+$',op):
            self.exprStack.append(long( op ))
        elif type(op) == type('') and re.search('^[-+]?[0-9]*.?[0-9]*$',op):  # need better....
            self.exprStack.append(float( op ))
        else:
            self.exprStack.append(op)
        
    def eval( self ):
        self.evaluateStack()
        v = self.exprStack.pop()
        return v
    
    def getexprstack( self ):
        return self.exprStack
        
    def setexprstack( self, stack ):
        self.exprStack = stack
