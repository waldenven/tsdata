#!/usr/bin/python
# -*- coding: iso-8859-1 -*-

import wx, wx.calendar, wxmpl, unicodedata, sys, re, os
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
from tsplotter import tsplotter
from pandas import DataFrame, Series

class simpleapp_wx(wx.Frame):
    def __init__(self,parent,id,title):
        wx.Frame.__init__(self,parent,id,title)
        self.parent        = parent
        self.plotter       = tsplotter()
        
        self.splitter      = wx.SplitterWindow(self, -1)
        self.canvas        = wxmpl.PlotPanel(self.splitter, -1, location=False, autoscaleUnzoom=False)
	self.panel_bottom  = wx.Panel(self.splitter,-1)
        self.panel_control = wx.Panel(self.panel_bottom, -1)
        self.plotbutton    = wx.Button(self.panel_control, -1, label='Plot')
        self.startdate     = wx.DatePickerCtrl(self.panel_control)
        self.enddate       = wx.DatePickerCtrl(self.panel_control)
        self.textfield     = wx.TextCtrl(self.panel_bottom, style=wx.TE_MULTILINE)
        
        self.menubar       = wx.MenuBar()
        
	self.figure        = self.canvas.get_figure()
        self.last_name_saved = None
        
        self.initialize()

    def initialize(self):
        # Menu
        self.menu_file = wx.Menu()
        
        self.menu_file_new    = self.menu_file.Append(wx.ID_NEW, 'New', 'New File')
        self.menu_file_open   = self.menu_file.Append(wx.ID_OPEN, 'Open', 'Open File')
        self.menu_file_save   = self.menu_file.Append(wx.ID_SAVE, 'Save', 'Save File')
        self.menu_file_saveas = self.menu_file.Append(wx.ID_SAVEAS, 'Save As', 'Save File As')
        self.menu_file.AppendSeparator()
        self.menu_file_quit   = self.menu_file.Append(wx.ID_EXIT, 'Quit', 'Quit application')
        self.menubar.Append(self.menu_file, '&File')
        self.SetMenuBar(self.menubar)
        
        self.Bind(wx.EVT_MENU, self.OnOpenFile, self.menu_file_open)
        self.Bind(wx.EVT_MENU, self.OnSaveFile, self.menu_file_save)
        self.Bind(wx.EVT_MENU, self.OnSaveAsFile, self.menu_file_saveas)
        self.Bind(wx.EVT_MENU, self.OnQuit, self.menu_file_quit)
        
        sizer_control = wx.BoxSizer(wx.HORIZONTAL)

        self.Bind(wx.EVT_BUTTON, self.OnPlotEvent, self.plotbutton)
        
        self.startdate.SetValue(wx.DateTime.Today() - wx.DateSpan(days=365))
        
        self.enddate.SetValue(wx.DateTime.Today())
        
        self.legendcheck = wx.CheckBox(self.panel_control, -1, 'Legend')
        self.legendcheck.SetValue(True)
        self.legendcheck.Bind(wx.EVT_CHECKBOX,self.OnLegendCheck)
        
        sizer_control.Add(self.plotbutton,0)
        sizer_control.Add(self.startdate,0)
        sizer_control.Add(self.enddate,0)
        sizer_control.Add(self.legendcheck,0)
        
        self.panel_control.SetSizer(sizer_control)
        
        self.textfield.Bind(wx.EVT_KEY_DOWN, self.OnKeyDown)
        self.textfield.SetFocus()
        self.textfield.SetValue('docot_CL@mml') 
        
        sizer_bottom  = wx.BoxSizer(wx.VERTICAL)
        sizer_bottom.Add(self.panel_control,0)
        sizer_bottom.Add(self.textfield,1, wx.EXPAND)
        self.panel_bottom.SetSizer(sizer_bottom)

        self.SetSize((800,600))
        self.splitter.SplitHorizontally(self.canvas, self.panel_bottom,450)

        # simulate Plot click to start
        self.OnPlotEvent(None)
        
        self.Show(True)

    def OnPlotEvent(self,event):
        text = self.textfield.GetValue()
        dr   = ( self.startdate.GetValue(), self.enddate.GetValue() )
        dr   = map(wx.calendar._wxdate2pydate, dr )
        self.plotter.__init__()
        self.plotter.daterange = dr
        self.plotter += unicodedata.normalize('NFKD', text).encode('ascii','ignore')
        self.plot()
        
    def plot(self):
        for a in self.figure.axes[1:]:
	    self.figure.delaxes(a) # may have been added by previous plots
	axes = self.figure.gca()
        axes.cla()

        self.plotter.plot(ax=axes)
        self.figure.autofmt_xdate()  # Better date formatting, including tick label rotation
        if self.legendcheck.IsChecked():
	    axes.legend(loc='best')

        self.canvas.draw()
        
    def show_data(self):
	df    = self.plotter.dataframe()
	text  = df.to_string(float_format=lambda x: '%.15g' % (x))
        db    = data_box(self, text)
        db.Show()
    
    def OnLegendCheck(self, event):
	axes = self.figure.axes[0]
	if self.legendcheck.IsChecked():  axes.legend(loc='best')
	else:                             axes.legend_ = None
	self.canvas.draw()
        
    def OnQuit(self, event):
	self.Close()
	sys.exit(0)
	
    def OnKeyDown(self, event):
	keycode = event.GetKeyCode()
        if keycode == wx.WXK_F9:
	    self.OnPlotEvent(event)
	elif keycode == wx.WXK_F5:
	    curPos  = self.textfield.GetInsertionPoint()
	    lineNum = self.textfield.PositionToXY(curPos)[1]
	    lines   = self.textfield.GetValue().split('\n')
	    lines.insert(lineNum+1, lines[lineNum])
	    self.textfield.SetValue('\n'.join(lines))
	    self.textfield.SetInsertionPoint(curPos)
	elif keycode in [ ord('H'), ord('T'), ord('B') ] and event.GetModifiers() == wx.MOD_CONTROL:
	    curPos  = self.textfield.GetInsertionPoint()
	    lineNum = self.textfield.PositionToXY(curPos)[1]
	    lines   = self.textfield.GetValue().split('\n')
	    line    = lines[lineNum]
	    
	    magicchar = { ord('H') : '*', ord('B') : '#', ord('T') : '>' }[ keycode ]
	    flagmatch = re.match('([#*>]*)(.*)$', line)
	    
	    flagdict   = { c:c in flagmatch.group(1) for c in '#*>'}
	    newflagdict = flagdict
	    newflagdict[magicchar] = not flagdict[magicchar]
	    
	    newflags    = ''.join(filter(lambda c: newflagdict[c],'#*>'))
	    lines[lineNum]  = newflags + flagmatch.group(2)
	    
	    self.textfield.SetValue('\n'.join(lines))
	    self.textfield.SetInsertionPoint(curPos + len(newflags) - len(flagmatch.group(1)))
	# Ctrl-L = toggle Legend, perhaps change to menu item instead
	elif keycode == ord('L') and event.GetModifiers() == wx.MOD_CONTROL:
	    self.legendcheck.SetValue(not self.legendcheck.GetValue())
	    self.OnLegendCheck(event)
	elif keycode == ord('U') and event.GetModifiers() == wx.MOD_CONTROL:
	    self.show_data()
	else:
	    event.Skip()
	    
    def OnOpenFile(self, event):
        #file_name = os.path.basename(self.last_name_saved)
        #if self.modify:
            #dlg = wx.MessageDialog(self, 'Save changes?', '', wx.YES_NO | wx.YES_DEFAULT | wx.CANCEL |
                        #wx.ICON_QUESTION)
            #val = dlg.ShowModal()
            #if val == wx.ID_YES:
                #self.OnSaveFile(event)
                #self.DoOpenFile()
            #elif val == wx.ID_CANCEL:
                #dlg.Destroy()
            #else:
                #self.DoOpenFile()
        #else:
        if True:
            self.DoOpenFile()

    def DoOpenFile(self):
        wcd = 'All files (*)|*|TSPlot files (*.plt)|*.plt|'
        dir = os.path.expanduser("~/tsplot/plots")
        open_dlg = wx.FileDialog(self, message='Choose a file', defaultDir=dir, defaultFile='',
                        wildcard=wcd, style=wx.OPEN|wx.CHANGE_DIR)
        if open_dlg.ShowModal() == wx.ID_OK:
            path = open_dlg.GetPath()

            try:
                file = open(path, 'r')
                text = file.read()
                file.close()
                if self.textfield.GetLastPosition():
                    self.textfield.Clear()
                self.textfield.WriteText(text)
                self.last_name_saved = path
                #self.statusbar.SetStatusText('', 1)
                #self.modify = False

            except IOError, error:
                dlg = wx.MessageDialog(self, 'Error opening file\n' + str(error))
                dlg.ShowModal()

            except UnicodeDecodeError, error:
                dlg = wx.MessageDialog(self, 'Error opening file\n' + str(error))
                dlg.ShowModal()

        open_dlg.Destroy()

    def OnSaveFile(self, event):
        if self.last_name_saved:

            try:
                file = open(self.last_name_saved, 'w')
                text = self.textfield.GetValue()
                file.write(text)
                file.close()
                #self.statusbar.SetStatusText(os.path.basename(self.last_name_saved) + ' saved', 0)
                #self.modify = False
                #self.statusbar.SetStatusText('', 1)

            except IOError, error:
                dlg = wx.MessageDialog(self, 'Error saving file\n' + str(error))
                dlg.ShowModal()
        else:
            self.OnSaveAsFile(event)

    def OnSaveAsFile(self, event):
        wcd='All files(*)|*|TSPlot files (*.plt)|*.plt|'
        dir = os.path.expanduser("~/tsplot/plots")
        save_dlg = wx.FileDialog(self, message='Save file as...', defaultDir=dir, defaultFile='',
                        wildcard=wcd, style=wx.SAVE | wx.OVERWRITE_PROMPT)
        if save_dlg.ShowModal() == wx.ID_OK:
            path = save_dlg.GetPath()

            try:
                file = open(path, 'w')
                text = self.textfield.GetValue()
                file.write(text)
                file.close()
                self.last_name_saved = os.path.basename(path)
                #self.statusbar.SetStatusText(self.last_name_saved + ' saved', 0)
                #self.modify = False
                #self.statusbar.SetStatusText('', 1)

            except IOError, error:
                dlg = wx.MessageDialog(self, 'Error saving file\n' + str(error))
                dlg.ShowModal()
        save_dlg.Destroy()

'''
Used to display raw data as monospaced text
'''
class data_box(wx.Dialog):
    def __init__(self, parent, disclaimer_text):
        wx.Dialog.__init__(self, parent,style=wx.RESIZE_BORDER )
        text = wx.TextCtrl(self, -1, disclaimer_text, style=wx.TE_MULTILINE | wx.TE_READONLY | wx.EXPAND| wx.TE_DONTWRAP )
        
        font1 = wx.Font(8, wx.MODERN, wx.NORMAL, wx.NORMAL, False)
	text.SetFont(font1)
        
        sizer = wx.BoxSizer(wx.VERTICAL)
        
        btnsizer = wx.BoxSizer()

        btn = wx.Button(self, wx.ID_OK)
        btnsizer.Add(btn, 0, wx.ALL, 5)
        btnsizer.Add((5,-1), 0, wx.ALL, 5)

        sizer.Add(text, 1, wx.EXPAND|wx.ALL, 5)    
        sizer.Add(btnsizer, 0, wx.ALIGN_CENTER_VERTICAL|wx.ALL, 5)   
        self.SetSizerAndFit(sizer)
        self.SetSize((640,480))   # bigger than minimum to start
        
        text.Bind(wx.EVT_KEY_DOWN, self.OnKeyDown)
        text.SetFocus()
        
    def OnKeyDown(self, event):
	keycode = event.GetKeyCode()
        if keycode == wx.WXK_ESCAPE:
	    self.Destroy()
	else:
	    event.Skip()
	    
if __name__ == "__main__":
    app = wx.App()
    frame = simpleapp_wx(None,-1,'TSPlot')
    app.MainLoop()
