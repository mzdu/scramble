#main.py
#Scramble Project

from libmain import doRender

 
import webapp2
import logging


class MainPageHandler(webapp2.RequestHandler):
    def get(self):
        values = dict()
        values['testarea'] = "this is a test string"       
        values['css'] = ['/static/css/main.css']
        values['javascript'] = ['']
        
        doRender(self, 'index.html', values)

class AboutPageHandler(webapp2.RequestHandler):
    def get(self):
        values = dict()
        values['css'] = ['/static/css/main.css']
        values['javascript'] = ['']
        
        doRender(self, 'about.html', values)
        
class PieChartPageHandler(webapp2.RequestHandler):
    def get(self):
        values = dict()
        values['css'] = ['']
        values['javascript'] = ['']
        
        doRender(self, 'piechart.html', values)

class ExtractionPageHandler(webapp2.RequestHandler):
    def get(self):
        values = dict()
        values['css'] = ['']
        values['javascript'] = ['']
        
        doRender(self, 'extraction.html', values)
        
app = webapp2.WSGIApplication([
                                          ('/main', MainPageHandler),
                                          ('/about', AboutPageHandler),
                                          ('/piechart', PieChartPageHandler)
                                          ('/.*', MainPageHandler)
                                          ],debug = True)
    

