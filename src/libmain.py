import jinja2
import os
import logging
import datamodel

################ Render a Page with Jinja2 Template #########################

jinja_environment = jinja2.Environment(loader = jinja2.FileSystemLoader(os.path.dirname(__file__) + '/templates'))

def doRender(handler, tname = 'index.html', values = {}):
    
    temp = jinja_environment.get_template(tname)
    handler.response.out.write(temp.render(values))
    return True

    
    
    
    
    
        