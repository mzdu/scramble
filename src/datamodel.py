#Database models used in Scramble

from google.appengine.ext import db

class Counter(db.Model):
    name = db.StringProperty()
    count = db.IntegerProperty()

