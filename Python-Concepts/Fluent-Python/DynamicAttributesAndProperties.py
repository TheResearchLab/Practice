""" Intro to osconfeed dynamic attributes examples"""
# import json 
# with open('python-concepts/fluent-python/osconfeed.json','r') as fp:
#     feed = json.load(fp)
# #sorted(feed['Schedule'].keys())

# for key, value in sorted(feed['Schedule'].items()):
#     print(f'{len(value):3} {key}')

# print(feed['Schedule']['speakers'][-1]['name'])
# print(feed['Schedule']['speakers'][-1]['serial'])
# print(feed['Schedule']['events'][40]['name'])
# print(feed['Schedule']['events'][40]['speakers'])

""" FrozenJSON to use like Javascript """
# import json
# from collections import abc
# import keyword 

# class FrozenJSON:
#     """A read-only façade for navigating a JSON-like object
#     using attribute notation
#     """    
#     def __init__(self,mapping):
#         self.__data = {}
#         for key, value in mapping.items():
#             if keyword.iskeyword(key):
#                 key+='_'


#     def __getattr__(self,name):
#         try:
#             return getattr(self.__data,name)
#         except AttributeError:
#             return FrozenJSON.build(self.__data[name])
    
#     def __dir__(self):
#         return self.__data.keys()
    
#     @classmethod 
#     def build(cls, obj):
#         if isinstance(obj, abc.Mapping):
#             return cls(obj)
#         elif isinstance(obj, abc.MutableSequence):
#             return [cls.build(item) for item in obj]
#         else:
#             return obj 
        


# raw_feed = json.load(open('python-concepts/fluent-python/osconfeed.json'))
# feed = FrozenJSON(raw_feed)
# len(feed.Schedule.speakers)

# print(feed.keys())
# print(sorted(feed.Schedule.keys()))

# for key, value in sorted(feed.Schedule.items()):
#     print(f'{len(value):3} {key}')

# print(feed.Schedule.speakers[-1].name)
# talk = feed.Schedule.events[40]
# print(type(talk))
# print(talk.speakers)
# print(talk.flavor)

""" Incorporating the New Method """
# from collections import abc 
# import keyword 

# class FrozenJSON:
#     def __new__(cls,arg):
#         if isinstance(arg,abc.Mapping):
#             return super().__new__(cls)
#         elif isinstance(arg, abc.MutableSequence):
#             return [cls(item) for item in arg]
#         else:
#             return arg 

#     def __init__(self,mapping):
#         self.__data = {}
#         for key, value in mapping.items():
#             if keyword.iskeyword(key):
#                 key+='_'
#             self.__data[key] = value 

#     def __getattr__(self, name):
#         try:
#             return getattr(self.__data,name)
#         except AttributeError:
#             return FrozenJSON(self.__data[name])
    
#     def __dir__(self):
#         return self.__data.keys()

"""Making Event Objects for Easier Retrieval"""
import inspect
import json 
from functools import cached_property, cache

JSON_PATH = r'python-concepts/fluent-python/osconfeed.json'

class Record:
    __index = None
    
    def __init__(self,**kwargs):
        self.__dict__.update(kwargs)

    def __repr__(self):
        return f'<{self.__class__.__name__} serial={self.serial!r}>'

    @staticmethod 
    def fetch(key):
        if Record.__index is None:
            Record.__index = load()
            return Record.__index[key] 
    
def load(path=JSON_PATH):
    records = {}
    with open(path) as fp:
        raw_data = json.load(fp)
    for collection, raw_records in raw_data['Schedule'].items():
        record_type = collection[:-1]
        for raw_record in raw_records:
            key = f'{record_type}.{raw_record["serial"]}'
            records[key] = Record(**raw_record)
    return records

    
records = load(JSON_PATH)
speaker = records['speaker.3471']
print(speaker)
print(speaker.name, speaker.twitter)

event = Record.fetch('event.33950')
print(event)
print(event.venue_serial)

class Event(Record):

    def __init__(self, **kwargs):
        self.__speaker_objs = None 
        super().__init__(**kwargs)
    
    def __repr__(self):
        try:
            return f'<{self.__class__.__name__} {self.name!r}'
        except AttributeError:
            return super().__repr__()

    @property
    @cache
    def venue(self):
        key = f'venue.{self.venue_serial}'
        return self.__class__.fetch(key) # use class method to retrieve fetch because if fetch was an attr of event self.fetch would retrieve that

    @property
    def speakers(self):
        if self.__speaker_objs is None:
            spkr_serials = self.__dict__['speakers']
            fetch = self.__class__.fetch 
            self.__speaker_objs = [fetch(f'speaker.{key}')
                                    for key in spkr_serials]
        return self.__speaker_objs

def load(path=JSON_PATH):
    records = {}
    with open(path) as fp:
        raw_data = json.load(fp)
    for collection, raw_records in raw_data['Schedule'].items():
        record_type = collection[:-1]
        cls_name = record_type.capitalize()
        cls = globals().get(cls_name, Record)
        if inspect.isclass(cls) and issubclass(cls,Record):
            factory = cls 
        else:
            factory = Record 
        for raw_record in raw_records:
            key = f'{record_type}.{raw_record["serial"]}'
            records[key] = factory(**raw_record)
    return records 


    