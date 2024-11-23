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
import json
from collections import abc 

class FrozenJSON:
    """A read-only façade for navigating a JSON-like object
    using attribute notation
    """    
    def __init__(self,mapping):
        self.__data = dict(mapping)

    def __getattr__(self,name):
        try:
            return getattr(self.__data,name)
        except AttributeError:
            return FrozenJSON.build(self.__data[name])
    
    def __dir__(self):
        return self.__data.keys()
    
    @classmethod 
    def build(cls, obj):
        if isinstance(obj, abc.Mapping):
            return cls(obj)
        elif isinstance(obj, abc.MutableSequence):
            return [cls.build(item) for item in obj]
        else:
            return obj 
        


raw_feed = json.load(open('python-concepts/fluent-python/osconfeed.json'))
feed = FrozenJSON(raw_feed)
len(feed.Schedule.speakers)

print(feed.keys())
print(sorted(feed.Schedule.keys()))

for key, value in sorted(feed.Schedule.items()):
    print(f'{len(value):3} {key}')

print(feed.Schedule.speakers[-1].name)
talk = feed.Schedule.events[40]
print(type(talk))
print(talk.speakers)
print(talk.flavor)

