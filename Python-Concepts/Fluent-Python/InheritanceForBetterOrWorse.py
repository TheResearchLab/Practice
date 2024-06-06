#%%

class DoppleDict(dict):
    def __setitem__(self,key,value):
        super().__setitem__(key,[value] *2)

dd = DoppleDict(one=1)
dd
dd['two'] = 2
dd
dd.update(three=3)
dd

class AnswerDict(dict):
    def __getitem__(self,key):
        return 42
    
ad = AnswerDict(a='foo')
ad['a']
d = {}
d.update(ad)
d['a']
d
# %%
