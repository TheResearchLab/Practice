#%%

class Vowels:
    def __getitem__(self,i):
        return 'AEIOU'[i]
    
v = Vowels()
v[4]

for i in v:
    print(i)

'E' in v # True
# %%
