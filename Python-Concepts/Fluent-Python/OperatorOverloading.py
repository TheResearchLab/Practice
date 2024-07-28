#%% 


principal = 4000 
rate = .08 
periods = 12 

interest = principal * ((1 + rate) ** periods - 1)
interest

# When x != +x 
# Change in context 
import decimal 
ctx = decimal.getcontext()
ctx.prec = 40 
one_third = decimal.Decimal('1') / decimal.Decimal('3')
display(one_third) 

one_third == +one_third
ctx.prec = 28 
print(one_third == +one_third) 
display(+one_third)
one_third

# counter example
from collections import Counter

ct = Counter('abracadabra')
ct
ct['b'] = -3 
ct['c'] = 0 
display(ct)
+ct # removes the non positive instances






# %%
