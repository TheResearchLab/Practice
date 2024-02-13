#%% 

# CHARACTER ISSUES

# encoding and decoding
s = 'café'
s_encoded = s.encode('utf8')
s_decoded = s_encoded.decode('utf8')
s_decoded

# BYTE ESSENTIALS
cafe = bytes('café',encoding='utf_8')
cafe[:1]
cafe_arr = bytearray(cafe)
cafe_arr[-1:]

# %%
