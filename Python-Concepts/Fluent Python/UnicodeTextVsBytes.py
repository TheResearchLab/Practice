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

#COPING WITH UnicodeDecodeError
octets = b'Montr\xe9al'
octets.decode('iso8859_7')
#octets.decode('utf_8') # given utf_8 the variable contains unexpected byte characters
octets.decode('utf_8',errors='replace')

# BOM: A USEFUL GREMLIN
u16 = 'El Niño'.encode('utf_16')
u16 # this is denoted with b'\xff\xfeE' for little-endian

list(u16) # 255 and 254 are the little-endian's encoding

u16le = 'El Niño'.encode('utf_16le')
list(u16le) # this is explicitly little-endina

u16be = 'El Niño'.encode('utf_16be')
list(u16be) # while le is 69 0 the be is 0 69

# NORMALIZING UNICODE FOR RELIABLE COMPARISONS

s1 = 'café'
s2 = 'cafe\N{COMBINING ACUTE ACCENT}'
s1,s2
len(s1),len(s2)# different character lengths
s1 == s2 #not equal
# %%
