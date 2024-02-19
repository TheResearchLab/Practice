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
s1 == s2 #not equal, strange because they are the same word

from unicodedata import normalize,name
s1 = 'café'
s2 = 'cafe\N{COMBINING ACUTE ACCENT}'
len(s1),len(s2)

len(normalize('NFC',s1)),len(normalize('NFC',s2)) # these are equal length now
normalize('NFC',s1) == normalize('NFC',s2) # these are now equal
normalize('NFD',s1) == normalize('NFD',s2)

# ohm normalized into a single greek upper case omega
ohm = '\u2126'
name(ohm) #OHM SIGN
ohm_c = normalize('NFC',ohm)
name(ohm_c) # normalized to single character
ohm_c,ohm
ohm == ohm_c
normalize('NFC',ohm) == ohm_c # now equal

# NFKC example

half = '\N{VULGAR FRACTION ONE HALF}'
print(half)
normalize('NFKC',half)

for char in normalize('NFKC',half):
    print(char,name(char), sep='\t')

four_squared =  '4²'
normalize('NFKC',four_squared)

micro =  'µ' #compatibility characters
micro_kc = normalize('NFKC',micro)
ord(micro),ord(micro_kc)
name(micro),name(micro_kc)

# CASE FOLDING 
micro = 'µ'
micro_cf = micro.casefold()
name(micro),name(micro_cf)

eszett =  'ß'
eszett_cf = eszett.casefold() # converts unicode to string
name(eszett)
eszett,eszett_cf


# UTILITY FUNCTIONS FOR NORMALIZED TEXT MATCHING

def nfc_equal(str1,str2):
    return normalize('NFC',str1) == normalize('NFC',str2)

def fold_equal(str1,str2):
    return (normalize('NFC',str1).casefold() == 
                normalize('NFC',str2.casefold()))

nfc_equal(s1,s2)
fold_equal(s1,s2)

# EXTREME "NORMALIZATION": TAKING OUT DIACRITICS
from unicodedata import combining


def shave_marks(txt):
    norm_txt = normalize('NFD',txt)
    shaved = ''.join(c for c in norm_txt 
                     if not combining(c))
    return normalize('NFC',shaved)

s2,shave_marks(s2)

# SORTING UNICODE TEXT
fruits = ['caju', 'atemoia', 'cajá', 'açaí', 'acerola']
sorted(fruits)

import locale 
my_locale = locale.setlocale(locale.LC_COLLATE,'pt_BR.UTF-8')
print(my_locale)
sorted_fruits = sorted(fruits,key = locale.strxfrm)
print(sorted_fruits) #properly sorts ignoring accents



# %%
