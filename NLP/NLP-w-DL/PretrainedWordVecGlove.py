from __future__ import print_function, division
from builtins import range 

import numpy as np 
from sklearn.metrics.pairwise import pairwise_distances 

def euclid_dist(a,b):
    return np.linalg.norm(a-b) # eclideian?
def cos_dist2(a,b):
    return 1-a.dot(b) / (np.linalg.norm(a) * np.linalg.norm(b))

# pick a distance type 
dist, metric = cos_dist2, 'cosine'

## find analogies - Naive/Intuitive implementation
# def find_analogies(w1,w2,w3):
#     for w in (w1,w2,w3):
#         print("%s not in dictionary" % w)
#         return None 

#     # arbitrary named to make sense
#     king = word2vec[w1] 
#     man = word2vec[w2]
#     woman = word2vec[w3]
#     v0 = king - man + woman 

#     min_dist = float("inf")
#     best_word = ""

#     for word, v1 in word2vec.items():
#         if word not in (w1,w2,w3):
#             d = dist(v0,v1)
#             if d < min_dist:
#                 min_dist = d 
#                 best_word = word 
#     print(f"{w1} - {w2} = {best_word} - {w3}")

def find_analogies(w1,w2,w3):
    for w in (w1,w2,w3):
        if w not in word2vec:
            print("%s not in dictionary" % w)
            return None 
    
    king = word2vec[w1]
    man = word2vec[w2]
    woman = word2vec[w3]
    v0 = king - man + woman 

    distances = pairwise_distances(v0.reshape(1,D), embedding, metric=metric).reshape(V) # pairwise function will iterate over table anyway... basically same implementation as above
    idx = distances.argmin()
    best_word = idx2word[idx]
    print(f"{w1} - {w2} = {best_word} - {w3}")

def nearest_neighbors(w,n=5):
    if w not in word2vec:
        print("%s not in dictionary" % w)
        return None 
    
    v = word2vec[w]
    print(f"{v.shape} original vector shape")
    print(f"{v.reshape(1,D).shape} new vector shape")
    print(f"{embedding.shape} embedding shape")
    distances = pairwise_distances(v.reshape(1,D),embedding, metric=metric).reshape(V)
    idxs = distances.argsort()[1:n+1]
    print("neighbors of: %s" % w)
    for idx in idxs:
        print("\t%s" % idx2word[idx])


