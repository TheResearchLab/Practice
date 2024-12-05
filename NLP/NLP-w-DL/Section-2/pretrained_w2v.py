from __future__ import print_function, division 
from builtins import range 

from gensim.models import KeyedVectors # this library converts binary files into formats friendly to object with methods for calc nearest neighbors & similarity

word_vectors = KeyedVectors.load_word2vec_format(r'NLP/NLP-w-DL/preprocessed_data/googleNews/GoogleNews-vectors-negative300.bin',binary=True)

def find_analogies(w1,w2,w3):
    r = word_vectors.most_similar(positive=[w1,w3], negative=[w2])
    print("%s-%s = %s-%s" % (w1,w2,r[0][0],w3))

def nearest_neighbors(w):
    r = word_vectors.most_similar(positive=[w]) #same function as above w/ different params
    print("neighbors of: %s" % (w))
    for word, score in r:
        print("\t%s" % word)

# results may be case-sensitive
find_analogies('king','man','woman')
find_analogies('man','woman','sister')
find_analogies('father','son','mother')
nearest_neighbors('king')
nearest_neighbors('japan')

