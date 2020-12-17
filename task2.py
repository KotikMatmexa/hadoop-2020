from mrjob.job import MRJob
from statistics import mean
import re

WORD_RE = re.compile(r"\w+")

class MRWordMean(MRJob):
    #получить длину слов
    def mapper(self, _, line):
        for word in WORD_RE.findall(line):           
            yield None, len(word)
    
    #получить среднее
    def reducer(self, _, lens):
        yield None, mean(lens)

if __name__ == '__main__':    
    MRWordMean.run() 
    