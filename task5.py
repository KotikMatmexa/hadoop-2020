from mrjob.job import MRJob
from mrjob.protocol import TextProtocol
import re

WORD_RE = re.compile(r'(?: [A-Za-zА-Яа-я][а-яa-z]\. )')
class MRWordAbr(MRJob):
    OUTPUT_PROTOCOL = TextProtocol 
    #получить абревиатуры в строке
    def mapper(self, _, line):
        for match in WORD_RE.findall(line):
            yield match.lower(), 1
    #полуичть число вхождений                   
    def combiner(self, word, counts):
        yield None, (sum(counts), word)
    #
    def reducer(self, _, word_count_pairs):
        pairs = list(word_count_pairs) 
        for counts, word in pairs:
            word_freq = counts/len(pairs) #проверка в какой доле документов встречается слово
            if word_freq > 0.02:   #проверка на пороговое значение (выбрано по принципу вхождения распространенных абревиатур
                yield word, str(word_freq)
        

if __name__ == "__main__":
    MRWordAbr.run() 
    