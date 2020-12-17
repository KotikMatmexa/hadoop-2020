from mrjob.job import MRJob
import re

WORD_RE = re.compile(r"\w+")

class MRWordLongest(MRJob):
    #подсчет длины слов, с ключом - длиной, значением - словом
    def mapper(self, _, line):
        for word in WORD_RE.findall(line):           
            yield len(word), word
    
    #создание пар длина слова - все слова, имеющие такую длину 
    def combiner(self, word_len, words):
        yield None, (word_len, ','.join(words))
        
    #получение максимального элемента
    def reducer(self, _, words_lens):
        yield max(words_lens)
        
if __name__ == '__main__':    
    MRWordLongest.run()