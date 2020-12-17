from mrjob.job import MRJob
from mrjob.protocol import TextProtocol
import re

WORD_RE = re.compile(r"\w+")

class MRWordUpper(MRJob):
    OUTPUT_PROTOCOL = TextProtocol
    #получаем слово и начинается ли оно с большой буквы
    def mapper(self, _, line):
        for word in WORD_RE.findall(line):
            yield word.lower(), word[0].isupper()
            
    #Cчиатем общее число слов и число слов с большой буквы    
    def reducer(self, word, upper):
        count = sum(x + 1j for x in upper)
        if count.real > int(count.imag) / 2 and int(count.imag) > 10:
            yield str(count.real), word 
            
if __name__ == '__main__':    
    MRWordUpper.run()  