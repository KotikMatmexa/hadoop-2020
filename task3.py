from mrjob.job import MRJob
from mrjob.step import MRStep
import re

WORD_RE = re.compile(r"[a-zA-Z']+")


class MRWordMostUsedENG(MRJob):
    #получить слова в строке
    def mapper_get_words(self, _, line):
        for word in WORD_RE.findall(line):
            yield (word.lower(), 1)
            
    #суммаризовать слова
    def combiner_count_words(self, word, counts):
        yield (word, sum(counts))

    #отправка всех пар в один reduce
    def reducer_count_words(self, word, counts):
        yield None, (sum(counts), word)

    #получить максимальное значение
    def reducer_find_max_word(self, _, word_count_pairs):
        yield max(word_count_pairs)

    #порядок применения операция
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_words,
                   combiner=self.combiner_count_words,
                   reducer=self.reducer_count_words),
            MRStep(reducer=self.reducer_find_max_word)
        ]


if __name__ == '__main__':
    MRWordMostUsedENG.run()
    #Результат
    #13024	"ru"