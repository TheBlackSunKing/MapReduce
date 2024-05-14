from mrjob.job import MRJob
from mrjob.job import MRStep
class Word_Count(MRJob):
    def mapper(self, _, line):
        for word in line.split():
            word.lower()
            yield word, 1

    def reducer(self, key, values):
        yield key, sum(values)
    def steps(self):

        return [
            MRStep(
                mapper = self.mapper,
                reducer= self.reducer
            )
        ]

if __name__ == "__main__":
    Word_Count.run()





    # python main.py Computerprogramming.txt
