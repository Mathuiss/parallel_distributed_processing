from mrjob.job import MRJob
from mrjob.job import MRStep


class Analysis (MRJob):
    def steps(self):
        return [MRStep(mapper=self.mapper_get_film_ids, reducer=self.reducer_count_ratings)]

    def mapper_get_film_ids(self, _, line):
        (user_id, film_id, rating, time) = line.split("\t")
        yield film_id, 1

    def reducer_count_ratings(self, film_id, num_ratings):
        yield film_id, sum(num_ratings)


if __name__ == "__main__":
    Analysis().run()
