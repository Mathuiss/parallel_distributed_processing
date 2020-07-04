from mrjob.job import MRJob
from mrjob.job import MRStep


class Analysis (MRJob):
    MRJob.SORT_VALUES = True

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_film_ids, reducer=self.reducer_count_ratings),
            MRStep()
        ]

    def mapper_get_film_ids(self, _, line):
        (user_id, film_id, rating, time) = line.split("\t")
        yield film_id, 1

    def reducer_count_ratings(self, film_id, num_ratings):
        yield film_id, sum(num_ratings)

    def mapper_sort_ratings(self, film_id, total_ratings):
        yield ('%020d' % int(total_ratings), int(film_id))

    def reducer_sorted_ratings(self, total_rating, film_id):
        for id in film_id:
            yield str(id), int(total_rating)


if __name__ == "__main__":
    Analysis().run()
