from mrjob.job import MRJob
from mrjob.step import MRStep


class CountMovieRatings(MRJob):
    # these are the steps for mrstep to take.
    def steps(self):
        # Here we are setting all the steps for MRJob
        # We transform the output from the file given in the command to get all movie IDs and the ratings.
        # Next, we calculate the ratings each movie has, and combine it with it's corresponding movie ID.
        # Then we reduce the movies and sort them.
        return [
            MRStep(mapper=self.mapper_get_movies,
                   reducer=self.reducer_count_ratings),
            MRStep(reducer=self.reducer_sort_movies_by_ratings)
        ]

    # Get the movies
    def mapper_get_movies(self, _, line):
        # Here we split the rows from each other
        (_, movie_id, _, _) = line.split('\t')
        # We 'return' the movie ID as the key value
        yield movie_id, 1

    # Calculate the sum of the ratings
    def reducer_count_ratings(self, movie_id, ratings):
        # We count al ratings every movie has
        yield None, (sum(ratings), movie_id)

    # Sort the movies by the ratings
    def reducer_sort_movies_by_ratings(self, _, movies):
        # Loop through each movie and print the movie ID with the number of ratings
        for count, movie_id in sorted(movies):
            yield (int(movie_id), int(count))


# Run the code
if __name__ == '__main__':
    CountMovieRatings.run()
