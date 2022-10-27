from tools.db_tool.orm import DORM
from tools.tablenames import Tablenames
from tools.exception import StandardError
from tools.time_tool import get_current_time


class FavorMovies:
    def __init__(self, user_id):
        self.user_id = user_id

    def get_movies(self):
        query_str = "movie.movie_id, movie.title"
        query_obj = DORM(Tablenames.favor, table_alias='favor').query(query_str)\
            .where(user_id=self.user_id, valid=1)\
            .join(Tablenames.movie, table_alias='movie', 
                    join_on='favor.movie_id=movie.movie_id',
                    query_where_condition_dict={'valid': 1})\
            .order_by('favor.add_time desc')
        ans = query_obj.dict_query()
        return ans
    def _get_movie_data_by_id(self, movie_id):
        query_obj = DORM(Tablenames.movie, table_alias='movie')\
            .query('movie_id')\
                .where(movie_id=movie_id)\
                .dict_query()
        return query_obj[0]
        
    def add_favor_movie(self, movie_id):
        favor_data = {'user_id': self.user_id, 'movie_id': movie_id}
        # query movie related data
        query_obj = DORM(Tablenames.favor, **favor_data).query('*').dict_query()
        if query_obj:
            favor_data['add_time'] = get_current_time()
            favor_data['valid'] = 1
            favor_data['id'] = query_obj[0]['id']

        res = DORM(Tablenames.favor, **favor_data).save()
        res.execute()
        return res
    
    def delete_favor_movie(self, movie_id):
        query_obj = DORM(Tablenames.favor, user_id=self.user_id, movie_id=movie_id).update(valid=0)
        query_ans = query_obj.execute()
        return query_ans