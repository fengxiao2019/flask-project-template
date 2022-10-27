from flask import Blueprint, request
from project.applibs.account.schema.favor import FavorMovie
from project import app, error_response, json_response 
from project.applibs.account.services.favor_movie import FavorMovies


rb = Blueprint('user', __name__, url_prefix='/movies/')


@rb.route('/<user_id>', methods=['GET'])
def get_favoriate_movies(user_id):
    """get favoriate moives

    Args:
        user_id (int): the user' id
    """
    query_data = FavorMovies(user_id).get_movies()
    return json_response(data=query_data)


@rb.route('/favor',methods=['POST'])
def add_favoriate_movie():
    """ add user favoriate movie
    """
    schema_obj = FavorMovie().load(request.json)
    if schema_obj.errors:
        return error_response(data=schema_obj.err_text)
    storage = schema_obj.storage
    res = FavorMovies(storage.user_id).add_favor_movie(storage.movie_id)
    print(res)
    return json_response()


@rb.route('/favor',methods=['DELETE'])
def delete_favoriate_movie():
    """delete user favoriate movie
    """
    schema_obj = FavorMovie().load(request.json)
    if schema_obj.errors:
        return error_response(data=schema_obj.err_text)
    storage = schema_obj.storage
    res = FavorMovies(storage.user_id).delete_favor_movie(storage.movie_id)
    return json_response()
