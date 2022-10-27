from importlib.metadata import requires
from marshmallow import fields, validates
from tools.base_schema import BaseDataSchema


class FavorMovie(BaseDataSchema):
    movie_id = fields.Int(required=True) # need to check the valid of the movie_id
    user_id = fields.Int(required=True) # need to check the valid of the user_id
