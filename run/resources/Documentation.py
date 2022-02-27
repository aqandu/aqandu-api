from flask_restful import Resource
from flask import render_template, make_response

class Documentation(Resource):
    def get(self, **kwargs):
        headers = {'Content-Type': 'text/html'}
        return make_response(
            render_template('api_docs.html'),
            200,
            headers
        )