from aqandu import app

# This is only used when running locally. When running live, gunicorn runs
# the application.
if __name__ == '__main__':
    import os
    os.environ['FLASK_ENV'] = 'development'
    app.run(host='127.0.0.1', port=8081, debug=True)
