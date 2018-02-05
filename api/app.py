from api import app

from api.resources.score import Score

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, debug=True)