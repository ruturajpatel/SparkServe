#!venv/bin/python
from views import app
import config

if __name__ == '__main__':
    app.run(debug=True, host=config.host)
