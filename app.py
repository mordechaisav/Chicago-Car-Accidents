from flask import Flask
from blueprints.accidents_bp import accidents_bp
app = Flask(__name__)
app.register_blueprint(accidents_bp)


if __name__ == '__main__':
    app.run()
