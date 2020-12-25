from flask import Flask

from config import DevelopmentConfig


app = Flask(__name__)
app.config.from_object(DevelopmentConfig)


@app.route("/")
def home():
    return "<h1>MADE > ML > HA #4</h1>"


if __name__ == "__main__":
    app.run()
