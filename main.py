from flask import Flask, render_template, flash, redirect, url_for
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate
from flask_wtf import FlaskForm as Form
from wtforms import StringField
from wtforms.validators import DataRequired

from config import DevelopmentConfig


app = Flask(__name__)
app.config.from_object(DevelopmentConfig)

db = SQLAlchemy(app)
migrate = Migrate(app, db)


class Score(db.Model):
    """Predicted score for users"""
    score = db.Column(db.Float)
    user_id = db.Column(db.Integer, db.ForeignKey("user.id"), primary_key=True)

    def __repr__(self):
        return f"Score: {self.score} Phone: {self.user.phone}"


class User(db.Model):
    """User features"""
    id = db.Column(db.Integer, primary_key=True)
    phone = db.Column(db.Integer, unique=True, nullable=False)
    score = db.relationship('Score', backref='user')
    # TODO: add other features for ML model

    @property
    def calculated_score(self):
        if len(self.score) == 0:
            return
        return self.score[0].score

    def __init__(self, phone: str):
        self.phone = self.phone_to_integer(phone)

    @staticmethod
    def phone_to_integer(phone: str):
        clean_phone = phone \
            .translate(str.maketrans("+-()", "    ")) \
            .replace(" ", "")
        return int(clean_phone)

    def __repr__(self):
        return f"<User: {self.phone}>"


class PhoneForm(Form):
    phone = StringField("Phone", validators=[DataRequired()])


@app.route("/", methods=["GET", "POST"])
def home():
    # TODO: List of categories: https://getbootstrap.com/docs/4.0/components/alerts/
    form = PhoneForm()
    if form.validate_on_submit():
        phone_data = form.phone.data
        user = User.query.filter_by(phone=User.phone_to_integer(phone_data)).first()
        if not user or user.calculated_score is None:
            flash(message=f"There are no transaction history for user: {phone_data}", category="warning")
            score=None
        else:
            score = user.calculated_score
        return render_template("index.html", title="Score check", form=form, score=score)
    return render_template("index.html", title="Score check", form=form)


if __name__ == "__main__":
    app.run()
