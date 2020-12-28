from flask import Flask, render_template, flash
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

# If predicted score is higher than threshold - reject the request
THRESHOLD_SCORE = 0.1


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
    form = PhoneForm()
    if form.validate_on_submit():
        phone_data = form.phone.data
        user = User.query.filter_by(phone=User.phone_to_integer(phone_data)).first()
        if not user or user.calculated_score is None:
            flash(message=f"There are no transaction history for user: {phone_data}", category="warning")
            score = None
        else:
            score = user.calculated_score
        return render_template(
            "index.html",
            title="Score check",
            form=form,
            score=score,
            threshold=THRESHOLD_SCORE
        )
    return render_template(
        "index.html",
        title="Score check",
        form=form,
        threshold=THRESHOLD_SCORE
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
