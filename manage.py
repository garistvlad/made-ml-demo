from main import app, db, User, Score, migrate


@app.shell_context_processor
def make_shell_context():
    return dict(
        app=app,
        db=db,
        User=User,
        Score=Score,
        migrate=migrate,
    )
