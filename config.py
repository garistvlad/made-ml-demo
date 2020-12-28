import os


class Config:
    pass


class ProductionConfig(Config):
    SECRET_KEY = os.environ.get("SECRET_KEY", "prod-extremely-secret-key")


class DevelopmentConfig(Config):
    DEBUG = True
    SQLALCHEMY_DATABASE_URI = "sqlite:///scoring.db"
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    SECRET_KEY = os.environ.get("SECRET_KEY", "dev-extremely-secret-key")
