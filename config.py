class Config:
    pass


class ProductionConfig(Config):
    pass


class DevelopmentConfig(Config):
    DEBUG = True
