from flask.cli import FlaskGroup


from project import app, db

cli = FlaskGroup(app)


# 在引入create_all 前，需要先把model 引入
from project.applibs.account.models.user import User


@cli.command("create_db")
def create_db():
    db.drop_all()
    db.create_all()
    db.session.commit()


if __name__ == '__main__':
    cli()