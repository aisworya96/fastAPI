from fastapi import FastAPI, HTTPException
from sqlalchemy import create_engine, Column, Integer, String, JSON, Sequence
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import requests
import json
import mysql.connector

app = FastAPI()

OMDB_API_KEY = "3b45697e"

username = 'root'
password = 'root'
host = 'localhost'
database_name = 'movies_db'
port = '3306'


create_db_connection = mysql.connector.connect(
    host=host,
    user=username,
    password=password,
    port=port
)


create_db_cursor = create_db_connection.cursor()


create_database_query = f"CREATE DATABASE IF NOT EXISTS {database_name}"


create_db_cursor.execute(create_database_query)


create_db_connection.commit()


create_db_cursor.close()
create_db_connection.close()


SQLALCHEMY_DATABASE_URL = f"mysql+mysqlconnector://{username}:{password}@{host}:{port}/{database_name}"
engine = create_engine(SQLALCHEMY_DATABASE_URL)

Base = declarative_base()

class Movie(Base):
    __tablename__ = "movies"

    id = Column(Integer, Sequence("movie_id_seq"), primary_key=True, index=True)
    title = Column(String(255), index=True)
    omdb_response = Column(JSON)

Base.metadata.create_all(bind=engine)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

@app.get("/get_movie_info")
def get_movie_info(title: str):


    omdb_url = f"http://www.omdbapi.com/?apikey={OMDB_API_KEY}&t={title}"


    omdb_response = requests.get(omdb_url)

    if omdb_response.status_code == 200:
        omdb_data = json.loads(omdb_response.text)


        db_movie = Movie(title=title, omdb_response=omdb_data)
        db = SessionLocal()
        db.add(db_movie)
        db.commit()
        db.refresh(db_movie)


        stored_movie = db.query(Movie).filter(Movie.title == title).first()

        db.close()

        return {"omdb_data": omdb_data, "stored_movie_in_db": stored_movie.__dict__}
    else:
        raise HTTPException(status_code=omdb_response.status_code, detail=omdb_response.text)

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=9000)

