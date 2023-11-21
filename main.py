from fastapi import FastAPI, HTTPException
from sqlalchemy import create_engine, Column, Integer, String, Text, Sequence, JSON, VARCHAR, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime
from sqlalchemy.orm import registry
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
    year = Column(Integer)
    rated = Column(String(50))
    released = Column(String(50))
    runtime = Column(String(50))
    plot = Column(Text)
    language = Column(String(255))
    country = Column(String(255))
    awards = Column(String(255))
    poster = Column(String(255))
    dvd = Column(String(50))
    imdb_id = Column(String(50))
    type = Column(String(50))
    ratings = Column(String(50))


class Artist(Base):
    __tablename__ = "artists"
    id = Column(Integer, Sequence("artist_id_seq"), primary_key=True, index=True)
    movie_id = Column(Integer)
    name = Column(String(255))
    type = Column(VARCHAR(50))


class Genre(Base):
    __tablename__ = "genres"
    id = Column(Integer, Sequence("genre_id_seq"), primary_key=True, index=True)
    movie_id = Column(Integer)
    type = Column(String(255))


Base.metadata.create_all(bind=engine)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


@app.post("/add_movie")
def add_movie(movie_data: dict):
    try:
        date_str = '20 Oct 2006'
        date_object = datetime.strptime(date_str, '%d %b %Y')
        formatted_date = date_object.strftime('%Y-%m-%d')

        genre_names = movie_data.get("Genre", "").split(", ")
        genres = []

        db = SessionLocal()

        for genre_name in genre_names:
            genre = db.query(Genre).filter(Genre.name == genre_name).first()
            if not genre:
                genre = Genre(name=genre_name)
                db.add(genre)
            genres.append(genre)

        # writer = get_or_create_artist(db, movie_data["Writer"])
        # actors = [get_or_create_artist(db, actor) for actor in movie_data["Actors"].split(", ")]

        db_movie = Movie(
            title=movie_data["Title"],
            year=movie_data["Year"],
            rated=movie_data["Rated"],
            released=formatted_date,
            runtime=movie_data["Runtime"],
            plot=movie_data["Plot"],
            language=movie_data["Language"],
            country=movie_data["Country"],
            awards=movie_data["Awards"],
            poster=movie_data["Poster"],
            dvd=movie_data["DVD"],
            imdb_id=movie_data["imdbID"],
            ratings=movie_data["Ratings"],
            genres=genres
        )
        db_movie.writer_artist = writer
        db_movie.actors_artists = actors

        db.add(db_movie)
        db.commit()
        db.refresh(db_movie)
        db.close()
        return {"message": "Movie added successfully"}
    except SQLAlchemyError as e:
        return HTTPException(status_code=500, detail=f"Database error: {str(e)}")


def get_or_create_artist(session, artist_names, movie_id, type):
    artists = artist_names.split(", ")
    for artist in artists:
        artist = Artist(name=artist, movie_id=movie_id, type=type)
        session.add(artist)
        session.commit()

@app.get("/get_movie_info")
def get_movie_info(title: str):
    omdb_url = f"http://www.omdbapi.com/?apikey={OMDB_API_KEY}&t={title}"

    omdb_response = requests.get(omdb_url)

    if omdb_response.status_code == 200:
        omdb_data = json.loads(omdb_response.text)
        try:
            db = SessionLocal()
            date_object = datetime.strptime(omdb_data["Released"], '%d %b %Y')
            formatted_date = date_object.strftime('%Y-%m-%d')

            ratings = omdb_data.get("Ratings", [])
            first_rating = ""
            if ratings:
                first_rating = ratings[0]["Value"]

            db_movie = Movie(
                title=omdb_data["Title"],
                year=omdb_data["Year"],
                rated=omdb_data["Rated"],
                released=formatted_date,
                runtime=omdb_data["Runtime"],
                plot=omdb_data["Plot"],
                language=omdb_data["Language"],
                country=omdb_data["Country"],
                awards=omdb_data["Awards"],
                poster=omdb_data["Poster"],
                dvd=omdb_data.get("DVD"),
                imdb_id=omdb_data.get("imdbID"),
                type=omdb_data.get("Type"),
                ratings=first_rating
            )

            db.add(db_movie)
            db.commit()
            db.refresh(db_movie)
            db.close()

            genre_names = omdb_data.get("Genre", "").split(", ")
            for genre_name in genre_names:
                genre = Genre(movie_id=db_movie.id,
                              type=genre_name)
                db.add(genre)
                db.commit()

            get_or_create_artist(db, omdb_data["Director"], db_movie.id, "Director")
            get_or_create_artist(db, omdb_data["Writer"], db_movie.id, "Writer")
            get_or_create_artist(db, omdb_data["Actors"], db_movie.id, "Actors")

            return {"omdb_data": omdb_data}
        except SQLAlchemyError as e:
            return HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    else:
        raise HTTPException(status_code=omdb_response.status_code, detail=omdb_response.text)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=9000)
    registry.configure()
