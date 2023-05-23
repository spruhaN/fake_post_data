import sqlalchemy
import os
import dotenv
from faker import Faker
import numpy as np

def database_connection_url():
    dotenv.load_dotenv()
    DB_USER: str = os.environ.get("POSTGRES_USER")
    DB_PASSWD = os.environ.get("POSTGRES_PASSWORD")
    DB_SERVER: str = os.environ.get("POSTGRES_SERVER")
    DB_PORT: str = os.environ.get("POSTGRES_PORT")
    DB_NAME: str = os.environ.get("POSTGRES_DB")
    return f"postgresql://{DB_USER}:{DB_PASSWD}@{DB_SERVER}:{DB_PORT}/{DB_NAME}"

# Create a new DB engine based on our connection string
engine = sqlalchemy.create_engine(database_connection_url(), use_insertmanyvalues=True)
categories = ['News', 'Sports', 'Politics', 'Entertainment']

with engine.begin() as conn:
    conn.execute(sqlalchemy.text("""
    DROP TABLE IF EXISTS likes;
    DROP TABLE IF EXISTS posts;
    DROP TABLE IF EXISTS users;
    DROP TABLE IF EXISTS category;

    CREATE TABLE 
        category (
            id int generated always as identity not null PRIMARY KEY,
            category_name text not null
        );

    CREATE TABLE
    users (
        id int generated always as identity not null PRIMARY KEY,
        username text not null,
        full_name text not null,
        birthday date not null
    );    
        
    CREATE TABLE
    posts (
        id int generated always as identity not null PRIMARY KEY,
        title text not null, 
        content text not null,
        created_at timestamp not null,
        visible boolean not null,
        poster_id int not null references users(id),
        category_id int  not null references category(id)
    );

    CREATE TABLE
    likes (
        user_id int references users(id),
        post_id int references posts(id),
        PRIMARY KEY (user_id, post_id),
        created_at timestamp not null
    );
    """))
    
    # populate initial posting categories
    for category in categories:    
        conn.execute(sqlalchemy.text("""
        INSERT INTO category (category_name) VALUES (:category_name);
        """), {"category_name": category})

num_users = 200000
fake = Faker()
posts_sample_distribution = np.random.default_rng().negative_binomial(0.04, 0.01, num_users)
category_sample_distribution = np.random.choice([1, 2, 3, 4], num_users, p=[0.1, 0.3, 0.1, 0.5])
total_posts = 0

# create fake posters with fake names and birthdays
with engine.begin() as conn:
    print("creating fake posters...")
    posts = []
    for i in range(num_users):
        if (i % 10 == 0):
            print(i)
        
        profile = fake.profile()

        poster_id = conn.execute(sqlalchemy.text("""
        INSERT INTO users (username, full_name, birthday) VALUES (:username, :name, :birthday) RETURNING id;
        """), {"username": profile['username'], "name": profile['name'], "birthday": profile['birthdate']}).scalar_one();

        num_posts = posts_sample_distribution[i]

        for _ in range(num_posts):
            total_posts += 1
            posts.append({
                "title": fake.sentence(),
                "content": fake.text(),
                "poster_id": poster_id,
                "category_id": category_sample_distribution[i].item(),
                "visible": fake.boolean(97),
                "created_at": fake.date_time_between(start_date='-5y', end_date='now', tzinfo=None)
            })

    if posts:
        conn.execute(sqlalchemy.text("""
        INSERT INTO posts (title, content, poster_id, category_id, visible, created_at) 
        VALUES (:title, :content, :poster_id, :category_id, :visible, :created_at);
        """), posts)

    max_post_id = conn.execute(sqlalchemy.text("""
    SELECT MAX(id) FROM posts;
    """
    )).scalar_one()

    print("total posts: ", total_posts)
    
    print("creating fake likes...")
    # create fake likes
    likes = []
    for user_id in range(poster_id, poster_id + num_users):
        if (user_id % 10 == 0):
            print(user_id)

        num_likes = min(total_posts, np.random.default_rng().negative_binomial(0.5, 0.001, 1).item())

        if num_likes:
            liked_post_id = np.random.default_rng().choice(max_post_id-1, size=num_likes, replace=False)

            for i in range(num_likes):
                likes.append({
                    "user_id": user_id,
                    "post_id": liked_post_id[i].item()+1,
                    "created_at": fake.date_time_between(start_date='-5y', end_date='now', tzinfo=None)
                })

    if likes:
        conn.execute(sqlalchemy.text("""
        INSERT INTO likes (user_id, post_id, created_at) 
        VALUES (:user_id, :post_id, :created_at);
        """), likes)
