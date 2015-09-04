import sqlite3
import json
from collections import Counter

db = sqlite3.connect("yelp_dataset.db")
c = db.cursor()

# Create tables
print("Creating businesses table.")
c.execute('''create table businesses
         (business_id text primary key,
          name text,
          full_address text,
          city text,
          state text,
          latitude double,
          longitude double, 
          stars decimal(1,1),
          review_count integer,
          open boolean)''')
print("Creating neighborhoods table.")
c.execute('''create table neighborhoods
          (business_id text,
           neighborhood text,
           primary key (business_id, neighborhood))''')
print("Creating categories table.")
c.execute('''create table categories
          (business_id text,
          category_name text,
          primary key (business_id, category_name))''')
print("Creating reviews table.")
c.execute('''create table reviews
         (business_id text,
          user_id text,
          stars decimal(1,1),
          date datetime, 
          primary key (business_id, user_id, date))''')
print("Creating users table.")
c.execute('''create table users
         (user_id text primary key,
          name text,
          review_count integer,
          average_stars decimal(1,1), 
          yelping_since datetime,
          fans integer)''')
print("Creating friends table.")
c.execute('''create table friends
         (user1_id text,
          user2_id text,
          primary key (user1_id, user2_id))''')
print("Creating checkins table.")
c.execute('''create table checkins
         (business_id text,
          day integer,
          num_checkins integer)''')

# Insert json records into tables
print("Populating businesses, neighborhoods, and categories table.")
f = open('yelp_academic_dataset_business.json')
for line in f.readlines():
  business = json.loads(line)
  c.execute('''insert into businesses(business_id, name, full_address, city, state, latitude, longitude, stars, review_count, open)
                  VALUES(?,?,?,?,?,?,?,?,?,?)''', (business['business_id'], business['name'], business['full_address'], business['city'], business['state'], business['latitude'], business['longitude'], business['stars'], business['review_count'], business['open']))
  if business['neighborhoods']:
    for neighborhood in business['neighborhoods']:
      c.execute('''insert into neighborhoods(business_id, neighborhood)
                    VALUES(?,?)''', (business['business_id'], neighborhood))
  if business['categories']:
    for category in business['categories']:
      c.execute('''insert into categories(business_id, category_name)
                    VALUES(?,?)''', (business['business_id'], category))

print("Populating reviews table.")
f = open('yelp_academic_dataset_review.json')
for line in f.readlines():
  review = json.loads(line)
  c.execute('''insert or replace into reviews(business_id, user_id, stars, date)
                  VALUES(?,?,?,?)''', (review['business_id'], review['user_id'], review['stars'], review['date']))

print("Populating users and friends table.")
f = open('yelp_academic_dataset_user.json')
for line in f.readlines():
  user = json.loads(line)
  c.execute('''insert into users(user_id, name, review_count, average_stars, yelping_since, fans)
                  VALUES(?,?,?,?,?,?)''', (user['user_id'], user['name'], user['review_count'], user['average_stars'], user['yelping_since'], user['fans']))
  if user['friends']:
    for friend in user['friends']:
       c.execute('''insert into friends(user1_id, user2_id)
                    VALUES(?,?)''', (user['user_id'], friend))

print("Populating checkins table.")
f = open('yelp_academic_dataset_checkin.json')
for line in f.readlines():
  business = json.loads(line)
  counts = Counter()
  for time, count in business['checkin_info'].items():
    counts[time[-1]] += count
  for day, count in counts.items():
    c.execute('''insert into checkins(business_id, day, num_checkins)
                VALUES(?,?,?)''', (business['business_id'], day, count))

print("Committing database changes.")
db.commit()
print("Done!")
c.close()


