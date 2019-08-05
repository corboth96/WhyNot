use smallmovies;

# QUERY 0: Get all movies
# Unpicked: Divergent
# why? Not in database
select movie_id, title, yearReleased from movie;

# QUERY 1: Get movies made after 2000 and before 2018
# Unpicked: The Beach
# WHY? year = 2000 (Expand to >=)
select movie_id, title, yearReleased from movie
where yearReleased<2018 and yearReleased>2000;

# QUERY 2: Get movies that Kate Winslet and Leonardo DiCaprio acted in
# Unpicked: Avatar 2
# WHY? Leonardo Dicaprio is not in the cast
select ss.movie_id, ss.title, ss.yearReleased from 
(select m.movie_id, title, yearReleased from movie m join roles r on m.movie_id = r.movie_id
where r.actor_id in (select actor_id from actor where fname = "Kate" and lname = "Winslet")) ss
inner join
(select m.movie_id, title, yearReleased from movie m join roles r on m.movie_id = r.movie_id where r.actor_id in 
(select actor_id from actor where fname = "Leonardo" and lname = "DiCaprio")) jc
on ss.movie_id = jc.movie_id;

# QUERY 3: Get all action movies that have more than one director
# Unpicked: Aladdin
# WHY? Not action movie
select * from movie m
left join moviegenres mg on m.movie_id = mg.movie_id
left join genre g on g.genre_id = mg.genre_id
where m.movie_id in 
(select movie_id from directedby 
group by movie_id 
having count(director_id)>=2) and g.genre = "Action";

# QUERY 4: Get all family movies with Director - John Lasseter and Actor - Tom Hanks
# Unpicked: Toy Story 3
# WHY? Not directed by John Lasseter
select g.movie_id as id, g.title, g.yearReleased
from
(select m.movie_id, title, yearReleased from movie m join moviegenres mg on m.movie_id = mg.movie_id 
where mg.genre_id in (select genre_id from genre where genre = "Family")) g
inner join
(select m.movie_id, title, yearReleased from movie m join directedby db on m.movie_id = db.movie_id
where db.director_id in (select director_id from director where fname = "John" and lname = "Lasseter")) d
inner join
(select m.movie_id, title, yearReleased from movie m join roles r on m.movie_id = r.movie_id
where r.actor_id in (select actor_id from actor where fname = "Tom" and lname = "Hanks")) a
on a.movie_id = g.movie_id and g.movie_id = d.movie_id;

# QUERY 5: Get all action movies that are directed by James Cameron
# Unpicked: Titanic
# WHY? Not an action movie
select g.movie_id, g.title, g.yearReleased from
(select m.movie_id, title, yearReleased from movie m join moviegenres mg on m.movie_id = mg.movie_id
where genre_id in (select genre_id from genre where genre = "Action")) g
inner join
(select m.movie_id, title, yearReleased from movie m join directedby db on m.movie_id = db.movie_id
where director_id in (select director_id from director where fname = "James" and lname = "Cameron")) d
using (movie_id);

# QUERY 6: Get all Drama movies released after 2000 that are directed by Steven Spielberg
# Unpicked: Saving Private Ryan
# WHY? Released before 2000
select * from 
(select m.movie_id as id, title, yearReleased from movie m join moviegenres mg on m.movie_id = mg.movie_id
where genre_id in (select genre_id from genre where genre = "Drama")) g
inner join
(select m.movie_id as id, title, yearReleased from movie m join directedby db on m.movie_id = db.movie_id
where director_id in (select director_id from director where fname = "Steven" and lname = "Spielberg")) d 
using (id)
where g.yearReleased > 2000;

# QUERY 7: Get all movies that are listed as romance, comedy AND family movies
# Unpicked: Forrest Gump
# WHY? Not Genre = Family
select m.movie_id, m.title, m.yearReleased from 
(select movie_id, title, yearReleased from movie) m
inner join
(select movie_id from moviegenres mg join genre g on g.genre_id = mg.genre_id where genre = 'Romance') a
inner join
(select movie_id from moviegenres mg join genre g on g.genre_id = mg.genre_id where genre = 'Comedy') r
inner join
(select movie_id from moviegenres mg join genre g on g.genre_id = mg.genre_id where genre = 'Family') d
on m.movie_id = a.movie_id and a.movie_id = r.movie_id and  r.movie_id = d.movie_id;

# QUERY 8: Get all James Cameron movies that have only James Cameron as a director
# Unpicked: Aliens of the Deep
# WHY? Movie has 2 directors
select ss.movie_id as id, ss.title, ss.yearReleased from
(select m.movie_id, title, yearReleased from movie m join directedby db on m.movie_id = db.movie_id
where director_id in (select director_id from director where fname = "James" and lname = "Cameron")) ss
where ss.movie_id in
(select movie_id from directedby 
group by movie_id 
having count(director_id)=1);

# QUERY 9: Get all movies that have more than 1 famous actor ("famous" meaning in more than 5 movies)
# Unpicked: Mrs. Doubtfire
# WHY? Has only 1 famous actor in it
select c.movie_id, c.title, c.yearReleased, movieCount from(
select m.movie_id,m.title,m.yearReleased,  count(*) as movieCount from movie m
join roles r on r.movie_id = m.movie_id
join actor a on a.actor_id = r.actor_id
where a.actor_id in (
select b.actor_id from (select a.actor_id, a.fname,a.lname, count(*) as cnt from Actor a 
join roles r on r.actor_id = a.actor_id 
group by a.actor_id,a.fname,a.lname) b
where b.cnt > 5)
group by m.title, m.movie_id,m.yearReleased
) c
where movieCount >= 2
order by c.movie_id;

# QUERY 10: Get all action movies
# Unpicked: Titanic
# WHY? Not an action movie
SELECT m.movie_id,m.title,m.yearReleased FROM Movie m
JOIN MovieGenres mg on mg.movie_id = m.movie_id
JOIN Genre g on g.genre_id = mg.genre_id WHERE g.genre = 'Action';

# QUERY 11: Get the title and actor count of all action movies made after 1990
# Unpicked: (Total Recall, 21)
# Why? made in 1990
select a.title, count(*) as actors from (
select m.movie_id, m.title, m.yearReleased, r.actor_id, g.genre_id, g.genre from movie m
left join roles r on r.movie_id = m.movie_id
left join moviegenres mg on mg.movie_id = m.movie_id
left join genre g on g.genre_id = mg.genre_id
where g.genre = 'Action' and m.yearReleased > 1990
) a
group by a.title;


# QUERY 12: Get the average year of movie releases for directors who directed action movies
# Unpicked: (John Lasseter, 2000)
# WHY? Not an action movie director
select d.fname, d.lname, avg(m.yearReleased) as AvgYearActive from Director d
left join DirectedBy db on db.director_id = d.director_id
left join Movie m on m.movie_id = db.movie_id
left join MovieGenres mg on mg.movie_id = m.movie_id
left join Genre g on g.genre_id = mg.genre_id
where g.genre='Action'
group by d.fname,d.lname;