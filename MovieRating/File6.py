from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
import pyspark.sql.functions as psf

spark = SparkSession \
    .builder \
    .appName("Hello") \
    .config("World") \
    .getOrCreate()

sc = spark.sparkContext
sqlContext = SQLContext(sc)
ratings = spark.createDataFrame(
    sc.textFile("ratings.dat").map(lambda l: l.split('::')),
    ["viewerid","movieid","rating","epoch"]
)
ratings.registerTempTable("ratings")

movies = spark.createDataFrame(
    sc.textFile("movies.dat").map(lambda l: l.split('::')),
    ["movieid","moviename","genre"]
)
movies.registerTempTable("movies")

users = spark.createDataFrame(
    sc.textFile("users.dat").map(lambda l: l.split('::')),
    ["userid","gender","age","occupation","zipcode"]
)
users.registerTempTable("users")



highestRateMovie = sqlContext.sql(""" SELECT
  row_number()
  OVER (
    ORDER BY b.moviename) AS position,
  b.moviename             AS title,
  round(a.avgrating,2)    AS avg_rating,
  a.moviecount            AS total_rating
FROM (SELECT
        movieid,
        moviecount,
        avgrating,
        rownum
      FROM (SELECT
              movieid,
              moviecount,
              avgrating,
              row_number()
              OVER (
                ORDER BY moviecount DESC) AS rownum
            FROM (SELECT
                    movieid,
                    count(*)    AS moviecount,
                    avg(rating) AS avgrating
                  FROM ratings
                  GROUP BY movieid
                  HAVING count(*) >= 10) AS tt) AS tt1
      WHERE tt1.rownum <= 10) AS a INNER JOIN movies b ON a.movieid = b.movieid""")


highestRateMovie.toPandas().to_csv("1_top_10.csv",index=False)

highestUserRating = sqlContext.sql(""" SELECT b.age as age_group_id,
  CASE b.age
       WHEN 1
         THEN "Under 18"
       WHEN 18
         THEN "18-24"
       WHEN 25
         THEN "25-34"
       WHEN 35
         THEN "35-44"
       WHEN 45
         THEN "45-49"
       WHEN 50
         THEN "50-55"
       WHEN 56
         THEN "56+" END as age_range
FROM (SELECT
        viewerid,
        usercount
      FROM (SELECT
              viewerid,
              count(*) AS usercount,
              max(count(*))
              OVER ()  AS maxcount
            FROM ratings
            GROUP BY viewerid) AS tt
      WHERE tt.maxcount = tt.usercount) AS a INNER JOIN users b ON a.viewerid = b.userid""")


highestUserRating.toPandas().to_csv("2_age_group_most_ratings.csv",index=False)


averageRatingComedy = sqlContext.sql(""" SELECT tt2.occupation as occupation_id ,case tt2.occupation when 0 then "other"
  when 1 then "academic/educator"
  when  2 then "artist"
  WHEN  3 THEN "clerical/admin"
  WHEN  4 THEN  "college/grad student"
  WHEN  5 THEN  "customer service"
  WHEN  6 THEN  "doctor/health care"
  WHEN  7 THEN  "executive/managerial"
  WHEN  8 THEN  "farmer"
  WHEN  9 THEN  "homemaker"
  WHEN 10 THEN  "K-12 student"
  WHEN 11 THEN  "lawyer"
  WHEN 12 THEN  "programmer"
  WHEN 13 THEN  "retired"
  WHEN 14 THEN  "sales/marketing"
  WHEN 15 THEN  "scientist"
  WHEN 16 THEN  "self-employed"
  WHEN 17 THEN  "technician/engineer"
  WHEN 18 THEN  "tradesman/craftsman"
  WHEN 19 THEN  "unemployed"
  WHEN 20 THEN  "writer" END as occupation FROM (SELECT viewerid,maxavg from (SELECT b.viewerid as viewerid, avg(b.rating) as avg, max(avg(b.rating)) over() as maxavg  FROM (SELECT movieid from movies where upper(genre) like "%COMEDY%") as a inner join ratings as b on a.movieid = b.movieid GROUP BY b.viewerid) as tt where tt.maxavg = tt.avg) as tt1 inner join users tt2 on tt2.userid = tt1.viewerid """)


averageRatingComedy.toPandas().to_csv("3_occupation_best_rating_comedies.csv",index=False)




bestRatedGenre = sqlContext.sql("""SELECT
  genre,
  maxavg AS avg_rating
FROM (SELECT
        avg(tt1.rating) AS avg,
        max(avg(tt1.rating))
        OVER ()         AS maxavg,
        tt2.genre       AS genre
      FROM (SELECT
              tt1.movieid,
              tt1.rating
            FROM ratings tt1 INNER JOIN (SELECT userid
                                         FROM users
                                         WHERE gender = 'M') tt2 ON tt1.viewerid = tt2.userid) tt1 INNER JOIN movies tt2
          ON tt2.movieid = tt1.movieid
      GROUP BY tt2.genre) AS tt3
WHERE tt3.avg = tt3.maxavg""")

bestRatedGenre.toPandas().to_csv("4_best_rated_genre.csv",index=False)


femaleRomantic = sqlContext.sql(""" SELECT
  ratingsfemale,
  ratingsmale
FROM (SELECT avg(tt2.rating) AS ratingsmale
      FROM users tt1, ratings tt2, movies tt3
      WHERE tt1.gender = 'M' AND upper(tt3.genre) LIKE '%ROMANCE%' AND tt2.viewerid = tt1.userid AND
            tt3.movieid = tt2.movieid) CROSS JOIN (SELECT avg(tt2.rating) AS ratingsfemale
                                                   FROM users tt1, ratings tt2, movies tt3
                                                   WHERE tt1.gender = 'F' AND upper(tt3.genre) LIKE '%ROMANCE%' AND
                                                         tt2.viewerid = tt1.userid AND tt3.movieid = tt2.movieid)""")

femaleRomantic.toPandas().to_csv("5_females_romantic_movies.csv",index=False)