import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class YelpQueries
{
  public static void main(String[] args) throws ClassNotFoundException
  {
    // load the sqlite-JDBC driver using the current class loader
    Class.forName("org.sqlite.JDBC");

    String dbLocation = "yelp_dataset.db"; 

    Connection connection = null;
    try
    {
      // create a database connection
      connection = DriverManager.getConnection("jdbc:sqlite:" + dbLocation);

      Statement statement = connection.createStatement();

      // Question 0
      statement.execute("DROP VIEW IF EXISTS q0"); // Clean out views
      String q0 = "CREATE VIEW q0 AS "
                   + "SELECT count(*) FROM reviews";
      statement.execute(q0);

      // Question 1
      statement.execute("DROP VIEW IF EXISTS q1");
      String q1 = "CREATE VIEW q1 AS " 
                  + "SELECT AVG(U.review_count) FROM users U WHERE U.review_count < 10"; // Replace this line
      statement.execute(q1);

      // Question 2
      statement.execute("DROP VIEW IF EXISTS q2");
      String q2 = "CREATE VIEW q2 AS "
                  + "SELECT U.name FROM users U WHERE U.yelping_since > '2014-11' AND U.review_count > 50"; // Replace this line
      statement.execute(q2);

      // Question 3
      statement.execute("DROP VIEW IF EXISTS q3");
      String q3 = "CREATE VIEW q3 AS "
                  + "SELECT B.name, B.stars FROM businesses B WHERE B.stars > 3 AND B.city = 'Pittsburgh'"; // Replace this line
      statement.execute(q3);

      // Question 4
      statement.execute("DROP VIEW IF EXISTS q4");
      String q4 = "CREATE VIEW q4 AS "
                  + "SELECT B.name FROM businesses B WHERE B.review_count >= 500 AND B.city = 'Las Vegas' ORDER BY B.stars ASC LIMIT 1"; // Replace this line
      statement.execute(q4);

      // Question 5
      statement.execute("DROP VIEW IF EXISTS q5");
      String q5 = "CREATE VIEW q5 AS "
                  + "SELECT B.name FROM businesses B INNER JOIN (SELECT * FROM checkins C WHERE C.day = 0 ORDER BY C.num_checkins DESC LIMIT 5) temp WHERE B.business_id = temp.business_id"; // Replace this line
      statement.execute(q5);

      // Question 6
      statement.execute("DROP VIEW IF EXISTS q6");
      String q6 = "CREATE VIEW q6 AS "
                  + "SELECT temp.day FROM (SELECT C.day, SUM(C.num_checkins) FROM checkins C GROUP BY C.day ORDER BY 2 DESC LIMIT 1) temp"; // Replace this line
      statement.execute(q6);

      // Question 7
      statement.execute("DROP VIEW IF EXISTS q7");
      String q7 = "CREATE VIEW q7 AS "
                  + "SELECT B.name FROM businesses B WHERE B.business_id IN (SELECT R.business_id FROM reviews R WHERE R.user_id IN (SELECT U.user_id FROM users U ORDER BY U.review_count DESC LIMIT 1))"; // Replace this line
      statement.execute(q7);

      // Question 8
      statement.execute("DROP VIEW IF EXISTS q8");
      String q8 = "CREATE VIEW q8 AS "
                  + "SELECT AVG(temp.stars) FROM (SELECT B.stars FROM businesses B WHERE B.city = 'Edinburgh' ORDER BY B.review_count DESC LIMIT ((SELECT COUNT(*) FROM businesses B1 WHERE B1.city = 'Edinburgh') / 10)) temp"; // Replace this line
      statement.execute(q8);

      // Question 9
      statement.execute("DROP VIEW IF EXISTS q9");
      String q9 = "CREATE VIEW q9 AS "
                  + "SELECT U.name FROM users U WHERE U.name LIKE '%..%'"; // Replace this line
      statement.execute(q9);

      // Question 10
      statement.execute("DROP VIEW IF EXISTS q10");
      String q10 = "CREATE VIEW q10 AS "
                  + "SELECT temp.city FROM (SELECT B.city, COUNT(*) FROM businesses B WHERE B.business_id IN (SELECT R.business_id FROM reviews R WHERE R.user_id IN (SELECT U.user_id FROM users U WHERE U.name LIKE '%..%')) GROUP BY B.city ORDER BY COUNT(*) DESC LIMIT 1) temp"; // Replace this line
      statement.execute(q10);

      connection.close();

    }
    catch(SQLException e)
    {
      // if the error message is "out of memory", 
      // it probably means no database file is found
      System.err.println(e.getMessage());
    }
    finally
    {
      try
      {
        if(connection != null)
          connection.close();
      }
      catch(SQLException e)
      {
        // connection close failed.
        System.err.println(e);
      }
    }
  }
}
