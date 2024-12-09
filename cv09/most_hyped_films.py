from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, DataTypes, EnvironmentSettings


def most_hyped_movies():
    # batch execution
    settings = EnvironmentSettings.in_batch_mode()
    t_env = StreamTableEnvironment.create(environment_settings=settings)

    t_env.execute_sql(
        """
        CREATE TABLE ratings (
            userId INT,
            movieId INT,
            rating INT,
            `timestamp` STRING
        ) WITH (
            'connector' = 'filesystem',
            'path' = '/files/data/u.data',
            'format' = 'csv',
            'csv.field-delimiter' = '\t',
            'csv.ignore-parse-errors' = 'true'
        )
    """
    )

    t_env.execute_sql(
        """
        CREATE TABLE movies (
            movieId INT,
            title STRING
        ) WITH (
            'connector' = 'filesystem',
            'path' = '/files/data/u-mod.item',
            'format' = 'csv',
            'csv.field-delimiter' = '|',
            'csv.ignore-parse-errors' = 'true'
        )
    """
    )

    # Calc of ratio
    t_env.execute_sql(
        """
        CREATE VIEW movie_ratios AS
        SELECT 
            m.title,
            COUNT(CASE WHEN r.rating = 5 THEN 1 END) as five_star_count,
            COUNT(*) as total_count,
            CAST(COUNT(CASE WHEN r.rating = 5 THEN 1 END) AS DOUBLE) / CAST(COUNT(*) AS DOUBLE) as ratio
        FROM ratings r
        JOIN movies m ON r.movieId = m.movieId
        GROUP BY m.title
        HAVING COUNT(*) >= 5  -- Filter movies with at least 5 ratings
    """
    )

    # Getnutí final resultu
    result = t_env.execute_sql(
        """
        SELECT title, five_star_count, total_count, ratio
        FROM (
            SELECT 
                *,
                ROW_NUMBER() OVER (ORDER BY ratio DESC, title) as rn
            FROM movie_ratios
        )
        WHERE rn <= 10
        ORDER BY ratio DESC, title
    """
    )
    # Print resultu
    result.print()


if __name__ == "__main__":
    most_hyped_movies()
