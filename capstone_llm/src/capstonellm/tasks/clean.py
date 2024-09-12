import argparse
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as sf

from capstonellm.common.spark import ClosableSparkSession

logger = logging.getLogger(__name__)


def clean(spark: SparkSession, environment: str, tag: str):
    if environment == "local":
        s3_input_path = f"/workspace/capstone-llm/capstone_llm/input/{tag}/"
        s3_output_path = f"/workspace/capstone-llm/capstone_llm/cleaned/{tag}/"
    else:
        s3_input_path = f"s3://dataminded-academy-capstone-llm-data-us/input/{tag}/"
        s3_output_path = f"s3://dataminded-academy-capstone-llm-data-us/cleaned/{tag}/"
    questions = spark.read.json(s3_input_path + "questions.json")
    questions.printSchema()
    
    answers = spark.read.json(s3_input_path + "answers.json")
    answers.printSchema()

    df_questions_items = questions.select("items") 
    df_exploded_q = df_questions_items.withColumn("questions", sf.explode(sf.col("items")))
    df_questions = df_exploded_q.select("questions.*").select("question_id", "title", "body").withColumnRenamed("body", "question_body")
    df_questions.show() # question_id, title, question_body

    df_answers_items = answers.select("items") 
    df_exploded_a = df_answers_items.withColumn("answers", sf.explode(sf.col("items")))
    df_answers = df_exploded_a.select("answers.*").select("question_id", "answer_id", "body").withColumnRenamed("body", "answer_body")
    df_answers.show() # question_id, answer_id, answer_body

    df = df_questions.join(df_answers, on="question_id").select("title", "question_body", "answer_body")
    df.write.json(s3_output_path)

    # def process_row(row):
    #     row.write.json(s3_output_path)

    # df.foreach(process_row)
    count = df.count()
    df.repartition(count).write.json()
    

def main():
    parser = argparse.ArgumentParser(description="capstone_llm")
    parser.add_argument(
        "-e", "--env", dest="env", help="environment we are executing in", required=True
    )
    parser.add_argument(
        "-t", "--tag", dest="tag", help="the tag to process",
        default="python-polars", required=False
    )
    logger.info("starting the cleaning job")

    args = parser.parse_args()
    common_spark_config = {
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
    }
    if args.env == "local":
        session = (
            SparkSession.builder.appName("Spark S3 Integration")
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4")
            .getOrCreate()
        )
        clean(session, args.env, args.tag)
    else:
        with ClosableSparkSession("capstone_llm", spark_config=common_spark_config) as session:
            clean(session, args.env, args.tag)


if __name__ == "__main__":
    main()
