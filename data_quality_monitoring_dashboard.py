import sys
import json
from datetime import datetime
from pyspark.context import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import col, when, count
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
import boto3

# Step 1: Initialize Glue Context and Spark Session
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
sqlContext = SQLContext(sc)

# Step 2: Read arguments passed from Lambda via Glue
args = getResolvedOptions(sys.argv, [
    'BUCKET_NAME',
    'OBJECT_KEY',
    'SNS_TOPIC_ARN',
    'ALERT_THRESHOLD',
    'EXPECTED_SCHEMA'
])

# Extract parameters
bucket_name = args['BUCKET_NAME']
object_key = args['OBJECT_KEY']
sns_topic_arn = args['SNS_TOPIC_ARN']
alert_threshold = float(args['ALERT_THRESHOLD'])
expected_schema = json.loads(args['EXPECTED_SCHEMA'])

# Derive paths
s3_input_path = f"s3://{bucket_name}/{object_key}"
s3_output_path = f"s3://{bucket_name}/output"
archival_output_path = f"s3://{bucket_name}/archival"

print(f"Processing file: {s3_input_path}")
print(f"Saving results to: {s3_output_path}")

# Step 3: Read input data from S3
input_df = spark.read.option("header", True).option("inferSchema", True).csv(s3_input_path)
print("Data Schema: ")
input_df.printSchema()

# Step 4: Data Quality Checks
def check_missing_values(df):
    total_records = df.count()
    missing_counts = df.select(
        [(count(when(col(c).isNull() | (col(c) == ""), c)).alias(c)) for c in df.columns]
    ).collect()[0].asDict()
    print("Missing Counts: ", missing_counts)
    missing_percentages = {
        col: (missing_counts[col] / total_records) * 100 for col in missing_counts
    }
    print("Missing percentage: ", missing_percentages)
    return missing_percentages

def check_duplicates(df, key_columns=None):
    if key_columns is None:
        key_columns = df.columns
    total_records = df.count()
    grouped_df = df.groupBy(key_columns).count()
    duplicate_records = grouped_df.filter(col("count") > 1).count()
    print("Duplicate records: ", duplicate_records)
    duplicate_percentage = (duplicate_records / total_records) * 100
    print("Duplicate percentage: ", duplicate_percentage)
    return duplicate_percentage

def check_data_types(df, schema):
    mismatched_columns = {}
    for column, expected_type in schema.items():
        if column in df.columns:
            actual_type = df.schema[column].dataType.typeName()
            if actual_type != expected_type:
                mismatched_columns[column] = {
                    "expected": expected_type,
                    "actual": actual_type
                }
    print("Mismatched columns: ", mismatched_columns)            
    return mismatched_columns

# Perform checks
missing_values_report = check_missing_values(input_df)
duplicate_percentage = check_duplicates(input_df, key_columns=["Product ID"])
data_type_issues = check_data_types(input_df, expected_schema)

# Step 5: Generate the report
current_date = datetime.now().strftime("%Y-%m-%d")
current_month = datetime.now().strftime("%Y-%m")
data_quality_report = []

data_quality_report.append({
    "Date": current_date
})

missing_values_passed = 100 - sum(missing_values_report[col] for col in missing_values_report)
missing_values_failed = sum(missing_values_report[col] for col in missing_values_report)
missing_values_threshold_met = missing_values_failed <= alert_threshold

data_quality_report.append({
    "check_name": "Missing Values",
    "passed": missing_values_passed,
    "failed": missing_values_failed,
    "threshold_percentage": alert_threshold,
    "threshold_met": missing_values_threshold_met
})

duplicates_passed = 100 - duplicate_percentage
duplicates_failed = duplicate_percentage
duplicates_threshold_met = duplicates_failed <= alert_threshold

data_quality_report.append({
    "check_name": "Duplicate Records",
    "passed": duplicates_passed,
    "failed": duplicates_failed,
    "threshold_percentage": alert_threshold,
    "threshold_met": duplicates_threshold_met
})

data_types_passed = 100 - len(data_type_issues)
data_types_failed = len(data_type_issues)
data_types_threshold_met = data_types_failed <= alert_threshold

data_quality_report.append({
    "check_name": "Consistency Issues",
    "passed": data_types_passed,
    "failed": data_types_failed,
    "threshold_percentage": alert_threshold,
    "threshold_met": data_types_threshold_met
})

report = {
    "data_quality_report": data_quality_report
}

# Step 6: Save report to S3 with a dynamic date
output_file_key = f"output/data_quality_report_{current_date}.json"
archival_file_key = f"archival/{current_month}/data_quality_report_{current_date}.json"

report_json = json.dumps(report, indent=2)
s3 = boto3.client("s3")

s3.put_object(Bucket=bucket_name, Key=output_file_key, Body=report_json)
s3.put_object(Bucket=bucket_name, Key=archival_file_key, Body=report_json)

output_path_with_date = f"{s3_output_path}/data_quality_report_{current_date}.json"
print(f"Report saved to S3 at: {output_path_with_date}")
print(f"Archival report saved to S3 at: {archival_output_path}/{current_month}/data_quality_report_{current_date}.json")

# Step 7: Send alert if threshold is exceeded
alert_message = ""
if duplicates_failed > alert_threshold:
    alert_message += f"Duplicate records exceed threshold: {duplicates_failed:.2f}%\n"

for col, percentage in missing_values_report.items():
    if percentage > alert_threshold:
        alert_message += f"Missing values in column '{col}' exceed threshold: {percentage:.2f}%\n"

if data_type_issues:
    alert_message += f"Data type mismatches detected: {data_type_issues}\n"

if alert_message:
    alert_message = f"Data Quality Issues Detected:\n{alert_message}\nReport: {output_path_with_date}"
    sns_client = boto3.client("sns")
    sns_client.publish(
        TopicArn=sns_topic_arn,
        Message=alert_message,
        Subject="Data Quality Alert"
    )
    print("Alert sent via SNS")

# Step 8: Graceful exit
if alert_message:
    print("Warning: Data quality issues exceeded the threshold. Please check the report.")
else:
    print("Data quality checks completed successfully.")
