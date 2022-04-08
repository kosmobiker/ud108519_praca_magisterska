provider "aws" {
  region = "us-east-1"
  profile = "master"
}

variable "region" {
    type    = string
    default = "us-east-1"
}

variable "glue_database_name"{
    type    = string
    default = "master_project"
}

variable "glue_parquet_table_name"{
    type    = string
    default = "historical_data"
}

resource "aws_iam_role" "lambda_role" {
name   = "Kosmobiker_Test_Lambda_Function_Role"
assume_role_policy = <<EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "VisualEditor0",
            "Effect": "Allow",
            "Action": [
                "s3:PutObject",
                "logs:CreateLogStream",
                "logs:CreateLogGroup",
                "logs:PutLogEvents"
            ],
            "Resource": [
                "arn:aws:s3:::kosmobiker-masterproject/*",
                "arn:aws:logs:*:*:*"
            ]
        }
    ]
}
EOF
}

resource "aws_iam_policy" "iam_policy_for_lambda" {
 name         = "aws_iam_policy_for_terraform_aws_lambda_role"
 path         = "/"
 description  = "AWS IAM Policy for managing aws lambda role"
 policy = <<EOF
{
 "Version": "2012-10-17",
 "Statement": [
   {
     "Action": [
       "logs:CreateLogGroup",
       "logs:CreateLogStream",
       "logs:PutLogEvents"
     ],
     "Resource": "arn:aws:logs:*:*:*",
     "Effect": "Allow"
   }
 ]
}
EOF
}

resource "aws_iam_role_policy_attachment" "attach_iam_policy_to_iam_role" {
 role        = aws_iam_role.lambda_role.name
 policy_arn  = aws_iam_policy.iam_policy_for_lambda.arn
}

# data "archive_file" "zip_the_python_code" {
# type        = "zip"
# source_dir  = "${path.module}/../src/lambda"
# output_path = "${path.module}/../src/lambda.zip"
# }

resource "aws_lambda_function" "terraform_lambda_func" {
filename                       = "${path.module}/../src/lambda/my-deployment-package.zip"
function_name                  = "Kosmo_Test_Lambda_Function"
role                           = aws_iam_role.lambda_role.arn
handler                        = "lambda_func.lambda_handler"
runtime                        = "python3.9"
depends_on                     = [aws_iam_role_policy_attachment.attach_iam_policy_to_iam_role]
}

resource "aws_glue_catalog_table" "aws_glue_catalog_table_parquet" {
  name          = var.glue_parquet_table_name
  database_name = var.glue_database_name

  table_type = "EXTERNAL_TABLE"

  parameters = {
    EXTERNAL              = "TRUE"
    "parquet.compression" = "SNAPPY"
  }

  partition_keys{
  	name = "coin"
  	type = "string"
  }

  storage_descriptor {
    location      = "s3://kosmobiker-masterproject/data/datalake/historical_data_parquet/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      name                  = "parquet-stream"
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"

      parameters = {
        "serialization.format" = 1
      }
    }

    columns {
      name = "timestamp"
      type = "bigint"
    }

    columns {
      name    = "prices"
      type    = "double"
    }

    columns {
      name    = "market_caps"
      type    = "double"
    }

    columns {
      name    = "total_volumes"
      type    = "double"
    }

    columns {
      name    = "formated_date"
      type    = "string"
    }

    columns {
      name    = "currency"
      type    = "string"
    }
  }
}