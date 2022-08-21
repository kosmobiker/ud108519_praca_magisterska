provider "aws" {
  region = "us-east-1"
  profile = "master"
}
variable "glue_database_name"{
    type    = string
    default = "darhevich_data_lake"
}

#Create policy documents for assume role and s3 permissions
data aws_iam_policy_document lambda_assume_role {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

data aws_iam_policy_document lambda_s3 {
  statement {
    actions = [
      "s3:PutObject",
      "s3:PutObjectAcl"
    ]

    resources = [
      "arn:aws:s3:::kosmobiker-masterproject/*",
      "arn:aws:logs:*:*:*"
    ]
  }
}
#Create an IAM policy
resource aws_iam_policy lambda_s3 {
  name        = "lambda-s3-permissions"
  description = "Contains S3 put permission for lambda"
  policy      = data.aws_iam_policy_document.lambda_s3.json
}
#Create a role
resource aws_iam_role lambda_role {
  name               = "lambda-role"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
}
#Attach policy to role
resource aws_iam_role_policy_attachment lambda_s3 {
  role       = aws_iam_role.lambda_role.name
  policy_arn = aws_iam_policy.lambda_s3.arn
}
#glue role
resource "aws_iam_role" "glue" {
  name = "AWSGlueServiceRoleDefault"
  assume_role_policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Action": "sts:AssumeRole",
      "Principal": {
        "Service": "glue.amazonaws.com"
      },
      "Effect": "Allow",
      "Sid": ""
    }
  ]
}
EOF
}

resource "aws_iam_role_policy_attachment" "glue_service" {
    role = "${aws_iam_role.glue.id}"
    policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

# If you don't already have a policy, uncomment this section
resource "aws_iam_role_policy" "my_s3_policy" {
 name = "my_s3_policy"
 role = "${aws_iam_role.glue.id}"
 policy = <<EOF
{
 "Version": "2012-10-17",
 "Statement": [
   {
     "Effect": "Allow",
     "Action": [
       "s3:*"
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
#Lambda functions used to fetch data from APIs
resource "aws_lambda_function" "daily_crypto_data" {
filename                       = "${path.module}/../src/lambda/daily_crypto_data.zip"
function_name                  = "daily_crypto_data"
role                           = aws_iam_role.lambda_role.arn
handler                        = "daily_crypto_data.lambda_handler"
runtime                        = "python3.9"
timeout                        = 180
}

resource "aws_lambda_function" "get_tweets" {
filename                       = "${path.module}/../src/lambda/get_tweets.zip"
function_name                  = "get_tweets"
role                           = aws_iam_role.lambda_role.arn
handler                        = "get_tweets.lambda_handler"
runtime                        = "python3.9"
timeout                        = 600
}

#Database creation
resource "aws_glue_catalog_database" "aws_glue_catalog_database" {
  name          = var.glue_database_name
  description   = "This database is used to store data"
  location_uri  = "s3://kosmobiker-masterproject/data/my_database"
}
