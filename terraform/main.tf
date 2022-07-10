provider "aws" {
  region = "us-east-1"
  profile = "master"
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
#lambda functions
# resource "aws_lambda_function" "coingecko_historical_data" {
# filename                       = "${path.module}/../src/lambda/my-deployment-package.zip"
# function_name                  = "coingecko_historical_data"
# role                           = aws_iam_role.lambda_role.arn
# handler                        = "lambda_func.lambda_handler"
# runtime                        = "python3.9"
# }

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