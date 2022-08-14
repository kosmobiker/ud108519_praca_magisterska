provider "aws" {
  region = "us-east-1"
  profile = "master"
}
variable "glue_database_name"{
    type    = string
    default = "darhevich_data_lake"
}
variable "glue_coin_info"{
    type    = string
    default = "coin_info"
}
variable "glue_ohlc_data"{
    type    = string
    default = "ohlc_data"
}
variable "glue_twitter_data"{
    type    = string
    default = "twitter_data"
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
#Lambda functions used to fetch data from APIs
# resource "aws_lambda_function" "daily_crypto_data" {
# filename                       = "${path.module}/../src/lambda/daily_crypto_data.zip"
# function_name                  = "daily_crypto_data"
# role                           = aws_iam_role.lambda_role.arn
# handler                        = "daily_crypto_data.lambda_handler"
# runtime                        = "python3.9"
# timeout                        = 180
# }

# resource "aws_lambda_function" "get_tweets" {
# filename                       = "${path.module}/../src/lambda/get_tweets.zip"
# function_name                  = "get_tweets"
# role                           = aws_iam_role.lambda_role.arn
# handler                        = "get_tweets.lambda_handler"
# runtime                        = "python3.9"
# timeout                        = 600
# }

#Database creation
resource "aws_glue_catalog_database" "aws_glue_catalog_database" {
  name          = var.glue_database_name
  description   = "This database is used to store data"
  location_uri  = "s3://kosmobiker-masterproject/data/my_database"
}

#Create table to store info about coins
resource "aws_glue_catalog_table" "aws_glue_catalog_table_csv" {
  name          = var.glue_coin_info
  database_name = var.glue_database_name

  table_type = "EXTERNAL_TABLE"

  parameters = {
    EXTERNAL                 = "TRUE"
    "classification"         = "csv"
    "skip.header.line.count" = "1"
  }

  storage_descriptor {
    input_format  = "org.apache.hadoop.mapred.TextInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"

    ser_de_info {
      name                  = "csv-stream"
      serialization_library = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"

      parameters = {
      "separatorChar" = ","
      "field.delim" = ","
      }
    }

columns {
        name = "Id"
        type = "bigint"
      }
      
      columns {
        name = "Url"
        type = "string"
      }     

      columns {
        name = "ImageUrl"
        type = "string"
      } 

      columns {
        name = "ContentCreatedOn"
        type = "bigint"
      }    

      columns {
        name = "Name"
        type = "string"
      }
      

      columns {
        name = "Symbol"
        type = "string"
      }

      columns {
        name = "CoinName"
        type = "string"
      }
      

      columns {
        name = "FullName"
        type = "string"
      }

      columns {
        name = "Description"
        type = "string"
      }
      
      columns {
        name = "AssetTokenStatus"
        type = "string"
      }

      columns {
        name = "Algorithm"
        type = "string"
      }

      columns {
        name = "ProofType"
        type = "string"
      }
      
      columns {
        name = "SortOrder"
        type = "bigint"
      }
      
      columns {
        name = "Sponsored"
        type = "boolean"
      }

      columns {
        name = "Taxonomy.Access"
        type = "string"
      }
 
      columns {
        name = "Taxonomy.FCA"
        type = "string"
      }
      
      columns {
        name = "Taxonomy.FINMA"
        type = "string"
      }
      
      columns {
        name = "Taxonomy.Industry"
        type = "string"
      }
      
      columns {
        name = "Taxonomy.CollateralizedAsset"
        type = "string"
      }

      columns {
        name = "Taxonomy.CollateralizedAssetType"
        type = "string"
      }

      columns {
        name = "Taxonomy.CollateralType"
        type = "string"
      }

      columns {
        name = "Taxonomy.CollateralInfo"
        type = "string"
      }
      
      columns {
        name = "Rating.Weiss.Rating"
        type = "string"
      }

      columns {
        name = "Rating.Weiss.TechnologyAdoptionRating"
        type = "string"
      }

      columns {
        name = "Rating.Weiss.MarketPerformanceRating"
        type = "string"
      }
  }
}

#create table to store ohlc data
resource "aws_glue_catalog_table" "aws_glue_catalog_table_parquet" {
  name          = var.glue_ohlc_data
  database_name = var.glue_database_name

  table_type = "EXTERNAL_TABLE"

  parameters = {
    EXTERNAL              = "TRUE"
    "parquet.compression" = "SNAPPY"
  }

  partition_keys{
  	name = "partition_col"
  	type = "string"
  }

  storage_descriptor {
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
      name = "ticker"
      type = "string"
    }
    
    columns {
      name = "date_time"
      type = "string"
    }

    columns {
      name = "open"
      type = "double"
    }

    columns {
      name = "high"
      type = "double"
    }

    columns {
      name = "low"
      type = "double"
    }
    
    columns {
      name = "close"
      type = "double"
    }
    
    columns {
      name = "volume_fsym"
      type = "double"
    }
    
    columns {
      name = "volume_tsym"
      type = "double"
    }
    
    columns {
      name = "currency"
      type = "string"
    }
    
    columns {
      name = "delta"
      type = "double"
    }
    
    columns {
      name = "time_stamp"
      type = "bigint"
    }
    
    columns {
      name = "year"
      type = "int"
    }
    
    columns {
      name = "month"
      type = "int"
    }
    

    columns {
      name = "day"
      type = "int"
    }
    
    columns {
      name = "hour"
      type = "int"
    }
    
    columns {
      name = "minute"
      type = "int"
    }
  }
}
