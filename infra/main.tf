resource "aws_lambda_function" "my_function" {
  filename      = "../app.zip"
  function_name = "${var.function_name}-${var.env}"
  role          = aws_iam_role.my_role.arn
  handler       = "app.handler.lambda_handler"
  runtime       = "python3.9"

  source_code_hash = filebase64sha256("../app.zip")

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_iam_role" "my_role" {
  name = "replenish4me-${var.function_name}-role-${var.env}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "my_policy_attachment" {
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
  role       = aws_iam_role.my_role.name
}

data "aws_caller_identity" "current" {}

resource "aws_iam_policy" "secrets_manager_policy" {
  name        = "${var.function_name}-secrets-manager-policy-${var.env}"
  description = "Policy to allow access to Secrets Manager"
  policy      = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Action = [
          "secretsmanager:GetSecretValue"
        ],
        Resource = "arn:aws:secretsmanager:${var.region}:${data.aws_caller_identity.current.account_id}:secret:*"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "secrets_manager_policy_attachment" {
  policy_arn = aws_iam_policy.secrets_manager_policy.arn
  role       = aws_iam_role.my_role.id
}

resource "aws_iam_policy" "describe_database_policy" {
  name        = "${var.function_name}-describe-database-policy-${var.env}"
  description = "Policy to allow access to Describe Database"
  policy      = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Action = [
          "rds:Describe*"
        ],
        Resource = "*"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "describe_database_policy_attachment" {
  policy_arn = aws_iam_policy.describe_database_policy.arn
  role       = aws_iam_role.my_role.id
}