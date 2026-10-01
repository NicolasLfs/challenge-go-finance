#!/bin/sh
set -eu

aws_local() {
  aws --endpoint-url "$AWS_ENDPOINT_URL" "$@"
}

json_escape() {
  printf '%s' "$1" | sed 's/\\/\\\\/g; s/"/\\"/g'
}

dlq_url=$(aws_local sqs create-queue \
  --queue-name wager-transactions-dlq.fifo \
  --attributes '{"FifoQueue":"true","ContentBasedDeduplication":"false"}' \
  --query QueueUrl --output text)
dlq_arn=$(aws_local sqs get-queue-attributes \
  --queue-url "$dlq_url" \
  --attribute-names QueueArn \
  --query 'Attributes.QueueArn' --output text)

redrive_policy=$(printf '{"deadLetterTargetArn":"%s","maxReceiveCount":"5"}' "$dlq_arn")
main_attributes=$(printf '{"FifoQueue":"true","ContentBasedDeduplication":"false","VisibilityTimeout":"60","RedrivePolicy":"%s"}' "$(json_escape "$redrive_policy")")
main_url=$(aws_local sqs create-queue \
  --queue-name wager-transactions.fifo \
  --attributes "$main_attributes" \
  --query QueueUrl --output text)
main_arn=$(aws_local sqs get-queue-attributes \
  --queue-url "$main_url" \
  --attribute-names QueueArn \
  --query 'Attributes.QueueArn' --output text)

allow_policy=$(printf '{"redrivePermission":"byQueue","sourceQueueArns":["%s"]}' "$main_arn")
aws_local sqs set-queue-attributes \
  --queue-url "$dlq_url" \
  --attributes "$(printf '{"RedriveAllowPolicy":"%s"}' "$(json_escape "$allow_policy")")"

aws_local sqs create-queue \
  --queue-name wager-events.fifo \
  --attributes '{"FifoQueue":"true","ContentBasedDeduplication":"false"}' >/dev/null

printf 'Provisioned wager-transactions.fifo, wager-transactions-dlq.fifo and wager-events.fifo\n'