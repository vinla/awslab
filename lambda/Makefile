.PHONY: create-bucket
create-bucket:
	aws s3api create-bucket --bucket ($bucket-name) --region eu-west-1 --profile ($profile) --create-bucket-configuration LocationConstraint=eu-west-1