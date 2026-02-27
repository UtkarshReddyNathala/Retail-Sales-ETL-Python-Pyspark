"""
s3_client_object.py
===================

AWS S3 Client Utility

Purpose:
Provides a reusable S3 client object using boto3 session.
This allows other modules to interact with S3 securely.

Usage:
    s3_provider = S3ClientProvider(aws_access_key, aws_secret_key)
    s3_client = s3_provider.get_client()
"""

import boto3


class S3ClientProvider:
    def __init__(self, aws_access_key=None, aws_secret_key=None):
        """
        Initialize S3 client provider with optional AWS credentials.

        Args:
            aws_access_key (str, optional): AWS Access Key ID
            aws_secret_key (str, optional): AWS Secret Access Key
        """
        self.aws_access_key = aws_access_key
        self.aws_secret_key = aws_secret_key

        # Create a boto3 session with the provided credentials
        self.session = boto3.Session(
            aws_access_key_id=self.aws_access_key,
            aws_secret_access_key=self.aws_secret_key
        )

        # Initialize S3 client from the session
        self.s3_client = self.session.client('s3')

    def get_client(self):
        """
        Get the boto3 S3 client instance.

        Returns:
            boto3.client: Configured S3 client
        """
        return self.s3_client
