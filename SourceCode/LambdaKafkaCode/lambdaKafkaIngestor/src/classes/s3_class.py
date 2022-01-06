from src.errors.lambda_error import LambdaError
import boto3
from botocore.exceptions import ClientError


class S3Management():
    """Manage s3 interactions."""

    def __init__(self, config, logger):
        """
        Constructor method. handles all config properties for class.
        :param config: config properties from config file
        :param logger: object to send logs to clowdwatch logs.
        """
        self.logger = logger
        self.config = config
        self.tmp_dir = config['lambda']['temp_folder']
        try:
            self.s3_client = boto3.client('s3', region_name=config['aws_region_name'])
        except (ClientError, KeyError) as error:
            raise LambdaError(
                f"Failed on [s3_class][__init__]. Detail: {error}")

    def create_truststore_from_s3(self):
        """
        read config properties related to s3 where SSL truststore Certificates were configured,
        read certificates from s3 and writes certificates/ private/ ca keys temporary.
        :return: updated config object with certificates/keys/ca temp path.
        """
        s3_trustore_location = self.config['kafka'].get("s3_ssl_ca_location")
        if s3_trustore_location:
            trustore_content = self.get_cert_value_from_s3(s3_trustore_location)
            self.write_content_to_tmp_file(trustore_content, "all.pem")
            self.config['kafka']["ssl_ca_location"] = self.tmp_dir + "all.pem"

        return self.config

    def get_cert_value_from_s3(self, s3_trustore_location):
        """
        retrieve a s3 content from a given s3 path.
        :param s3_trustore_location: truststore path
        :return: truststore content
        """
        try:
            self.truststore_bucket_name = self.config['kafka']['s3_ssl_ca_location'].split("/")[0]
            self.truststore_s3_key = '/'.join(self.config['kafka']['s3_ssl_ca_location'].split("/")[1:])
            self.logger.info("Getting truststore info from s3 bucket: " + str(self.truststore_bucket_name))
            data = self.s3_client.get_object(
                 Bucket=self.truststore_bucket_name,
                 Key=self.truststore_s3_key
            )
            contents = data['Body'].read()
            return contents

        except (ClientError, KeyError) as error:
            raise LambdaError("Failed on [s3_class][get_cert_value_from_s3]."
                              f"\nDetail: {error}")

    def write_content_to_tmp_file(self, content, file_name):
        """
        write a string content to a local file given the local file name.
        :param content : string with the content to write
        :param content : local file name in the lambda context
        """
        with open(self.tmp_dir + file_name, "w") as text_file: 
            text_file.write(content.decode('utf-8'))
